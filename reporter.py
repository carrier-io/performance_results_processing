from json import loads, dumps
import csv
import datetime
from time import time
import requests
from os import environ
from traceback import format_exc

from centry_loki import log_loki
from data_manager import DataManager
from perfreporter.base_reporter import Reporter
from perfreporter.junit_reporter import JUnitReporter
from perfreporter.email_reporter import EmailReporter


def finish_test_report(args, response_times, test_status):
    headers = {
        'Authorization': f'bearer {args["token"]}',
        'Content-type': 'application/json'
    }
    lg_type = args["influx_db"].split("_")[0] if "_" in args["influx_db"] else args["influx_db"]
    data = {'build_id': args["build_id"], 'test_name': args["simulation"], 'lg_type': lg_type,
            'missed': int(0),
            'test_status': test_status,
            'vusers': args["users"],
            'duration': args['duration'], 'response_times': dumps(response_times)}
    url = f'{args["base_url"]}/api/v1/backend_performance/reports/{args["project_id"]}'
    r = requests.put(url, json=data, headers=headers)
    logger.info(r.text)
    try:
        if r.json()["message"] == "updated":
            logger.info("Post processing finished")
    except:
        logger.error("Failed update report")
        data = {"test_status": {"status": "ERROR", "percentage": 100, "description": "Failed update report"}}
        url = f'{args["base_url"]}/api/v1/backend_performance/report_status/{args["project_id"]}/{args["report_id"]}'
        response = requests.put(url, json=data, headers=headers)
        try:
            logger.info(response.json()["message"])
        except:
            logger.info(response.text)


# def parse_quality_gate(quality_gate_data: dict) -> dict:
#     '''Parse QualityGate configuration from integrations.
#     If any of the values in the dictionary is -1,
#     then we set the corresponding flag as False.
#     '''
#     if not quality_gate_data:
#         return {}
#     logger.info("Parsing QualityGate configuration")
#     try:
#         return {
#             'check_functional_errors': quality_gate_data['error_rate'] != -1,
#             'check_performance_degradation': quality_gate_data['degradation_rate'] != -1,
#             'check_missed_thresholds': quality_gate_data['missed_thresholds'] != -1,
#             'error_rate': quality_gate_data['error_rate'],
#             'performance_degradation_rate': quality_gate_data['degradation_rate'],
#             'missed_thresholds_rate': quality_gate_data['missed_thresholds'],
#         }
#     except Exception as e:
#         logger.error("Failed to parse QualityGate configuration")
#         logger.error(e)
#         return {}


def get_reporters(reporters_config) -> list:
    reporters = []
    for reporter in Reporter.__subclasses__():
        logger.info(f'Check reporter: {reporter}')
        if reporter.is_valid_config(reporters_config):
            logger.info(f'{reporter} is valid')
            reporters.append(reporter)
        else:
            logger.info(f"{reporter} values missing, proceeding without {reporter}")
    return reporters


def reporting_junit(data_manager, args, current_test_results, aggregated_test_data, all_checks, 
                    reasons_to_fail_report, quality_gate_config, s3_config):
    headers = {'Authorization': f'bearer {args["token"]}'} if args["token"] else {}
    logger.info('Start reporting to JUnit')
    try:
        results_bucket = args['simulation'].replace("_", "").lower()
        _, _, thresholds, _ = data_manager.compare_with_thresholds(current_test_results, aggregated_test_data, 
                                                                   quality_gate_config, True)
        report = JUnitReporter.create_report(thresholds, args['build_id'], all_checks, reasons_to_fail_report)
        files = {'file': open(report, 'rb')}
        upload_url = f'{args["base_url"]}/api/v1/artifacts/artifacts/{args["project_id"]}/{results_bucket}'
        requests.post(upload_url, params=s3_config, allow_redirects=True, files=files, headers=headers)
    except Exception as e:
        logger.error("Failed to create junit report")
        logger.error(e)
    
def reporting(data_manager, args, aggregated_test_data, integrations, quality_gate_config, report_performance_degradation,
              compare_baseline_per_request_details, report_missed_thresholds, compare_with_thresholds):
    if integrations and integrations.get("reporters"):
        if "reporter_email" in integrations["reporters"].keys():
            logger.info(f'Reporting to Email')
            resp = EmailReporter.process_report(args, aggregated_test_data, integrations, quality_gate_config)
            logger.info(resp)
            logger.info('Reporting to Email finished')
        reporters = get_reporters(integrations["reporters"])
        aggregated_errors = data_manager.get_aggregated_errors(quality_gate_config)
        for active_reporter in reporters:
            logger.info(f'Reporting to {active_reporter}')
            try:
                reporter = active_reporter(args, integrations["reporters"], quality_gate_config)
            except Exception:
                logger.error(f'Failed to create {active_reporter}')
                continue
            logger.info('Reporter created')
            if reporter.check_functional_errors and aggregated_errors:
                reporter.report_errors(aggregated_errors)
                logger.info('report_errors is done')
            if reporter.check_performance_degradation and report_performance_degradation:
                reporter.report_performance_degradation(compare_baseline_per_request_details, report_performance_degradation)
                logger.info('report_performance_degradation is done')
            if reporter.check_missed_thresholds and report_missed_thresholds:
                reporter.report_missed_thresholds(compare_with_thresholds, report_missed_thresholds)
                logger.info('report_missed_thresholds is done')


def get_loki_logger(args):
    loki_context = {"url": f"http://{args['influx_host']}:3100/loki/api/v1/push",
                    "hostname": "post-processor", 
                    "labels": {"build_id": args['build_id'],
                               "project": args['project_id'],
                               "report_id": args['report_id']}}
    return log_loki.get_logger(loki_context)


if __name__ == '__main__':
    timestamp = time()
    integrations = loads(environ.get("integrations", '{}'))
    quality_gate_config = integrations.get('processing', {}).get('quality_gate', {})
    s3_config = integrations.get('system', {}).get('s3_integration', {})
    
    args = DataManager.get_args()
    logger = get_loki_logger(args)
    data_manager = DataManager(args, s3_config, logger)
    total_checked_thresholds, performance_degradation_rate, missed_threshold_rate = 0, 0, 0
    compare_baseline_summary, compare_baseline_per_request = [], []
    compare_baseline_per_request_details = []
    try:
        response_times = data_manager.get_response_times()
        comparison_data = data_manager.get_comparison_data()
        current_test_results, error_count = data_manager.send_summary_table_data(response_times, comparison_data, timestamp)
        try:
            data_manager.send_engine_health_cpu()
        except Exception as e:
            logger.error("Failed to send_engine_health_cpu")
            logger.error(e)
        try:
            data_manager.send_engine_health_memory()
        except Exception as e:
            logger.error("Failed to send_engine_health_memory")
            logger.error(e)
        try:
            data_manager.send_engine_health_load()
        except Exception as e:
            logger.error("Failed to send_engine_health_load")
            logger.error(e)
        try:
            data_manager.send_loki_errors()
        except Exception as e:
            logger.error("Failed to send_loki_errors")
            logger.error(e)

        logger.info('Compare with baseline')
        try:
            if quality_gate_config.get("baseline", {}).get("checked"):
                baseline = data_manager.get_baseline()
                if baseline:
                    logger.info('Check summary')
                    baseline_summary = list(filter(lambda req: req['method'] == 'All', baseline))[0]
                    current_test_summary = list(filter(lambda req: req['method'] == 'All', current_test_results))[0]
                    compare_baseline_summary = data_manager.compare_with_baseline_summary(baseline_summary, current_test_summary, quality_gate_config)
                    compare_baseline_per_request, compare_baseline_per_request_details = data_manager.compare_with_baseline_per_request(
                        baseline, current_test_results, quality_gate_config)
                #performance_degradation_rate, compare_with_baseline = data_manager.compare_with_baseline(baseline, current_test_results)
        except Exception as e:
            logger.error("Failed to compare with baseline")
            logger.error(e)

        logger.info('Compare with thresholds')
        aggregated_test_data = {}
        compare_with_thresholds, compare_with_globaly_applicable = [], []
        try:
            aggregated_test_data = {
                'throughput': round(float(args['total_requests_count']) / float(args['duration']), 3),
                'ko': error_count, 'total': args['total_requests_count'], 'request_name': 'all',
                "min": float(response_times["min"]), "max": float(response_times["max"]),
                "avg": float(response_times["mean"]), "pct50": response_times["pct50"],
                "pct75": response_times["pct75"], "pct90": response_times["pct90"],
                "pct95": response_times["pct95"], "pct99": response_times["pct99"]
            }
            if quality_gate_config.get("SLA", {}).get("checked"):
                total_checked_thresholds, missed_threshold_rate, compare_with_thresholds, compare_with_globaly_applicable = \
                    data_manager.compare_with_thresholds(current_test_results, aggregated_test_data, quality_gate_config)
        except Exception as e:
            logger.error("Failed to compare with thresholds")
            logger.error(e)

        reasons_to_fail_report = []
        all_checks = []
        
        report_performance_degradation = []
        report_missed_thresholds = []
        
        all_checks.extend(compare_baseline_summary)
        all_checks.extend(compare_baseline_per_request)
        all_checks.extend(compare_with_globaly_applicable)

        logger.info('Check Baseline')
        for each in compare_baseline_summary:
            if each["status"] == "Failed":
                reasons_to_fail_report.append(each["message"])
                report_performance_degradation.append(each)
        for each in compare_baseline_per_request:
            if each["status"] == "Failed":
                reasons_to_fail_report.append(each["message"])
                report_performance_degradation.append(each)

        logger.info('Check SLAs')
        for each in compare_with_globaly_applicable:
            if each["status"] == "Failed":
                reasons_to_fail_report.append(each["message"])
                report_missed_thresholds.append(each)
        if quality_gate_config.get("SLA", {}).get("checked") and total_checked_thresholds:
            thresholds_quality_gate = quality_gate_config.get("settings", {}).get("per_request_results", {}).get("percentage_of_failed_requests", 20)
            if missed_threshold_rate > thresholds_quality_gate:
                _res = {"type": "SLAs per request",
                        "status": "Failed",
                        "message": f"Missed more than {thresholds_quality_gate}% SLAs"
                        }
                all_checks.append(_res)
                reasons_to_fail_report.append(_res["message"])
                report_missed_thresholds.append(_res)
            else:
                _res = {"type": "SLAs per request",
                        "status": "Success",
                        "message": f"Successfully met more than {thresholds_quality_gate}% SLAs"
                        }
                all_checks.append(_res)

        logger.info('Set test status')
        test_status = {"status": "Finished", "percentage": 100, "description": "Test is finished"}
        if quality_gate_config:
            if reasons_to_fail_report:
                test_status = {"status": "Failed", 
                               "percentage": 100,
                               "description": "; ".join(reasons_to_fail_report)}
            else:
                test_status = {"status": "Success", 
                               "percentage": 100,
                               "description": "Quality gate passed"}

        finish_test_report(args, response_times, test_status)

        logger.info('Start reporting')
        if quality_gate_config:
            reporting_junit(data_manager, args, current_test_results, aggregated_test_data, 
                            all_checks, reasons_to_fail_report, quality_gate_config, s3_config)

        reporting(data_manager, args, aggregated_test_data, integrations, quality_gate_config, report_performance_degradation,
                  compare_baseline_per_request_details, report_missed_thresholds, compare_with_thresholds)
        logger.info('Finish reporting')

    except Exception as e:
        logger.error("Failed to update report")
        logger.error(e)

    logger.info(f"Finish main processing: {round(time() - timestamp, 2)} sec")
    
    data_manager.upload_test_results(f"/tmp/{args['build_id']}.csv")
    data_manager.upload_test_results(f"/tmp/users_{args['build_id']}.csv")
    data_manager.delete_test_data()

    logger.info(f"Total execution time for reporter: {round(time() - timestamp, 2)} sec")
