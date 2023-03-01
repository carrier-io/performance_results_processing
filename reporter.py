from json import loads, dumps
import csv
import datetime
from time import time
from influxdb import InfluxDBClient
import requests
from os import environ
import operator
from traceback import format_exc


DELETE_TEST_DATA = "delete from {} where build_id='{}'"
DELETE_USERS_DATA = "delete from \"users\" where build_id='{}'"
integrations = loads(environ.get("integrations", '{}'))

def get_args():
    with open("/tmp/args.json", "r") as f:
        return loads(f.read())


def get_response_times():
    with open("/tmp/response_times.csv", "r") as f:
        lines = f.readlines()
        headers = lines[0].split(",")
        values = lines[1].split(",")
        response_times = {}
        for i in range(len(headers)):
            response_times[headers[i].replace("\n", "")] = int(float(values[i].replace("\n", "")))
        return response_times


def get_comparison_data():
    comparison_data = []
    with open("/tmp/comparison.csv", "r") as f:
        csv_reader = csv.DictReader(f)
        # convert each csv row into python dict
        for row in csv_reader:
            # add this python dict to json array
            comparison_data.append(row)
    return comparison_data


def send_summary_table_data(args, client, response_times, comparison_data, timestamp):
    points = []
    for req in comparison_data:
        influx_record = {
            "measurement": "api_comparison",
            "tags": {
                "simulation": args['simulation'],
                "env": args['env'],
                "users": args["users"],
                "test_type": args['type'],
                "build_id": args['build_id'],
                "request_name": req['request_name'],
                "method": req['method'],
                "duration": args['duration']
            },
            "time": datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%SZ'),
            "fields": {
                "throughput": round(float(req["total"]) / float(args['duration']), 3),
                "total": int(req["total"]),
                "ok": int(req["ok"]),
                "ko": int(req["ko"]),
                "1xx": int(req["1xx"]),
                "2xx": int(req["2xx"]),
                "3xx": int(req["3xx"]),
                "4xx": int(req["4xx"]),
                "5xx": int(req["5xx"]),
                "NaN": int(req["NaN"]),
                "min": float(req["min"]),
                "max": float(req["max"]),
                "mean": round(float(req["mean"]), 2),
                "pct50": int(req["pct50"]),
                "pct75": int(req["pct75"]),
                "pct90": int(req["pct90"]),
                "pct95": int(req["pct95"]),
                "pct99": int(req["pct99"]),
            }
        }
        points.append(influx_record)

    # Summary
    error_count = sum(point['fields']['ko'] for point in points)
    points.append({"measurement": "api_comparison", "tags": {"simulation": args['simulation'],
                                                             "env": args['env'], "users": args["users"],
                                                             "test_type": args['type'], "duration": args['duration'],
                                                             "build_id": args['build_id'],
                                                             "request_name": "All", "method": "All"},
                   "time": datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%SZ'),
                   "fields": {"throughput": round(float(args['total_requests_count']) / float(args['duration']), 3),
                              "total": int(args['total_requests_count']),
                              "ok": sum(point['fields']['ok'] for point in points),
                              "ko": error_count,
                              "1xx": sum(point['fields']['1xx'] for point in points),
                              "2xx": sum(point['fields']['2xx'] for point in points),
                              "3xx": sum(point['fields']['3xx'] for point in points),
                              "4xx": sum(point['fields']['4xx'] for point in points),
                              "5xx": sum(point['fields']['5xx'] for point in points),
                              "NaN": sum(point['fields']['NaN'] for point in points),
                              "min": float(response_times["min"]), "max": float(response_times["max"]),
                              "mean": float(response_times["mean"]), "pct50": response_times["pct50"],
                              "pct75": response_times["pct75"], "pct90": response_times["pct90"],
                              "pct95": response_times["pct95"], "pct99": response_times["pct99"]}})
    client.switch_database(args['comparison_db'])
    try:
        client.write_points(points)
    except Exception as e:
        print(e)
        print(f'Failed connection to {args["influx_host"]}, database - comparison')

    # Send comparison data to minio
    fields = ['time', '1xx', '2xx', '3xx', '4xx', '5xx', 'NaN', 'build_id', 'duration',
              'env', 'ko', 'max', 'mean', 'method', 'min', 'ok', 'pct50', 'pct75', 'pct90',
              'pct95', 'pct99', 'request_name', 'simulation', 'test_type', 'throughput', 'total', 'users']

    res = list(client.query("select * from api_comparison where build_id=\'{}\'".format(args['build_id'])).get_points())
    with open(f"/tmp/summary_table_{args['build_id']}.csv", "w", newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields)
        writer.writeheader()
        for line in res:
            writer.writerow(line)
    upload_test_results(args, f"/tmp/summary_table_{args['build_id']}.csv")

    return res, error_count


def upload_test_results(args, filename):
    bucket = args['simulation'].replace("_", "").lower()
    import gzip
    import shutil
    with open(filename, 'rb') as f_in:
        with gzip.open(f"{filename}.gz", 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    upload_file(f"{filename}.gz", args["base_url"], args["project_id"], args["token"], bucket=bucket)


def upload_file(file_name, galloper_url, project_id, token, bucket="reports"):
    file = {'file': open(f"{file_name}", 'rb')}
    try:
        requests.post(f"{galloper_url}/api/v1/artifacts/artifacts/{project_id}/{bucket}",
                      files=file, allow_redirects=True, headers={'Authorization': f"Bearer {token}"})
    except Exception as e:
        print(e)


def finish_test_report(args, response_times):
    headers = {
        'Authorization': f'bearer {args["token"]}',
        'Content-type': 'application/json'
    }
    test_status = {"status": "Finished", "percentage": 100, "description": "Test is finished"}
    lg_type = args["influx_db"].split("_")[0] if "_" in args["influx_db"] else args["influx_db"]
    data = {'build_id': args["build_id"], 'test_name': args["simulation"], 'lg_type': lg_type,
            'missed': int(0),
            'test_status': test_status,
            'vusers': args["users"],
            'duration': args['duration'], 'response_times': dumps(response_times)}
    url = f'{args["base_url"]}/api/v1/backend_performance/reports/{args["project_id"]}'
    r = requests.put(url, json=data, headers=headers)
    print(r.text)
    try:
        if r.json()["message"] == "updated":
            print("Post processing finished")
    except:
        print("Failed update report")
        data = {"test_status": {"status": "ERROR", "percentage": 100, "description": "Failed update report"}}
        headers = {'content-type': 'application/json', 'Authorization': f'bearer {args["token"]}'}
        url = f'{args["base_url"]}/api/v1/backend_performance/report_status/{args["project_id"]}/{args["report_id"]}'
        response = requests.put(url, json=data, headers=headers)
        try:
            print(response.json()["message"])
        except:
            print(response.text)

def _compare_with_baseline(args, baseline=None, last_build=None):
    comparison_metric = args['comparison_metric']
    compare_with_baseline = []
    if not baseline:
        print("Baseline not found")
        return 0, []
    for request in last_build:
        for baseline_request in baseline:
            if request['request_name'] == baseline_request['request_name']:
                if int(request[comparison_metric]) > int(baseline_request[comparison_metric]):
                    compare_with_baseline.append({"request_name": request['request_name'],
                                                    "response_time": request[comparison_metric],
                                                    "baseline": baseline_request[comparison_metric]
                                                 })
    performance_degradation_rate = round(float(len(compare_with_baseline) / len(last_build)) * 100, 2)
    return performance_degradation_rate, compare_with_baseline

def get_baseline(args):
    headers = {'Authorization': f'bearer {args["token"]}'}
    baseline_url = f"{args['base_url']}/api/v1/backend_performance/baseline/{args['project_id']}?" \
                   f"test_name={args['simulation']}&env={args['env']}"
    res = requests.get(baseline_url, headers={**headers, 'Content-type': 'application/json'}).json()
    return res["baseline"]

def _compare_with_thresholds(args, test, test_data, add_green=False):
    compare_with_thresholds = []
    total_checked = 0
    total_violated = 0
    headers = {'Authorization': f'bearer {args["token"]}'}
    thresholds_url = f"{args['base_url']}/api/v1/backend_performance/thresholds/{args['project_id']}?" \
                     f"test={args['simulation']}&env={args['env']}&order=asc"
    _thresholds = requests.get(thresholds_url, headers={**headers, 'Content-type': 'application/json'}).json()

    def compile_violation(request, th, total_checked, total_violated, compare_with_thresholds, add_green=False):
        total_checked += 1
        color, metric = compare_request_and_threhold(request, th)
        if add_green or color is not "green":
            compare_with_thresholds.append({
                "request_name": request['request_name'],
                "target": th['target'],
                "aggregation": th["aggregation"],
                "metric": metric,
                "threshold": color,
                "value": th["value"]
            })
        if color is not "green":
            total_violated += 1
        return total_checked, total_violated, compare_with_thresholds

    globaly_applicable: list = list(filter(lambda _th: _th['scope'] == 'all', _thresholds))
    every_applicable: list = list(filter(lambda _th: _th['scope'] == 'every', _thresholds))
    individual: list = list(filter(lambda _th: _th['scope'] != 'every' and _th['scope'] != 'all', _thresholds))
    individual_dict: dict = dict()
    for each in individual:
        if each['scope'] not in individual_dict:
            individual_dict[each['scope']] = []
        individual_dict[each['scope']].append(each)
    for request in test:
        thresholds = []
        targets = []
        if request['request_name'] in individual_dict:
            for ind in individual_dict[request['request_name']]:
                targets.append(ind['target'])
            thresholds.extend(individual_dict[request['request_name']])
        for th in every_applicable:
            if th['target'] not in targets:
                thresholds.append(th)
        for th in thresholds:
            total_checked, total_violated, compare_with_thresholds = compile_violation(
                request, th, total_checked, total_violated, compare_with_thresholds, add_green)
    if globaly_applicable:
        for th in globaly_applicable:
            total_checked, total_violated, compare_with_thresholds = compile_violation(
                test_data, th, total_checked, total_violated, compare_with_thresholds, add_green)
    violated = 0
    if total_checked:
        violated = round(float(total_violated / total_checked) * 100, 2)
    return total_checked, violated, compare_with_thresholds

def compare_request_and_threhold(request, threshold):
    COMPARISON_RULES = {"gte": "ge", "lte": "le", "gt": "gt", "lt": "lt", "eq": "eq"}
    comparison_method = getattr(operator, COMPARISON_RULES[threshold['comparison']])
    if threshold['target'] == 'response_time':
        metric = request[threshold['aggregation']] if threshold['aggregation'] != "avg" else request["mean"]
    elif threshold['target'] == 'throughput':
        metric = request['throughput']
    else:  # Will be in case error_rate is set as target
        metric = round(float(request['ko'] / request['total']) * 100, 2)
    if comparison_method(metric, threshold['value']):
        return "red", metric
    return "green", metric

# def trigger_task_with_smtp_integration(args, integration):
#     email_notification_id = integration["reporters"]["reporter_email"].get("task_id")
#     if email_notification_id:
#         emails = integration["reporters"]["reporter_email"].get("recipients", [])
#         if emails:
#             task_url = f"{galloper_url}/api/v1/tasks/task/{project_id}/{email_notification_id}"
#             event = {
#                 "galloper_url": galloper_url,
#                 "token": token,
#                 "project_id": project_id,
#                 "influx_host": args["influx_host"],
#                 "influx_port": args["influx_port"],
#                 "influx_user": args["influx_user"],
#                 "influx_password": args["influx_password"],
#                 "influx_db": args['influx_db'],
#                 "comparison_db": args['comparison_db'],
#                 "test": args['simulation'],
#                 "user_list": emails,
#                 "notification_type": "api",
#                 "test_type": args["type"],
#                 "env": args["env"],
#                 "users": users_count,
#                 "smtp_host": integration["reporters"]["reporter_email"]["integration_settings"]["host"],
#                 "smtp_port": integration["reporters"]["reporter_email"]["integration_settings"]["port"],
#                 "smtp_user": integration["reporters"]["reporter_email"]["integration_settings"]["user"],
#                 "smtp_sender": integration["reporters"]["reporter_email"]["integration_settings"]["sender"],
#                 "smtp_password": integration["reporters"]["reporter_email"]["integration_settings"]["passwd"],
#             }
#             if quality_gate_config.get('check_functional_errors'):
#                 event["error_rate"] = quality_gate_config['error_rate']
#             if quality_gate_config.get('check_performance_degradation'):
#                 event["performance_degradation_rate"] = quality_gate_config['performance_degradation_rate']
#             if quality_gate_config.get('check_missed_thresholds'):
#                 event["missed_thresholds"] = quality_gate_config['missed_thresholds_rate']
#
#             res = requests.post(task_url, json=event, headers={'Authorization': f'bearer {token}',
#                                                                'Content-type': 'application/json'})
#             logger.info("Email notification")
#             logger.info(res.text)


if __name__ == '__main__':
    timestamp = time()
    args = get_args()
    print("args")
    print(args)
    client = InfluxDBClient(args["influx_host"], args["influx_port"], args["influx_user"], args["influx_password"],
                            args["influx_db"])
    performance_degradation_rate, missed_threshold_rate = 0, 0
    compare_with_baseline, compare_with_thresholds = None, None
    try:
        response_times = get_response_times()
        comparison_data = get_comparison_data()
        current_test_results, error_count = send_summary_table_data(args, client, response_times, comparison_data, timestamp)

        # Compare with baseline
        try:
            baseline = get_baseline(args)
            performance_degradation_rate, compare_with_baseline = _compare_with_baseline(args, baseline, current_test_results)
        except Exception as e:
            print("Failed to compare with baseline")
            print(e)
        finish_test_report(args, response_times)

        # Compare with thresholds
        try:
            aggregated_test_data = {
                'throughput': round(float(args['total_requests_count']) / float(args['duration']), 3),
                'ko': error_count, 'total': args['total_requests_count'], 'request_name': 'all',
                "min": float(response_times["min"]), "max": float(response_times["max"]),
                "avg": float(response_times["mean"]), "pct50": response_times["pct50"],
                "pct75": response_times["pct75"], "pct90": response_times["pct90"],
                "pct95": response_times["pct95"], "pct99": response_times["pct99"]
            }
            total_checked_thresholds, missed_threshold_rate, compare_with_thresholds = _compare_with_thresholds(args, current_test_results, aggregated_test_data)
        except Exception as e:
            print(e)

        print("integrations")
        print(integrations)
        # if integrations and integrations.get("reporters") and "reporter_email" in integrations["reporters"].keys():
        #     trigger_task_with_smtp_integration(args, integrations)
    except Exception as e:
        print("Failed to update report")
        print(e)
    print(f"Finish main processing: {round(time() - timestamp, 2)} sec")
    upload_test_results(args, f"/tmp/{args['build_id']}.csv")
    upload_test_results(args, f"/tmp/users_{args['build_id']}.csv")
    client.switch_database(args['influx_db'])
    res = client.query(DELETE_TEST_DATA.format(args["simulation"], args["build_id"]))
    res2 = client.query(DELETE_USERS_DATA.format(args["build_id"]))
    print("DELETED **************")
    print(res)
    print(res2)
    print(f"Total execution time for reporter: {round(time() - timestamp, 2)} sec")