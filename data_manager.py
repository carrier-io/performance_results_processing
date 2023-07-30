import datetime
import operator
import requests
import csv
from influxdb import InfluxDBClient
from os import environ
from json import loads


DELETE_TEST_DATA = "delete from {} where build_id='{}'"
DELETE_USERS_DATA = "delete from \"users\" where build_id='{}'"
SELECT_LAST_BUILD_DATA = "select * from api_comparison where build_id=\'{}\'"
SELECT_HEALTH_CPU = '''
    SELECT 
        mean(\"usage_system\") as "system",
        mean(\"usage_user\") as "user",
        mean(\"usage_softirq\") as "softirq",
        mean(\"usage_iowait\") as "iowait"
    FROM \"cpu\" 
    WHERE build_id='{}'
    AND cpu='cpu-total' 
    AND time>='{}'
    AND time<='{}'
    GROUP BY time({}), host
        '''
SELECT_HEALTH_MEMORY = '''
    SELECT 
        HeapMemoryUsage.used as "heap memory", 
        NonHeapMemoryUsage.used as "non-heap memory"
    FROM "java_memory" 
    WHERE "build_id" = '{}'
    AND time >= '{}'
    AND time <= '{}'
    GROUP BY host
'''
SELECT_HEALTH_LOAD = '''
    SELECT 
        mean(load1) as "load1",
        mean(load5) as "load5",
        mean(load15) as "load15"
    FROM "system" 
    WHERE "build_id" = '{}'
    AND time >= '{}'
    AND time <= '{}'
    GROUP BY time({}), host
'''
COMPARISON_RULES = {"gte": "ge", "lte": "le", "gt": "gt", "lt": "lt", "eq": "eq"}


class DataManager():
    def __init__(self, args, s3_config, logger):
        self.args = args
        self.base_url = args['base_url']
        self.token = args["token"]
        self.build_id = self.args['build_id']
        self.project_id = args['project_id']
        self.start_time = self.args['start_time']
        self.end_time = self.args['end_time']
        self.last_build_data = None
        self.s3_config = s3_config
        self.headers = {'Authorization': f'bearer {args["token"]}'} if args["token"] else {}
        self.logger = logger
        self.client = self._get_client()

    def _get_client(self):
        return InfluxDBClient(host=self.args["influx_host"],
                              port=self.args['influx_port'],
                              username=self.args['influx_user'],
                              password=self.args['influx_password'])

    def delete_test_data(self):
        self.client.switch_database(self.args['influx_db'])
        self.client.query(DELETE_TEST_DATA.format(self.args["simulation"], self.build_id))
        self.client.query(DELETE_USERS_DATA.format(self.args["build_id"]))
        self.logger.info("Test data were deleted")

    @staticmethod
    def get_args():
        with open("/tmp/args.json", "r") as f:
            return loads(f.read())

    @staticmethod
    def get_response_times():
        with open("/tmp/response_times.csv", "r") as f:
            lines = f.readlines()
            headers = lines[0].split(",")
            values = lines[1].split(",")
            response_times = {}
            for i in range(len(headers)):
                response_times[headers[i].replace("\n", "")] = int(float(values[i].replace("\n", "")))
            return response_times

    @staticmethod
    def get_comparison_data():
        comparison_data = []
        with open("/tmp/comparison.csv", "r") as f:
            csv_reader = csv.DictReader(f)
            for row in csv_reader:
                comparison_data.append(row)
        return comparison_data

    def get_baseline(self):
        baseline_url = f"{self.base_url}/api/v1/backend_performance/baseline/{self.project_id}?" \
                    f"test_name={self.args['simulation']}&env={self.args['env']}"
        res = requests.get(baseline_url, headers={**self.headers, 'Content-type': 'application/json'}).json()
        return res["baseline"]

    def upload_test_results(self, filename):
        bucket = self.args['simulation'].replace("_", "").lower()
        import gzip
        import shutil
        with open(filename, 'rb') as f_in:
            with gzip.open(f"{filename}.gz", 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        self._upload_file(f"{filename}.gz", bucket=bucket)

    def _upload_file(self, file_name, bucket="reports"):
        file = {'file': open(f"{file_name}", 'rb')}
        try:
            requests.post(f"{self.base_url}/api/v1/artifacts/artifacts/{self.project_id}/{bucket}",
                          params=self.s3_config, files=file, allow_redirects=True, 
                          headers={'Authorization': f"Bearer {self.token}"})
        except Exception as e:
            self.logger.error(e)

    def send_summary_table_data(self, response_times, comparison_data, timestamp):
        points = []
        for req in comparison_data:
            influx_record = {
                "measurement": "api_comparison",
                "tags": {
                    "simulation": self.args['simulation'],
                    "env": self.args['env'],
                    "users": self.args["users"],
                    "test_type": self.args['type'],
                    "build_id": self.build_id,
                    "request_name": req['request_name'],
                    "method": req['method'],
                    "duration": self.args['duration']
                },
                "time": datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%SZ'),
                "fields": {
                    "throughput": round(float(req["total"]) / float(self.args['duration']), 3),
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
        points.append({"measurement": "api_comparison", "tags": {"simulation": self.args['simulation'],
                                                                "env": self.args['env'],
                                                                "users": self.args["users"],
                                                                "test_type": self.args['type'],
                                                                "duration": self.args['duration'],
                                                                "build_id": self.build_id,
                                                                "request_name": "All",
                                                                "method": "All"
                                                                },
                       "time": datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%SZ'),
                       "fields": {"throughput": round(float(self.args['total_requests_count']) / float(self.args['duration']), 3),
                                  "total": int(self.args['total_requests_count']),
                                  "ok": sum(point['fields']['ok'] for point in points),
                                  "ko": error_count,
                                  "1xx": sum(point['fields']['1xx'] for point in points),
                                  "2xx": sum(point['fields']['2xx'] for point in points),
                                  "3xx": sum(point['fields']['3xx'] for point in points),
                                  "4xx": sum(point['fields']['4xx'] for point in points),
                                  "5xx": sum(point['fields']['5xx'] for point in points),
                                  "NaN": sum(point['fields']['NaN'] for point in points),
                                  "min": float(response_times["min"]),
                                  "max": float(response_times["max"]),
                                  "mean": float(response_times["mean"]),
                                  "pct50": response_times["pct50"],
                                  "pct75": response_times["pct75"],
                                  "pct90": response_times["pct90"],
                                  "pct95": response_times["pct95"],
                                  "pct99": response_times["pct99"]}})
        self.client.switch_database(self.args['comparison_db'])
        try:
            self.client.write_points(points)
        except Exception as e:
            self.logger.error(e)
            self.logger.error(f'Failed connection to {self.args["influx_host"]}, database - comparison')

        # Send comparison data to minio
        fields = ['time', '1xx', '2xx', '3xx', '4xx', '5xx', 'NaN', 'build_id', 'duration',
                'env', 'ko', 'max', 'mean', 'method', 'min', 'ok', 'pct50', 'pct75', 'pct90',
                'pct95', 'pct99', 'request_name', 'simulation', 'test_type', 'throughput', 'total', 'users']

        res = list(self.client.query(SELECT_LAST_BUILD_DATA.format(self.build_id)).get_points())
        with open(f"/tmp/summary_table_{self.build_id}.csv", "w", newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fields)
            writer.writeheader()
            for line in res:
                writer.writerow(line)
        self.upload_test_results(f"/tmp/summary_table_{self.build_id}.csv")
        return res, error_count

    def send_engine_health_cpu(self):
        fields = "time,system,user,softirq,iowait,host"
        self.client.switch_database(self.args['telegraf_db'])
        for each in ["1s", "5s", "30s", "1m", "5m", "10m"]:
            _results = self.client.query(SELECT_HEALTH_CPU.format(self.build_id, self.start_time, self.end_time, each))
            with open(f"/tmp/health_cpu_{self.build_id}_{each}.csv", "w", newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fields.split(','))
                writer.writeheader()
                for (_, groups), series in _results.items():
                    for line in series:
                        writer.writerow({**line, **groups})
            self.upload_test_results(f"/tmp/health_cpu_{self.build_id}_{each}.csv")
        self.client.switch_database(self.args['influx_db'])
        print("********engine_health_cpu done")

    def send_engine_health_memory(self):
        fields = "time,heap memory,non-heap memory,host"
        self.client.switch_database(self.args['telegraf_db'])
        _results = self.client.query(SELECT_HEALTH_MEMORY.format(self.build_id, self.start_time, self.end_time))
        with open(f"/tmp/health_memory_{self.build_id}.csv", "w", newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fields.split(','))
            writer.writeheader()
            for (_, groups), series in _results.items():
                for line in series:
                    writer.writerow({**line, **groups})
        self.upload_test_results(f"/tmp/health_memory_{self.build_id}.csv")
        self.client.switch_database(self.args['influx_db'])
        print("********engine_health_memory done")

    def send_engine_health_load(self):
        fields = "time,load1,load5,load15,host"
        self.client.switch_database(self.args['telegraf_db'])
        for each in ["1s", "5s", "30s", "1m", "5m", "10m"]:
            _results = self.client.query(SELECT_HEALTH_LOAD.format(self.build_id, self.start_time, self.end_time, each))
            with open(f"/tmp/health_load_{self.build_id}_{each}.csv", "w", newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fields.split(','))
                writer.writeheader()
                for (_, groups), series in _results.items():
                    for line in series:
                        writer.writerow({**line, **groups})
            self.upload_test_results(f"/tmp/health_load_{self.build_id}_{each}.csv")
        self.client.switch_database(self.args['influx_db'])
        print("********engine_health_load done")


    def send_loki_errors(self):
        url = f"{self.args['loki_host']}:{self.args['loki_port']}/loki/api/v1/query_range"
        data = {
            "direction": "BACKWARD",
            "limit": 5000,
            "query": '{filename="/tmp/' + self.args['simulation'] + '.log"}',
            "start": self.start_time,
            "end": self.end_time
        }
        results = requests.get(url, params=data, headers={"Content-Type": "application/json"}).json()
        t_format = "%Y-%m-%dT%H:%M:%SZ"
        fields = ['time',
                  'Error key',
                  'Request name',
                  'Method',
                  'Response code',
                  'URL',
                  'Error message',
                  'Request params',
                  'Headers',
                  'Response body'
                  ]
        issues = []
        for result in results["data"]["result"]:
            for line in result['values']:
                issue = {'time': datetime.datetime.fromtimestamp(int(line[0])/1000000000).strftime(t_format)}
                values = line[1].strip().split("\t")
                for value in values:
                    if ": " in value:
                        k, v = value.split(": ", 1)
                        if k in fields:
                            issue[k] = v
                if 'Error key' in issue.keys():
                    issues.append(issue)
        with open(f"/tmp/errors_{self.build_id}.csv", "w", newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fields)
            writer.writeheader()
            for line in issues:
                writer.writerow(line)
        self.upload_test_results(f"/tmp/errors_{self.build_id}.csv")
        print("loki_errors done")

    def compare_with_thresholds(self, test, test_data, quality_gate_config, add_green=False):
        compare_with_thresholds = []
        total_checked = 0
        total_violated = 0
        headers = {'Authorization': f'bearer {self.token}'}
        thresholds_url = f"{self.base_url}/api/v1/backend_performance/thresholds/{self.project_id}?" \
                        f"test={self.args['simulation']}&env={self.args['env']}&order=asc"
        _thresholds = requests.get(thresholds_url, headers={**headers, 'Content-type': 'application/json'}).json()

        def compile_violation(request, th, total_checked, total_violated, compare_with_thresholds, 
                              quality_gate_config, add_green=False):
            color, metric = self._compare_request_and_threhold(request, th, quality_gate_config, is_summary=False)
            if color:
                total_checked += 1
                if add_green or color == "red":
                    compare_with_thresholds.append({
                        "request_name": request['request_name'],
                        "target": th['target'],
                        "aggregation": th["aggregation"],
                        "metric": metric,
                        "threshold": color,
                        "value": th["value"]
                    })
                if color == "red":
                    total_violated += 1
            return total_checked, total_violated, compare_with_thresholds

        def compile_globals(request, th, compare_with_globaly_applicable, quality_gate_config):
            color, metric = self._compare_request_and_threhold(request, th, quality_gate_config, is_summary=True)
            if color == "red":
                compare_with_globaly_applicable.append({
                    "type": "SLA",
                    "status": "Failed",
                    "message": f"{th['target']} for current test exceeded SLA value - {th['value']}%"
                })
            return compare_with_globaly_applicable

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
                    request, th, total_checked, total_violated, compare_with_thresholds, quality_gate_config, add_green)
        compare_with_globaly_applicable = []
        if globaly_applicable:
            for th in globaly_applicable:
                compare_with_globaly_applicable = compile_globals(test_data, th, compare_with_globaly_applicable, 
                                                                  quality_gate_config)
        violated = 0
        if total_checked:
            violated = round(float(total_violated / total_checked) * 100, 2)
        return total_checked, violated, compare_with_thresholds, compare_with_globaly_applicable

    @staticmethod
    def _compare_request_and_threhold(request, threshold, quality_gate_config, is_summary=True):
        # comparison_method = getattr(operator, COMPARISON_RULES[threshold['comparison']])
        # if threshold['target'] == 'response_time':
        #     metric = request[threshold['aggregation']] if threshold['aggregation'] != "avg" else request["mean"]
        # elif threshold['target'] == 'throughput':
        #     metric = request['throughput']
        # else:  # Will be in case error_rate is set as target
        #     metric = round(float(request['ko'] / request['total']) * 100, 2)
        # if comparison_method(metric, threshold['value']):
        #     return "red", metric
        # return "green", metric
        
        if is_summary:
            quality_gate = quality_gate_config.get("settings", {}).get("summary_results", {})
        else:
            quality_gate = quality_gate_config.get("settings", {}).get("per_request_results", {})
        comparison_method = getattr(operator, COMPARISON_RULES[threshold['comparison']])

        if quality_gate.get("check_response_time") and threshold['target'] == 'response_time':
            metric = request[threshold['aggregation']] if threshold['aggregation'] != "avg" else request["mean"]
            div_value = quality_gate.get("response_time_deviation")
        elif quality_gate.get("check_throughput") and threshold['target'] == 'throughput':
            metric = request['throughput']
            div_value = quality_gate.get("throughput_deviation")
        elif quality_gate.get("check_error_rate") and threshold['target'] == 'error_rate':
            metric = round(float(request['ko'] / request['total']) * 100, 2)
            div_value = quality_gate.get("error_rate_deviation")
        else:
            return None, None
        if threshold['comparison'] in ('gte', 'gt'):
            if comparison_method(metric, threshold['value'] + div_value):
                return "red", metric
        elif threshold['comparison'] in ('lte', 'lt'):
            if comparison_method(metric, threshold['value'] - div_value):
                return "red", metric
        elif threshold['comparison'] in ('eq',):
            if metric > (threshold['value'] + div_value) or metric < (threshold['value'] - div_value):
                return "red", metric
        return "green", metric

    # def compare_with_baseline(self, baseline=None, last_build=None):
    #     comparison_metric = self.args['comparison_metric']
    #     compare_with_baseline = []
    #     if not baseline:
    #         self.logger.warning("Baseline not found")
    #         return 0, []
    #     for request in last_build:
    #         for baseline_request in baseline:
    #             if request['request_name'] == baseline_request['request_name']:
    #                 if int(request[comparison_metric]) > int(baseline_request[comparison_metric]):
    #                     compare_with_baseline.append({"request_name": request['request_name'],
    #                                                     "response_time": request[comparison_metric],
    #                                                     "baseline": baseline_request[comparison_metric]
    #                                                 })
    #     performance_degradation_rate = round(float(len(compare_with_baseline) / len(last_build)) * 100, 2)
    #     return performance_degradation_rate, compare_with_baseline

    def compare_with_baseline_summary(self, baseline_summary, current_test_summary, quality_gate_config):
        compare_baseline_summary = []
        
        if quality_gate_config.get("settings", {}).get("summary_results", {}).get("check_error_rate"):
            div_value = quality_gate_config.get("settings", {}).get("summary_results", {}).get("error_rate_deviation")
            baseline_error_rate = round(float(baseline_summary["ko"] / baseline_summary["total"] * 100), 2)
            current_test_error_rate = round(float(current_test_summary["ko"] / current_test_summary["total"] * 100), 2)
            error_rate_div = current_test_error_rate - baseline_error_rate
            if error_rate_div > div_value:
                _res = {"type": "Baseline error rate", 
                        "status": "Failed", 
                        "message": f"Error rate for current test results - {current_test_error_rate}% is higher than error rate for baseline test - {baseline_error_rate}%"
                        }
            else:
                _res = {"type": "Baseline error rate", 
                        "status": "Success", 
                        "message": "Error rate for current test results is less or equal to error rate for baseline test"
                        }
            compare_baseline_summary.append(_res)
            
        if quality_gate_config.get("settings", {}).get("summary_results", {}).get("check_throughput"):
            div_value = quality_gate_config.get("settings", {}).get("summary_results", {}).get("throughput_deviation")
            throughput_div = float(baseline_summary["throughput"]) - float(current_test_summary["throughput"])
            if throughput_div > div_value:
                _res = {"type": "Baseline throughput", 
                        "status": "Failed", 
                        "message": f"Throughput for current test results - {current_test_summary['throughput']} req/sec is less baseline - {baseline_summary['throughput']} req/sec"
                        }
            else:
                _res = {"type": "Baseline throughput", 
                        "status": "Success", 
                        "message": "Throughput for current test results is higher or equal to throughput for baseline test"}    
            compare_baseline_summary.append(_res)

        comparison_metric = quality_gate_config["baseline"].get('rt_baseline_comparison_metric', 'pct95')
        if quality_gate_config.get("settings", {}).get("summary_results", {}).get("check_response_time"):
            div_value = quality_gate_config.get("settings", {}).get("summary_results", {}).get("response_time_deviation")
            response_time_div = int(current_test_summary[comparison_metric]) - int(baseline_summary[comparison_metric])
            if response_time_div > div_value:
                _res = {"type": "Baseline response time", 
                        "status": "Failed",
                        "message": f"Response time for current test results by {comparison_metric} - {current_test_summary[comparison_metric]} ms is higher than response time for baseline test - {baseline_summary[comparison_metric]} ms"
                        }
            else:
                _res = {"type": "Baseline response time", 
                        "status": "Success",
                        "message": f"Response time for current test results by {comparison_metric} is less or equal to response time for baseline test"
                        }
            compare_baseline_summary.append(_res)

        return compare_baseline_summary

    def compare_with_baseline_per_request(self, baseline, current_test_results, quality_gate_config):
        comparison_metric = quality_gate_config["baseline"].get('rt_baseline_comparison_metric', 'pct95')
        compare_baseline_per_request = []
        compare_baseline_per_request_details = []
        total_checks, failed = 0, 0
        for _baseline_res in baseline:
            for _res in current_test_results:
                if _baseline_res['method'] != 'All' and _baseline_res['request_name'] == _res['request_name'] and \
                        _baseline_res['method'] == _res['method']:
                    if quality_gate_config.get("settings", {}).get("per_request_results", {}).get("check_error_rate"):
                        total_checks += 1
                        baseline_error_rate = round(float(_baseline_res["ko"] / _baseline_res["total"] * 100), 2)
                        current_test_error_rate = round(float(_res["ko"] / _res["total"] * 100), 2)
                        error_rate_div = current_test_error_rate - baseline_error_rate
                        div_value = quality_gate_config.get("settings", {}).get("per_request_results", {}).get("error_rate_deviation")
                        if error_rate_div > div_value:
                            failed += 1
                            compare_baseline_per_request_details.append({"request_name": _res['request_name'],
                                                                         "target": "error_rate",
                                                                         "metric": current_test_error_rate,
                                                                         "baseline": baseline_error_rate
                                                                         })
                            
                    if quality_gate_config.get("settings", {}).get("per_request_results", {}).get("check_throughput"):
                        total_checks += 1
                        div_value = quality_gate_config.get("settings", {}).get("summary_results", {}).get("throughput_deviation")
                        throughput_div = float(_baseline_res["throughput"]) - float(_res["throughput"])
                        if throughput_div > div_value:
                            failed += 1
                            compare_baseline_per_request_details.append({"request_name": _res['request_name'],
                                                                         "target": "throughput",
                                                                         "metric": _res["throughput"],
                                                                         "baseline": _baseline_res["throughput"]
                                                                         })
                            
                    if quality_gate_config.get("settings", {}).get("per_request_results", {}).get("check_response_time"):
                        total_checks += 1
                        div_value = quality_gate_config.get("settings", {}).get("per_request_results", {}).get("response_time_deviation")
                        response_time_div = int(_res[comparison_metric]) - int(_baseline_res[comparison_metric])
                        if response_time_div > div_value:
                            failed += 1
                            compare_baseline_per_request_details.append({"request_name": _res['request_name'],
                                                                         "target": "response_time",
                                                                         "metric": _res[comparison_metric],
                                                                         "baseline": _baseline_res[comparison_metric]
                                                                         })

        failed_requests_rate = round(float(failed / total_checks * 100), 2)
        qg_failed_requests_rate = quality_gate_config.get("settings", {}).get("per_request_results", {}).get("percentage_of_failed_requests", 20)
        if failed_requests_rate > qg_failed_requests_rate:
            compare_baseline_per_request.append(
                {"type": "Baseline compare per request", 
                 "status": "Failed",
                 "message": f"Percentage of failed requests compare to baseline is more than {qg_failed_requests_rate}%"
                 })
        else:
            compare_baseline_per_request.append(
                {"type": "Baseline compare per request", 
                 "status": "Success",
                 "message": f"Percentage of failed requests compare to baseline is less than {qg_failed_requests_rate}%"
                 })

        return compare_baseline_per_request, compare_baseline_per_request_details

    def get_aggregated_errors(self, quality_gate_config):
        aggregated_errors = {}
        if quality_gate_config.get("settings", {}).get("summary_results", {}).get("check_error_rate"):
            with open(f"/tmp/errors_{self.build_id}.csv", "r") as csvfile:
                errors = csv.DictReader(csvfile)
                for each in errors:
                    if each['Error key'] in aggregated_errors:
                        aggregated_errors[each['Error key']]['Error count'] += 1
                    else:
                        aggregated_errors[each['Error key']] = {
                            'Request name': each['Request name'],
                            'Method': each['Method'],
                            'Request headers': each['Headers'],
                            'Error count': 1,
                            'Response code': each['Response code'],
                            'Request URL': each['URL'],
                            'Request_params': [each['Request params']],
                            'Response': [each['Response body']],
                            'Error_message': [each['Error message']],
                        }
        return aggregated_errors
