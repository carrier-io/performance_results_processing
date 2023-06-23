from os import environ
import requests
from influxdb import InfluxDBClient
from time import sleep, time
import datetime
from json import loads, dumps


result_fields = "time,request_name,method,response_time,status,status_code,lg_id"
user_fields = "time,active,lg_id"
TOTAL_REQUEST_COUNT = "select count(\"response_time\") from {} where build_id='{}'"
SELECT_USERS_COUNT = "select sum(\"max\") from (select max(\"user_count\") from \"users\" where " \
                     "build_id='{}' group by lg_id)"
SELECT_USERS_DATA = "select time, active, lg_id from \"users\" where build_id=\'{}\' and time>\'{}\'"
SELECT_REQUESTS_DATA = "select time, request_name, lg_id, method, response_time, status, status_code from {} " \
                       "where build_id=\'{}\' and time>\'{}\'"
build_id = environ.get("build_id")
base_url = environ.get("base_url")
project_id = environ.get("project_id")
token = environ.get("token")
report_id = environ.get("report_id")
exec_params = loads(environ.get("exec_params"))
results_file_name = f"/tmp/{build_id}.csv"
users_file_name = f"/tmp/users_{build_id}.csv"


headers = {'Authorization': f'bearer {token}'}


def get_args():
    r = requests.get(f'{base_url}/api/v1/backend_performance/reports/{project_id}?report_id={report_id}',
                     headers={**headers, 'Content-type': 'application/json'}).json()
    args = {'base_url': base_url, 'project_id': project_id, 'token': token, 'type': r['type'], 'simulation': r['name'],
            'build_id': r['build_id'], 'report_id': report_id, 'env': r['environment'], 'comparison_metric': 'pct95',
            'test_limit': 5, "influx_host": exec_params["influxdb_host"], "influx_port": "8086",
            "influx_user": exec_params["influxdb_user"], "influx_password": exec_params["influxdb_password"],
            "influx_db": exec_params["influxdb_database"], "comparison_db": exec_params["influxdb_comparison"],
            "telegraf_db": exec_params["influxdb_telegraf"], "loki_host": exec_params["loki_host"],
            "loki_port": exec_params["loki_port"]}

    return args


def get_test_status():
    url = f'{base_url}/api/v1/backend_performance/report_status/{project_id}/{report_id}'
    res = requests.get(url, headers=headers)
    return res.json()


def run(args):
    test_name = args["simulation"]
    with open(results_file_name, "a+") as f:
        f.write(f"{result_fields}\n")
    with open(users_file_name, "a+") as f:
        f.write(f"{user_fields}\n")
    client = InfluxDBClient(args["influx_host"], args["influx_port"], args["influx_user"], args["influx_password"],
                            args["influx_db"])
    requests_last_read_time = '1970-01-01T19:25:26.005Z'
    users_last_read_time = '1970-01-01T19:25:26.005Z'
    iteration = 0
    processing_time = 0
    req_count = 0
    start_time, end_time = "", ""
    while True:
        try:
            iteration += 1
            pause = 60 - processing_time if processing_time < 60 else 1
            sleep(pause)
            tik = time()
            status = get_test_status()
            print(f"Status: {status['message']}")
            requests_data = list(
                client.query(SELECT_REQUESTS_DATA.format(test_name, build_id, requests_last_read_time)).get_points())
            users_data = list(client.query(SELECT_USERS_DATA.format(build_id, users_last_read_time)).get_points())
            if not start_time:
                start_time = requests_data[0]['time']
            if status["message"] in ["Post processing", "Canceled", "Failed"] and len(requests_data) == 0:
                print("Looks like tests are done")
                break
            if status["message"] in ["Failed"]:
                print("Got failed status message")
                break
            if requests_data:
                requests_last_read_time = requests_data[-1]['time']
                users_last_read_time = users_data[-1]['time']
                req_count += len(requests_data)

                # results
                with open(results_file_name, "a+") as f:
                    for _res in requests_data:
                        _result_line = ""
                        try:
                            for _field in result_fields.split(","):
                                if _field == "response_time":
                                    int(_res[_field])
                                _result_line += f'{_res[_field]},'
                            f.write(f"{_result_line[:-1]}\n")
                        except:
                            req_count -= 1

                # users
                with open(users_file_name, "a+") as f:
                    for each in users_data:
                        _result_line = ""
                        for _field in user_fields.split(","):
                            _result_line += f'{each[_field]},'
                        f.write(f"{_result_line[:-1]}\n")

            processing_time = round(time() - tik, 2)
            print(f"Iteration: {iteration}")
            print(f"req_count: {req_count}")
            print(f"Total time - {processing_time} sec")
        except Exception as e:
            print(e)
            sleep(30)
    _ts = time()
    try:
        print("Lets check total requests count ...")
        total = int(list(client.query(TOTAL_REQUEST_COUNT.format(test_name, build_id)).get_points())[0]["count"])
        print(f"Total from influx: {total}. Total from PP: {req_count}. {total == req_count}")
        args['total_requests_count'] = req_count
        args["start_time"] = start_time
        args["end_time"] = requests_last_read_time

        # get users count
        data = client.query(SELECT_USERS_COUNT.format(build_id))
        data = list(data.get_points())[0]
        users = int(data['sum'])

        # calculate duration
        start_time = int(str(datetime.datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()).split(".")[0])
        end_time = int(
            str(datetime.datetime.strptime(requests_last_read_time, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()).split(".")[0])
        duration = end_time - start_time

        args["duration"] = duration
        args["users"] = users
    except Exception as e:
        print(e)
        args['total_requests_count'] = req_count
        args["start_time"] = requests_last_read_time
        args["end_time"] = requests_last_read_time
        args["duration"] = 0
        args["users"] = 0
    with open("/tmp/args.json", "w") as f:
        f.write(dumps(args))
    other_processing_time = round(time() - _ts, 2)
    print(f"other_processing_time: {other_processing_time} sec")


if __name__ == '__main__':
    args = get_args()
    run(args)