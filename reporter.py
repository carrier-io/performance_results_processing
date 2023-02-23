from json import loads, dumps
import csv
import datetime
from time import time
from influxdb import InfluxDBClient
import requests
from traceback import format_exc


DELETE_TEST_DATA = "delete from {} where build_id='{}'"
DELETE_USERS_DATA = "delete from \"users\" where build_id='{}'"

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


def send_summary_table_data(args, response_times, comparison_data, timestamp):
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
    points.append({"measurement": "api_comparison", "tags": {"simulation": args['simulation'],
                                                             "env": args['env'], "users": args["users"],
                                                             "test_type": args['type'], "duration": args['duration'],
                                                             "build_id": args['build_id'],
                                                             "request_name": "All", "method": "All"},
                   "time": datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%SZ'),
                   "fields": {"throughput": round(float(args['total_requests_count']) / float(args['duration']), 3),
                              "total": int(args['total_requests_count']),
                              "ok": sum(point['fields']['ok'] for point in points),
                              "ko": sum(point['fields']['ko'] for point in points),
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
    client = InfluxDBClient(args["influx_host"], args["influx_port"], args["influx_user"], args["influx_password"],
                            args["comparison_db"])
    try:
        client.write_points(points)
    except Exception as e:
        print(e)
        print(f'Failed connection to {args["influx_host"]}, database - comparison')

    # Send comparison data to minio
    # TODO refactor to not query InfluxDB as we already have all necessary data
    fields = ['time', '1xx', '2xx', '3xx', '4xx', '5xx', 'NaN', 'build_id', 'duration',
              'env', 'ko', 'max', 'mean', 'method', 'min', 'ok', 'pct50', 'pct75', 'pct90',
              'pct95', 'pct99', 'request_name', 'simulation', 'test_type', 'throughput', 'total', 'users']

    res = client.query("select * from api_comparison where build_id=\'{}\'".format(args['build_id'])).get_points()

    with open(f"/tmp/summary_table_{args['build_id']}.csv", "w", newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields)
        writer.writeheader()
        for line in res:
            writer.writerow(line)
    upload_test_results(args, f"/tmp/summary_table_{args['build_id']}.csv")

    print("********summary_table Done")
    return client


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


if __name__ == '__main__':
    timestamp = time()
    print("Reporter *****************")
    args = get_args()
    print("args")
    print(args)
    client = InfluxDBClient(args["influx_host"], args["influx_port"], args["influx_user"], args["influx_password"],
                            args["influx_db"])
    try:
        response_times = get_response_times()
        print("response_times")
        print(response_times)
        comparison_data = get_comparison_data()
        send_summary_table_data(args, response_times, comparison_data, timestamp)
        finish_test_report(args, response_times)
    except Exception as e:
        print("Failed to update report")
        print(e)
    print(f"Finish main processing: {round(time() - timestamp, 2)} sec")
    upload_test_results(args, f"/tmp/{args['build_id']}.csv")
    upload_test_results(args, f"/tmp/users_{args['build_id']}.csv")
    res = client.query(DELETE_TEST_DATA.format(args["simulation"], args["build_id"]))
    res2 = client.query(DELETE_USERS_DATA.format(args["build_id"]))
    print("DELETED **************")
    print(res)
    print(res2)
    print(f"Total execution time for reporter: {round(time() - timestamp, 2)} sec")