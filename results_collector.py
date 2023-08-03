import asyncio
import csv
import logging
from pathlib import Path
from typing import Generator, Tuple

import requests
from influxdb import InfluxDBClient
from time import time
from datetime import datetime

from models import CollectorConfig, TestData, InfluxQueries, TestStatus, AllArgs, TestStatuses
from utils import build_api_url, get_loki_logger


class Collector:
    def __init__(self, config: CollectorConfig | dict | None = None):
        if config:
            if isinstance(config, dict):
                self.config = CollectorConfig.model_validate(config)
            else:
                self.config = config
        else:
            self.config = CollectorConfig.from_env()

        try:
            self.logger = get_loki_logger(
                loki_host=self.config.exec_params.loki_host,
                loki_port=self.config.exec_params.loki_port,
                build_id=self.config.build_id,
                report_id=self.config.report_id,
                project_id=self.config.project_id,
                hostname=self.config.logger_hostname,
                logger_stop_words=self.config.logger_stop_words
            )
        except:
            self.logger = logging.getLogger()

        self.test_data = self._fetch_test_data()
        self._test_status = self.test_data.test_status
        self.logger.info(f'Test status: {self.test_status}')
        self.exit_if_needed()

        if self.config.manual_run:
            new_status = TestStatus(
                status=TestStatuses.POST_PROCESSING,
                percentage=80,
                description='Manually triggered post processing'
            )
            self.logger.info(f'Setting status to: {new_status.status}')
            self.set_test_status(new_status)
        self.influx_queries = InfluxQueries(
            test_name=self.test_data.name,
            request_data_fields=self.config.result_fields,
            user_fields=self.config.user_fields,
        )
        self.requests_start_time = None
        self.requests_end_time = None
        if self.config.debug:
            self.logger.info(f'Config:\n{self.config.model_dump_json(indent=2)}')

    def _fetch_test_data(self) -> TestData:
        url = '/'.join(map(str, [
            self.config.base_url,
            build_api_url('backend_performance', 'reports').lstrip('/'),
            self.config.project_id
        ]))
        params = {'report_id': self.config.report_id}
        r = requests.get(url, headers=self.config.api_headers, params=params)
        if not r.ok:
            raise Exception('Could not fetch test params from centry by url: %s' % url)
        return TestData.model_validate(r.json())

    @property
    def test_status(self) -> TestStatus:
        delta = datetime.utcnow() - self._test_status._updated
        if delta.total_seconds() > self.config.test_status_update_interval:
            self._test_status = self._get_test_status()
            self.logger.info(f'Refreshing test status: {self._test_status}')
            self.exit_if_needed()
        return self._test_status

    def exit_if_needed(self) -> None:
        if self._test_status.test_finalized and not self.config.manual_run:
            self.logger.critical('Test got the finalized status. This post processor will be terminated')
            exit(32)

    def _get_test_status(self) -> TestStatus:
        resp = requests.get(
            self.config.report_status_url,
            headers=self.config.api_headers
        ).json()
        return TestStatus(status=resp['message'])

    def set_test_status(self, status: TestStatus) -> TestStatus:
        resp = requests.put(
            self.config.report_status_url,
            headers=self.config.api_headers,
            json={"test_status": status.model_dump(exclude_none=True)}
        )
        if resp.ok:
            new_status = resp.json()['message'].lower()
            if not self.test_status.status == new_status:
                self.logger.warning(f'Statuses are not equal: {self.test_status.status} != {new_status}')
            self._test_status = status
        return self._test_status

    def get_influx_client(self) -> InfluxDBClient:
        return InfluxDBClient(
            host=self.config.exec_params.influxdb_host,
            port=self.config.exec_params.influxdb_port,
            username=self.config.exec_params.influxdb_user,
            password=self.config.exec_params.influxdb_password,
            database=self.config.exec_params.influxdb_database
        )

    def dump_to_csv(self,
                    file_path: Path,
                    data_chunk: Generator,
                    headers: list | None = None
                    ) -> Tuple[dict, int]:
        first_row = None
        last_row = None
        file_exists = file_path.exists()
        total_rows = 0

        if not headers:
            first_row = next(data_chunk)
            last_row = first_row
            total_rows += 1
            headers = first_row.keys()

        with open(file_path, 'a', newline='') as csvfile:
            csv_writer = csv.DictWriter(csvfile, fieldnames=headers)

            if not file_exists:
                csv_writer.writeheader()
            if first_row:
                csv_writer.writerow(first_row)

            for row in data_chunk:
                if not first_row:
                    first_row = row

                csv_writer.writerow(row)
                last_row = row
                total_rows += 1

        if file_path == self.config.results_file_path:
            if not self.requests_start_time:
                try:
                    self.requests_start_time = datetime.fromisoformat(first_row['time'].strip('Z'))
                except TypeError:
                    ...

        return last_row, total_rows

    def query_requests_data(self, client: InfluxDBClient, params: dict) -> Tuple[dict, int]:
        where = ' where ' + self.influx_queries.where(params)
        query = self.influx_queries.requests_data.format(where=where)
        query += f' limit {self.config.influx_query_limit}'
        req_data = client.query(query).get_points()
        return self.dump_to_csv(self.config.results_file_path, req_data, self.config.result_fields)

    def query_users_data(self, client: InfluxDBClient, params: dict) -> Tuple[dict, int]:
        where = ' where ' + self.influx_queries.where(params)
        query = self.influx_queries.users_data.format(where=where)
        query += f' limit {self.config.influx_query_limit}'
        req_data = client.query(query).get_points()
        return self.dump_to_csv(self.config.users_file_path, req_data, self.config.user_fields)

    async def collect_requests(self, client: InfluxDBClient) -> Tuple[int, float]:
        total_proc_time = 0
        total_rows = 0
        params = {
            'build_id=': self.config.build_id,
            'time>': 0
        }
        empty_attempts = 0
        sleep_time = self.config.iteration_sleep
        stop_collection = False
        while not stop_collection:
            self.logger.info(f'Collecting requests: Sleeping: {sleep_time}')
            await asyncio.sleep(sleep_time)
            iteration_start = time()
            row_count = None
            self.logger.info('Collecting requests: Start iteration')
            while row_count is None or row_count == self.config.influx_query_limit:
                self.logger.info('Collecting requests: Querying influx')
                last_row, row_count = self.query_requests_data(
                    client, params
                )
                total_rows += row_count
                try:
                    params['time>'] = last_row['time']
                    self.requests_end_time = datetime.fromisoformat(last_row['time'].strip('Z'))
                    empty_attempts = 0
                except TypeError:
                    if empty_attempts >= self.config.max_empty_attempts:
                        if not self.test_status.test_finished:
                            self.logger.info('Collecting requests: Exceeded max attempts. Assuming test is stuck')
                        self.logger.info('Collecting requests: Done')
                        stop_collection = True
                    else:
                        empty_attempts += 1
                        self.logger.info(f'Collecting requests: Got empty response. '
                                         f'Attempt: {empty_attempts}/{self.config.max_empty_attempts}')
                        if self.test_status.test_finished:
                            self.logger.info('Assuming test finished')
                            self.logger.info('Collecting requests: Done')
                            stop_collection = True
                proc_time = time() - iteration_start
                total_proc_time += proc_time
                self.logger.info(f'Collecting requests: proc_time: {proc_time:.3}s')
                self.logger.info(f'Collecting requests: Requests processed: {row_count}')

                sleep_time = max(
                    min(self.config.iteration_sleep, 1),
                    int(self.config.iteration_sleep - proc_time)
                )
        return total_rows, total_proc_time

    async def collect_users(self, client: InfluxDBClient) -> Tuple[int, float]:
        total_proc_time = 0
        total_rows = 0
        params = {
            'build_id=': self.config.build_id,
            'time>': 0
        }
        empty_attempts = 0
        sleep_time = self.config.iteration_sleep + self.config.iteration_sleep // 2  # task delay
        stop_collection = False
        while not stop_collection:
            self.logger.info(f'Collecting users: Sleeping: {sleep_time}')
            await asyncio.sleep(sleep_time)
            iteration_start = time()
            row_count = None
            self.logger.info('Collecting users: Start iteration')
            while row_count is None or row_count == self.config.influx_query_limit:
                self.logger.info('Collecting users: Querying influx')
                last_row, row_count = self.query_users_data(
                    client, params
                )
                total_rows += row_count
                try:
                    params['time>'] = last_row['time']
                    empty_attempts = 0
                except TypeError:
                    if empty_attempts >= self.config.max_empty_attempts:
                        if not self.test_status.test_finished:
                            self.logger.info('Collecting users: Exceeded max attempts. Assuming test is stuck')
                        self.logger.info('Collecting users: Done')
                        stop_collection = True
                    else:
                        empty_attempts += 1
                        self.logger.info(f'Collecting users: Got empty response. '
                                         f'Attempt: {empty_attempts}/{self.config.max_empty_attempts}')
                        if self.test_status.test_finished:
                            self.logger.info('Assuming test finished')
                            self.logger.info('Collecting users: Done')
                            stop_collection = True
                proc_time = time() - iteration_start
                total_proc_time += proc_time
                self.logger.info(f'Collecting users: proc_time: {proc_time:.3}s')
                self.logger.info(f'Collecting users: Users processed: {row_count}')

                sleep_time = max(
                    min(self.config.iteration_sleep, 1),
                    int(self.config.iteration_sleep - proc_time)
                )
        return total_rows, total_proc_time

    def collect_users_count(self, client: InfluxDBClient) -> int:
        where = ' where ' + self.influx_queries.where({'build_id=': self.config.build_id})
        query = self.influx_queries.users_count.format(where=where)
        req_data = client.query(query).get_points()
        try:
            return int(next(req_data)['sum'])
        except StopIteration:
            return 0

    async def accumulate_data(self) -> None:
        client = self.get_influx_client()

        requests_task = asyncio.Task(self.collect_requests(client))
        users_task = asyncio.Task(self.collect_users(client))

        await asyncio.gather(requests_task, users_task)
        req_total_rows, req_total_proc_time = requests_task.result()
        self.logger.info(f'Requests start time: {self.requests_start_time}')
        self.logger.info(f'Requests end time: {self.requests_end_time}')
        self.logger.info(
            f'Requests collector processed {req_total_rows} rows | processing_time {req_total_proc_time:.2}s')
        usr_total_rows, usr_total_proc_time = users_task.result()
        self.logger.info(f'Users collector processed {usr_total_rows} rows | processing_time {usr_total_proc_time:.2}s')
        proc_time = time()
        users_count = self.collect_users_count(client)

        all_args = AllArgs.model_validate({
            **self.config.model_dump(),
            **self.config.exec_params.model_dump(),
            **self.test_data.model_dump(),
            'total_requests_count': req_total_rows,
            'start_time': self.requests_start_time or 0,
            'end_time': self.requests_end_time or 0,
            'users': users_count
        })
        with open(self.config.args_file_path, 'w') as out:
            out.write(all_args.model_dump_json(indent=2, exclude={'exec_params'}))

        self.logger.info(f'User count and all args dump done | processing_time: {time() - proc_time:.2}s')
        self.logger.info(f'Total processing time: {sum((proc_time, req_total_proc_time, usr_total_proc_time))}')


if __name__ == '__main__':
    collector = Collector()
    asyncio.run(collector.accumulate_data())
