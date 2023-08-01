import json
from os import environ
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, field_validator, computed_field, ConfigDict, model_validator

from utils import build_api_url
from datetime import datetime


class TestStatus(BaseModel):
    status: str
    percentage: Optional[int] = None
    description: Optional[str] = None

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self._updated = datetime.utcnow()

    @property
    def test_finished(self) -> bool:
        return self.status.lower() in {
            "post processing", "failed",
            "canceled", "cancelled",
        }


class InfluxQueries:
    _users_count = r'''
        select sum("mx_usr_cnt") as sum
        from (
            select max("user_count") as mx_usr_cnt
            from "users" 
            {where}
            group by lg_id
        )
    '''

    _users_data = r'''
        select %s
        from "users"
        {where}
        order by time asc
    '''
    _requests_data = r'''
        select %s
        from "%s"
        {where}
        order by time asc
    '''

    def __init__(self, test_name: str, *, request_data_fields: list, user_fields: list):
        self.users_count = self.sanitize_query(self._users_count)
        self.users_data = self.sanitize_query(
            self._users_data % ', '.join(user_fields)
        )
        self.requests_data = self.sanitize_query(
            self._requests_data % (', '.join(request_data_fields), test_name)
        )

    @staticmethod
    def sanitize_query(query: str) -> str:
        return ' '.join([i.strip() for i in query.split('\n')])

    def where(self, clause: dict, operator: str = 'and') -> str:
        expressions = []
        for k, v in clause.items():
            if isinstance(v, str):
                v = f"'{v}'"
            expressions.append(f"{k}{v}")
        c = f' {operator} '.join(expressions)
        return c


class ExecParams(BaseModel):
    influxdb_host: str
    influxdb_port: int = 8086
    influxdb_user: str
    influxdb_password: str
    influxdb_database: str
    influxdb_comparison: str
    influxdb_telegraf: str
    loki_host: str
    loki_port: int = 3100


class CollectorConfig(BaseModel):
    @classmethod
    def from_env(cls) -> 'CollectorConfig':
        env_dict = {i: environ.get(i) for i in {
            'build_id', 'base_url', 'project_id',
            'token', 'report_id', 'exec_params',
            'manual_run', 'max_empty_attempts',
            'influx_query_limit', 'iteration_sleep',
            'test_status_update_interval'
        }}
        return cls(**env_dict)

    result_fields: list = [
        'time', 'request_name', 'method', 'response_time',
        'status', 'status_code', 'lg_id'
    ]

    user_fields: list = ['time', 'active', 'lg_id']
    build_id: str
    base_url: str
    project_id: int
    token: str
    report_id: int
    exec_params: ExecParams
    manual_run: bool = False
    max_empty_attempts: int = 10
    influx_query_limit: int = 500000
    iteration_sleep: int = 60
    test_status_update_interval: int = 60
    output_path: Path | str = Path('/', 'tmp')

    @model_validator(mode='before')
    @classmethod
    def set_manual_run_constants(cls, data: dict) -> dict:
        if data.get('manual_run'):
            data['max_empty_attempts'] = data.get('max_empty_attempts', 0)
            data['iteration_sleep'] = data.get('iteration_sleep', 0)
        if isinstance(data.get('output_path'), str):
            data['output_path'] = Path(data['output_path'])
        return data

    @field_validator('base_url')
    @classmethod
    def strip_slash(cls, value: str) -> str:
        return value.rstrip('/')

    @field_validator('exec_params', mode='before')
    @classmethod
    def load_json(cls, value: str | dict | None) -> dict:
        if value is None:
            return dict()
        if isinstance(value, str):
            return json.loads(value)
        return value

    @computed_field
    @property
    def results_file_path(self) -> Path:
        return self.output_path.joinpath(f'results_{self.build_id}.csv')

    @computed_field
    @property
    def users_file_path(self) -> Path:
        return self.output_path.joinpath(f'users_{self.build_id}.csv')

    @property
    def args_file_path(self) -> Path:
        return self.output_path.joinpath('args.json')

    @computed_field
    @property
    def api_headers(self) -> dict:
        return {'Authorization': f'Bearer {self.token}'}

    @computed_field
    @property
    def report_status_url(self) -> str:
        return '/'.join(map(str, [
            self.base_url,
            build_api_url('backend_performance', 'report_status', skip_mode=True).lstrip('/'),
            self.project_id,
            self.report_id
        ]))


class TestData(BaseModel):
    # model_config = ConfigDict()

    type: str
    name: str
    environment: str
    test_status: TestStatus


class AllArgs(CollectorConfig, TestData, ExecParams):
    total_requests_count: int
    start_time: datetime
    end_time: datetime
    users: int

    @computed_field
    @property
    def duration(self) -> int:
        return int((self.end_time - self.start_time).total_seconds())
