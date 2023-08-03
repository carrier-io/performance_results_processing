from typing import Iterable

from centry_loki import log_loki


def build_api_url(
        plugin: str, file_name: str, mode: str = 'default',
        api_version: int = 1, trailing_slash: bool = False,
        skip_mode: bool = False
) -> str:
    struct = ['/api', f'v{api_version}', plugin, file_name]
    if not skip_mode:
        struct.append(mode)
    url = '/'.join(struct)
    if trailing_slash:
        url += '/'
    return url


def get_loki_logger(loki_host: str,
                    build_id: str, project_id: int, report_id: int,
                    loki_port: int = 3100,
                    hostname: str = 'post-processor',
                    loki_api_path: str = '/api/v1/push',
                    logger_stop_words: Iterable = (),
                    **kwargs) -> 'Logger':
    context = {
        'url': f'{loki_host}:{loki_port}/loki{loki_api_path}',
        'hostname': hostname,
        'labels': {
            'build_id': build_id,
            'project': project_id,
            'report_id': report_id
        }
    }
    return log_loki.get_logger(context, secrets=logger_stop_words)
