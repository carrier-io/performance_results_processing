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
