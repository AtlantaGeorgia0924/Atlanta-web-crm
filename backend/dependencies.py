from fastapi import Request


def get_runtime(request: Request):
    return request.app.state.runtime
