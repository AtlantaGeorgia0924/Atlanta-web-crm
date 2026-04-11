import os

import uvicorn


if __name__ == '__main__':
    uvicorn.run(
        'backend.main:app',
        host=os.getenv('FASTAPI_HOST', '127.0.0.1'),
        port=int(os.getenv('FASTAPI_PORT', '8000')),
        reload=os.getenv('FASTAPI_RELOAD', '0') == '1',
    )