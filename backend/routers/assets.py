from fastapi import APIRouter, Depends

from backend.dependencies import get_runtime

router = APIRouter(prefix='/api/assets', tags=['assets'])


@router.get('/logo')
def get_logo(runtime=Depends(get_runtime)):
    return runtime.get_logo_payload()