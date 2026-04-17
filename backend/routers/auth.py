from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from backend.auth import InactiveUserError, InvalidCredentialsError
from backend.dependencies import get_auth_service, get_current_user

router = APIRouter(prefix='/api/auth', tags=['auth'])


class LoginRequest(BaseModel):
    username: str = Field(min_length=1)
    password: str = Field(min_length=1)


@router.post('/login')
def login(payload: LoginRequest, auth_service=Depends(get_auth_service)):
    try:
        user = auth_service.authenticate_user(payload.username, payload.password)
    except InvalidCredentialsError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(exc),
        ) from exc
    except InactiveUserError as exc:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=str(exc),
        ) from exc

    return {
        'access_token': auth_service.create_access_token(user),
        'token_type': 'bearer',
        'expires_in': auth_service.settings.jwt_expiration_minutes * 60,
        'user': user,
    }


@router.get('/me')
def get_me(current_user=Depends(get_current_user)):
    return current_user