from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from backend.auth import InactiveUserError, InvalidCredentialsError, TokenValidationError
from backend.dependencies import get_auth_service, get_current_user

router = APIRouter(prefix='/api/auth', tags=['auth'])


class LoginRequest(BaseModel):
    username: str = Field(min_length=1)
    password: str = Field(min_length=1)


class RefreshRequest(BaseModel):
    refresh_token: str = Field(min_length=1)


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
        'refresh_token': auth_service.create_refresh_token(user),
        'token_type': 'bearer',
        'expires_in': auth_service.settings.jwt_expiration_minutes * 60,
        'refresh_expires_in': auth_service.settings.jwt_refresh_expiration_days * 24 * 60 * 60,
        'user': user,
    }


@router.post('/refresh')
def refresh(payload: RefreshRequest, auth_service=Depends(get_auth_service)):
    try:
        token_payload = auth_service.validate_refresh_token(payload.refresh_token)
    except TokenValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail='Refresh token is invalid or expired.',
            headers={'WWW-Authenticate': 'Bearer'},
        ) from exc

    user = auth_service.get_user_by_id(token_payload.get('sub'))
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail='Authenticated user was not found.',
            headers={'WWW-Authenticate': 'Bearer'},
        )
    if not user.get('is_active'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail='User account is inactive.',
        )

    public_user = auth_service.public_user(user)
    return {
        'access_token': auth_service.create_access_token(user),
        'refresh_token': auth_service.create_refresh_token(user),
        'token_type': 'bearer',
        'expires_in': auth_service.settings.jwt_expiration_minutes * 60,
        'refresh_expires_in': auth_service.settings.jwt_refresh_expiration_days * 24 * 60 * 60,
        'user': public_user,
    }


@router.get('/me')
def get_me(current_user=Depends(get_current_user)):
    return current_user