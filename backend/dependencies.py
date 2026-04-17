from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from backend.auth import TokenValidationError

bearer_scheme = HTTPBearer(auto_error=False)


def get_runtime(request: Request):
    return request.app.state.runtime


def get_auth_service(request: Request):
    return request.app.state.auth_service


def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
    auth_service=Depends(get_auth_service),
):
    if credentials is None or not credentials.credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail='Not authenticated.',
            headers={'WWW-Authenticate': 'Bearer'},
        )

    try:
        payload = auth_service.validate_access_token(credentials.credentials)
        user = auth_service.get_user_by_id(payload.get('sub'))
    except TokenValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(exc),
            headers={'WWW-Authenticate': 'Bearer'},
        ) from exc

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
    return auth_service.public_user(user)


def require_admin(current_user=Depends(get_current_user)):
    if current_user.get('role') != 'admin':
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail='Admin access required.',
        )
    return current_user


def require_staff(current_user=Depends(get_current_user)):
    role = str(current_user.get('role') or '').lower()
    if role not in ('admin', 'staff'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail='Staff or admin access required.',
        )
    return current_user
