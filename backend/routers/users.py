from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from backend.auth import UserExistsError, UserNotFoundError
from backend.dependencies import get_auth_service, require_admin

router = APIRouter(
    prefix='/api/users',
    tags=['users'],
    dependencies=[Depends(require_admin)],
)


class CreateUserRequest(BaseModel):
    username: str = Field(min_length=3)
    password: str = Field(min_length=6)
    role: str = 'staff'
    is_active: bool = True


class UpdateUserRequest(BaseModel):
    role: str | None = None
    is_active: bool | None = None
    logo_url: str | None = None


@router.get('')
def list_users(auth_service=Depends(get_auth_service)):
    return {
        'users': auth_service.list_users(),
    }


@router.post('')
def create_user(payload: CreateUserRequest, auth_service=Depends(get_auth_service)):
    try:
        user = auth_service.create_user(
            username=payload.username,
            password=payload.password,
            role=payload.role,
            is_active=payload.is_active,
        )
    except UserExistsError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return {
        'user': user,
    }


@router.patch('/{user_id}')
def update_user(
    user_id: int,
    payload: UpdateUserRequest,
    auth_service=Depends(get_auth_service),
    current_user=Depends(require_admin),
):
    if payload.role is None and payload.is_active is None and payload.logo_url is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail='Provide role, is_active, or logo_url to update.')

    current_user_id = int(current_user.get('id') or 0)
    is_self_update = user_id == current_user_id
    if is_self_update and payload.is_active is False:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail='You cannot disable your own account.',
        )

    if is_self_update and payload.role is not None and str(payload.role).strip().lower() != 'admin':
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail='You cannot remove your own admin role.',
        )

    try:
        user = auth_service.update_user(
            user_id=user_id,
            role=payload.role,
            is_active=payload.is_active,
            logo_url=payload.logo_url,
        )
    except UserNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return {
        'user': user,
    }