from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.dependencies import get_runtime, require_admin
from services.client_service import (
    build_client_directory_rows,
    build_matched_contact_updates,
    build_selected_contact_updates,
    find_existing_client_key,
    import_sheet_phone_numbers_to_registry,
    match_contact_to_client_name,
    normalize_client_name,
    normalize_phone_number,
    set_client_phone,
    validate_client_entry,
)

router = APIRouter(
    prefix='/api/clients',
    tags=['clients'],
    dependencies=[Depends(require_admin)],
)


class ContactInput(BaseModel):
    name: str = ''
    phone: str = ''
    label: str = ''


class NormalizePhoneRequest(BaseModel):
    value: Any = None


class NormalizeNameRequest(BaseModel):
    value: Any = None


class FindExistingKeyRequest(BaseModel):
    name: str
    registry: dict[str, str] = Field(default_factory=dict)


class SetClientPhoneRequest(BaseModel):
    name: str
    phone: str
    registry: dict[str, str] = Field(default_factory=dict)


class ValidateClientEntryRequest(BaseModel):
    name: str
    phone: str


class MatchContactRequest(BaseModel):
    contact_name: str
    candidate_names: list[str]


class SelectedContactUpdatesRequest(BaseModel):
    selected_contacts: list[ContactInput]


class MatchedContactUpdatesRequest(BaseModel):
    imported_contacts: list[ContactInput]
    candidate_names: list[str]
    registry: dict[str, str] = Field(default_factory=dict)


class ClientDirectoryRowsRequest(BaseModel):
    registry: dict[str, str] = Field(default_factory=dict)


class ImportSheetPhonesRequest(BaseModel):
    values: list[list[Any]]
    name_col: int | None = None
    phone_col: int | None = None
    registry: dict[str, str] = Field(default_factory=dict)


class LiveClientUpsertRequest(BaseModel):
    previous_name: str | None = None
    name: str
    phone: str
    gender: str | None = None
    sync_sheet: bool = True
    force_refresh: bool = False


class LiveClientDeleteRequest(BaseModel):
    name: str
    sync_sheet: bool = True


class ClientChangeHistoryRequest(BaseModel):
    limit: int = 100


@router.post('/normalize/phone')
def normalize_phone_endpoint(payload: NormalizePhoneRequest):
    return {
        'phone': normalize_phone_number(payload.value)
    }


@router.post('/normalize/name')
def normalize_name_endpoint(payload: NormalizeNameRequest):
    return {
        'name': normalize_client_name(payload.value)
    }


@router.post('/existing-key/find')
def find_existing_key_endpoint(payload: FindExistingKeyRequest):
    return {
        'key': find_existing_client_key(payload.name, payload.registry)
    }


@router.post('/entry/set-phone')
def set_client_phone_endpoint(payload: SetClientPhoneRequest):
    registry = dict(payload.registry)
    added, changed, key = set_client_phone(payload.name, payload.phone, registry)
    return {
        'added': added,
        'changed': changed,
        'key': key,
        'registry': registry,
    }


@router.post('/entry/validate')
def validate_client_entry_endpoint(payload: ValidateClientEntryRequest):
    return validate_client_entry(payload.name, payload.phone)


@router.post('/contacts/match')
def match_contact_endpoint(payload: MatchContactRequest):
    return {
        'match': match_contact_to_client_name(payload.contact_name, payload.candidate_names)
    }


@router.post('/contacts/selected-updates')
def build_selected_contact_updates_endpoint(payload: SelectedContactUpdatesRequest):
    return {
        'updates': build_selected_contact_updates(
            [contact.model_dump() for contact in payload.selected_contacts]
        )
    }


@router.post('/contacts/matched-updates')
def build_matched_contact_updates_endpoint(payload: MatchedContactUpdatesRequest):
    return build_matched_contact_updates(
        [contact.model_dump() for contact in payload.imported_contacts],
        payload.candidate_names,
        dict(payload.registry),
    )


@router.post('/directory/rows')
def build_client_directory_rows_endpoint(payload: ClientDirectoryRowsRequest):
    return {
        'rows': build_client_directory_rows(payload.registry)
    }


@router.post('/sheet/import-phones')
def import_sheet_phone_numbers_endpoint(payload: ImportSheetPhonesRequest):
    registry = dict(payload.registry)
    added, updated = import_sheet_phone_numbers_to_registry(
        payload.values,
        payload.name_col,
        payload.phone_col,
        registry,
    )
    return {
        'added': added,
        'updated': updated,
        'registry': registry,
    }


@router.get('/live')
def live_clients_endpoint(force_reload: bool = False, runtime=Depends(get_runtime)):
    return runtime.get_client_registry_payload(force_reload=force_reload)


@router.post('/live/upsert')
def live_client_upsert_endpoint(payload: LiveClientUpsertRequest, runtime=Depends(get_runtime)):
    try:
        result = runtime.upsert_client(
            payload.name,
            payload.phone,
            payload.gender,
            payload.previous_name,
            sync_sheet=payload.sync_sheet,
            force_refresh=payload.force_refresh,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    if result.get('error'):
        raise HTTPException(status_code=400, detail=result['error'])
    return result


@router.post('/live/delete')
def live_client_delete_endpoint(payload: LiveClientDeleteRequest, runtime=Depends(get_runtime)):
    try:
        result = runtime.delete_client(payload.name, sync_sheet=payload.sync_sheet)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    if result.get('error'):
        raise HTTPException(status_code=404, detail=result['error'])
    return result


@router.post('/live/import-sheet-phones')
def live_import_sheet_phones_endpoint(force_refresh: bool = False, runtime=Depends(get_runtime)):
    try:
        return runtime.import_sheet_phone_numbers_to_clients(force_refresh=force_refresh)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get('/live/change-history')
def live_client_change_history(limit: int = 100, runtime=Depends(get_runtime)):
    try:
        return runtime.get_client_change_history(limit=limit)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get('/google-contacts')
def google_contacts_endpoint(search: str = '', force_refresh: bool = False, runtime=Depends(get_runtime)):
    try:
        return runtime.get_google_contacts_payload(search=search, force_refresh=force_refresh)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post('/google-contacts/sync')
def sync_google_contacts_endpoint(search: str = '', runtime=Depends(get_runtime)):
    try:
        return runtime.get_google_contacts_payload(search=search, force_refresh=True)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
