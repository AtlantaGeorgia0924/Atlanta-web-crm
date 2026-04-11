from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.dependencies import get_runtime
from services.name_fix_service import (
    build_name_fix_all_updates,
    build_name_fix_summary,
    build_name_fix_updates,
    find_name_mismatches,
    fuzzy_score,
)

router = APIRouter(prefix='/api/name-fix', tags=['name-fix'])


class FuzzyScoreRequest(BaseModel):
    a: Any = None
    b: Any = None


class FindNameMismatchesRequest(BaseModel):
    values: list[list[Any]]
    client_names: list[str]


class NameFixEntry(BaseModel):
    raw: str = ''
    rows: list[int] = Field(default_factory=list)
    candidates: list[str] = Field(default_factory=list)


class BuildNameFixUpdatesRequest(BaseModel):
    values: list[list[Any]]
    mismatch_entry: NameFixEntry
    correct_name: str


class BuildNameFixAllUpdatesRequest(BaseModel):
    values: list[list[Any]]
    mismatch_entries: list[NameFixEntry]


class BuildNameFixSummaryRequest(BaseModel):
    mismatch_entries: list[NameFixEntry]


class LiveApplyNameFixRequest(BaseModel):
    mismatch_entry: NameFixEntry
    correct_name: str
    force_refresh: bool = False


class LiveApplyAllNameFixRequest(BaseModel):
    mismatch_entries: list[NameFixEntry]
    force_refresh: bool = False


@router.post('/fuzzy-score')
def fuzzy_score_endpoint(payload: FuzzyScoreRequest):
    return {
        'score': fuzzy_score(payload.a, payload.b)
    }


@router.post('/mismatches')
def find_name_mismatches_endpoint(payload: FindNameMismatchesRequest):
    return {
        'mismatches': find_name_mismatches(payload.values, payload.client_names)
    }


@router.post('/update-plan')
def build_name_fix_updates_endpoint(payload: BuildNameFixUpdatesRequest):
    return {
        'updates': build_name_fix_updates(
            payload.values,
            payload.mismatch_entry.model_dump(),
            payload.correct_name,
        )
    }


@router.post('/update-all-plan')
def build_name_fix_all_updates_endpoint(payload: BuildNameFixAllUpdatesRequest):
    return {
        'updates': build_name_fix_all_updates(
            payload.values,
            [entry.model_dump() for entry in payload.mismatch_entries],
        )
    }


@router.post('/summary')
def build_name_fix_summary_endpoint(payload: BuildNameFixSummaryRequest):
    return {
        'summary': build_name_fix_summary(
            [entry.model_dump() for entry in payload.mismatch_entries]
        )
    }


@router.get('/live')
def live_name_fix_endpoint(force_refresh: bool = False, runtime=Depends(get_runtime)):
    return runtime.get_live_name_mismatches(force_refresh=force_refresh)


@router.post('/live/apply')
def live_apply_name_fix_endpoint(payload: LiveApplyNameFixRequest, runtime=Depends(get_runtime)):
    try:
        result = runtime.apply_name_fix(
            payload.mismatch_entry.model_dump(),
            payload.correct_name,
            force_refresh=payload.force_refresh,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    if result.get('error'):
        raise HTTPException(status_code=400, detail=result['error'])
    return result


@router.post('/live/apply-all')
def live_apply_name_fix_all_endpoint(payload: LiveApplyAllNameFixRequest, runtime=Depends(get_runtime)):
    try:
        result = runtime.apply_name_fix_all(
            [entry.model_dump() for entry in payload.mismatch_entries],
            force_refresh=payload.force_refresh,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    if result.get('error'):
        raise HTTPException(status_code=400, detail=result['error'])
    return result
