from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import Body, Depends, FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.staticfiles import StaticFiles

from seo_dashboard.auth import create_token, dashboard_password, verify_token
from seo_dashboard.db import init_db
from seo_dashboard.service import DashboardService


ROOT = Path(__file__).resolve().parent
FRONTEND_DIST = ROOT / "frontend" / "dist"
DEMO_SAMPLE_FILE = ROOT / "sample_data" / "data-ranking-demo.xlsx"
DEMO_SHEET_URL = "https://docs.google.com/spreadsheets/d/1R51BQmKESrbWHfdCCskqLQQJIeVQunHLOfKBJP4Ajt4/edit?usp=sharing"

app = FastAPI(
    title="SEO Ranking Dashboard",
    version="1.0.0",
    docs_url="/api/docs",
    openapi_url="/api/openapi.json",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

security = HTTPBearer(auto_error=False)
service = DashboardService()


def require_auth(credentials: HTTPAuthorizationCredentials | None = Depends(security)) -> dict[str, Any]:
    if not credentials or not credentials.credentials:
        raise HTTPException(status_code=401, detail="Vui lòng đăng nhập.")
    try:
        return verify_token(credentials.credentials)
    except Exception as exc:  # pragma: no cover - simple auth guard
        raise HTTPException(status_code=401, detail="Phiên đăng nhập không hợp lệ hoặc đã hết hạn.") from exc


@app.on_event("startup")
async def startup_event() -> None:
    init_db()
    bootstrap_demo_project()
    app.state.refresh_task = asyncio.create_task(refresh_loop())


@app.on_event("shutdown")
async def shutdown_event() -> None:
    task = getattr(app.state, "refresh_task", None)
    if task:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def refresh_loop() -> None:
    while True:
        try:
            service.refresh_due_projects()
        except Exception:
            pass
        await asyncio.sleep(300)


def bootstrap_demo_project() -> None:
    if service.list_projects() or not DEMO_SAMPLE_FILE.exists():
        return
    project = service.create_project(
        "Demo SEO Ranking",
        sheet_url=DEMO_SHEET_URL,
        source_name="Data ranking demo",
        source_type="google_sheet",
        refresh_interval_minutes=30,
    )
    service.import_upload(project["id"], DEMO_SAMPLE_FILE.name, DEMO_SAMPLE_FILE.read_bytes())


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "timestamp": datetime.now().isoformat()}


@app.post("/api/auth/login")
def login(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    password = str(payload.get("password") or "")
    if password != dashboard_password():
        raise HTTPException(status_code=401, detail="Mật khẩu không đúng.")
    return {"token": create_token(), "expires_in_seconds": 24 * 60 * 60}


@app.get("/api/auth/session")
def session(_: dict[str, Any] = Depends(require_auth)) -> dict[str, Any]:
    return {"ok": True}


@app.get("/api/projects")
def list_projects(_: dict[str, Any] = Depends(require_auth)) -> dict[str, Any]:
    return {"projects": service.list_projects()}


@app.post("/api/projects")
def create_project(payload: dict[str, Any] = Body(default={}), _: dict[str, Any] = Depends(require_auth)) -> dict[str, Any]:
    project = service.create_project(
        payload.get("name") or "SEO Project",
        sheet_url=payload.get("sheet_url"),
        source_name=payload.get("source_name"),
        source_type=payload.get("source_type") or "upload",
        refresh_interval_minutes=int(payload.get("refresh_interval_minutes") or 30),
        anthropic_api_key=payload.get("anthropic_api_key"),
    )
    return {"project": project}


@app.delete("/api/projects/{project_id}")
def delete_project(project_id: int, _: dict[str, Any] = Depends(require_auth)) -> dict[str, bool]:
    service.delete_project(project_id)
    return {"ok": True}


@app.post("/api/projects/test-sheet")
def test_sheet(payload: dict[str, Any] = Body(default={}), _: dict[str, Any] = Depends(require_auth)) -> dict[str, Any]:
    sheet_url = (payload.get("sheet_url") or "").strip()
    if not sheet_url:
        raise HTTPException(status_code=400, detail="Vui lòng nhập link dữ liệu public.")
    return service.test_google_sheet(sheet_url, payload.get("sheet_gid"))


@app.post("/api/projects/{project_id}/upload")
async def upload_file(
    project_id: int,
    file: UploadFile = File(...),
    _: dict[str, Any] = Depends(require_auth),
) -> dict[str, Any]:
    payload = await file.read()
    return service.import_upload(project_id, file.filename or "ranking.xlsx", payload)


@app.post("/api/projects/{project_id}/refresh")
def refresh_project(project_id: int, _: dict[str, Any] = Depends(require_auth)) -> dict[str, Any]:
    return service.refresh_from_google_sheet(project_id)


@app.get("/api/projects/{project_id}/overview")
def project_overview(project_id: int, _: dict[str, Any] = Depends(require_auth)) -> dict[str, Any]:
    return service.get_overview(project_id)


@app.get("/api/projects/{project_id}/groups")
def group_view(
    project_id: int,
    current_date: str | None = Query(default=None),
    baseline_date: str | None = Query(default=None),
    status: str = Query(default="all"),
    main_cluster: str | None = Query(default=None),
    tag: str = Query(default="all"),
    sort_by: str = Query(default="health_score"),
    active_scenario_id: str | None = Query(default=None),
    sub_cluster_mode: str | None = Query(default=None),
    clustering_mode: str | None = Query(default=None),
    custom_primary_tag_prefix: str | None = Query(default=None),
    custom_secondary_tag_prefix: str | None = Query(default=None),
    _: dict[str, Any] = Depends(require_auth),
) -> dict[str, Any]:
    custom_config = None
    if custom_primary_tag_prefix or custom_secondary_tag_prefix:
        custom_config = {
            "primary_tag_prefix": custom_primary_tag_prefix or "",
            "secondary_tag_prefix": custom_secondary_tag_prefix or "",
        }
    return service.get_group_view(
        project_id,
        current_date=current_date,
        baseline_date=baseline_date,
        status_filter=status,
        main_cluster=main_cluster,
        tag_filter=tag,
        sort_by=sort_by,
        active_scenario_id=active_scenario_id,
        legacy_mode=sub_cluster_mode or clustering_mode or "auto",
        custom_config=custom_config,
    )


@app.get("/api/projects/{project_id}/keywords")
def keyword_table(
    project_id: int,
    current_date: str | None = Query(default=None),
    search: str = Query(default=""),
    groups: str = Query(default=""),
    clusters: str = Query(default=""),
    status: str = Query(default="all"),
    vol_min: int = Query(default=0),
    vol_max: int = Query(default=1000000),
    rank_min: float = Query(default=0),
    rank_max: float = Query(default=101),
    movers_only: bool = Query(default=False),
    sort_by: str = Query(default="current_rank"),
    sort_dir: str = Query(default="asc"),
    _: dict[str, Any] = Depends(require_auth),
) -> dict[str, Any]:
    return service.get_keyword_table(
        project_id,
        {
            "current_date": current_date,
            "search": search,
            "groups": groups,
            "clusters": clusters,
            "status": status,
            "vol_min": vol_min,
            "vol_max": vol_max,
            "rank_min": rank_min,
            "rank_max": rank_max,
            "movers_only": movers_only,
            "sort_by": sort_by,
            "sort_dir": sort_dir,
        },
    )


@app.get("/api/projects/{project_id}/keywords/{keyword_id}")
def keyword_detail(project_id: int, keyword_id: int, _: dict[str, Any] = Depends(require_auth)) -> dict[str, Any]:
    return service.get_keyword_detail(project_id, keyword_id)


@app.post("/api/projects/{project_id}/keywords/{keyword_id}/notes")
def save_keyword_notes(
    project_id: int,
    keyword_id: int,
    payload: dict[str, Any] = Body(default={}),
    _: dict[str, Any] = Depends(require_auth),
) -> dict[str, Any]:
    return service.save_keyword_notes(project_id, keyword_id, payload.get("notes") or "")


@app.post("/api/projects/{project_id}/keywords/{keyword_id}/insight")
def keyword_insight(project_id: int, keyword_id: int, _: dict[str, Any] = Depends(require_auth)) -> dict[str, Any]:
    return service.generate_keyword_insight(project_id, keyword_id)


@app.get("/api/projects/{project_id}/settings")
def get_settings(project_id: int, _: dict[str, Any] = Depends(require_auth)) -> dict[str, Any]:
    return service.get_settings(project_id)


@app.post("/api/projects/{project_id}/settings")
def update_settings(
    project_id: int,
    payload: dict[str, Any] = Body(default={}),
    _: dict[str, Any] = Depends(require_auth),
) -> dict[str, Any]:
    return service.update_project_settings(project_id, payload)


@app.post("/api/projects/{project_id}/view-state")
def save_view_state(
    project_id: int,
    payload: dict[str, Any] = Body(default={}),
    _: dict[str, Any] = Depends(require_auth),
) -> dict[str, Any]:
    return service.update_project_view_state(project_id, payload)


@app.post("/api/projects/{project_id}/recluster")
def recluster(project_id: int, _: dict[str, Any] = Depends(require_auth)) -> dict[str, Any]:
    return service.recluster_keywords(project_id)


@app.post("/api/projects/{project_id}/insights/weekly")
def weekly_insight(project_id: int, _: dict[str, Any] = Depends(require_auth)) -> dict[str, Any]:
    return service.generate_weekly_summary(project_id, force=True)


@app.post("/api/projects/{project_id}/insights/daily-note/generate")
def generate_daily_note(
    project_id: int,
    payload: dict[str, Any] = Body(default={}),
    _: dict[str, Any] = Depends(require_auth),
) -> dict[str, Any]:
    insight_date = str(payload.get("insight_date") or "").strip()
    if not insight_date:
        raise HTTPException(status_code=400, detail="Thiếu ngày cần tạo nhận xét.")
    return service.generate_daily_note(
        project_id,
        insight_date,
        str(payload.get("seo_input") or ""),
    )


@app.post("/api/projects/{project_id}/insights/daily-note/pin")
def pin_daily_note(
    project_id: int,
    payload: dict[str, Any] = Body(default={}),
    _: dict[str, Any] = Depends(require_auth),
) -> dict[str, Any]:
    insight_date = str(payload.get("insight_date") or "").strip()
    if not insight_date:
        raise HTTPException(status_code=400, detail="Thiếu ngày cần ghim ghi chú.")
    return service.save_pinned_daily_note(
        project_id,
        insight_date,
        str(payload.get("content") or ""),
        str(payload.get("seo_input") or ""),
    )


@app.delete("/api/projects/{project_id}/insights/daily-note/pin")
def unpin_daily_note(
    project_id: int,
    insight_date: str = Query(...),
    _: dict[str, Any] = Depends(require_auth),
) -> dict[str, bool]:
    return service.remove_pinned_daily_note(project_id, insight_date)


@app.post("/api/projects/{project_id}/insights/weekly-note")
def save_weekly_note(
    project_id: int,
    payload: dict[str, Any] = Body(default={}),
    _: dict[str, Any] = Depends(require_auth),
) -> dict[str, Any]:
    return service.save_weekly_note(project_id, payload.get("content") or "")


@app.post("/api/projects/{project_id}/insights/cluster")
def cluster_insight(
    project_id: int,
    payload: dict[str, Any] = Body(default={}),
    _: dict[str, Any] = Depends(require_auth),
) -> dict[str, Any]:
    cluster_name = payload.get("cluster_name")
    if not cluster_name:
        raise HTTPException(status_code=400, detail="Thiếu tên cụm.")
    return service.generate_cluster_pattern_insight(project_id, cluster_name)


@app.post("/api/projects/{project_id}/shares/client-view")
def create_client_view(
    project_id: int,
    payload: dict[str, Any] = Body(default={}),
    _: dict[str, Any] = Depends(require_auth),
) -> dict[str, Any]:
    return service.create_client_view_share(project_id, payload)


@app.post("/api/projects/{project_id}/shares/seo-view")
def create_seo_view(
    project_id: int,
    payload: dict[str, Any] = Body(default={}),
    _: dict[str, Any] = Depends(require_auth),
) -> dict[str, Any]:
    return service.create_seo_view_share(project_id, payload)


@app.post("/api/projects/{project_id}/shares/report-snapshot")
def create_report_snapshot(
    project_id: int,
    payload: dict[str, Any] = Body(default={}),
    _: dict[str, Any] = Depends(require_auth),
) -> dict[str, Any]:
    return service.create_report_snapshot_share(project_id, payload)


@app.post("/api/public/{share_token}/login")
def public_share_login(
    share_token: str,
    payload: dict[str, Any] = Body(default={}),
) -> dict[str, Any]:
    password = str(payload.get("password") or "")
    return service.login_public_share(share_token, password)


@app.get("/api/public/{share_token}")
def public_share_payload(
    share_token: str,
    group_current_date: str | None = Query(default=None),
    group_baseline_date: str | None = Query(default=None),
    group_status: str = Query(default=""),
    group_main_cluster: str | None = Query(default=None),
    group_tag: str = Query(default=""),
    group_sort_by: str = Query(default=""),
    active_scenario_id: str | None = Query(default=None),
    keyword_current_date: str | None = Query(default=None),
    keyword_search: str = Query(default=""),
    keyword_groups: str = Query(default=""),
    keyword_clusters: str = Query(default=""),
    keyword_status: str = Query(default=""),
    keyword_vol_min: int | None = Query(default=None),
    keyword_vol_max: int | None = Query(default=None),
    keyword_rank_min: float | None = Query(default=None),
    keyword_rank_max: float | None = Query(default=None),
    keyword_movers_only: bool | None = Query(default=None),
    keyword_sort_by: str = Query(default=""),
    keyword_sort_dir: str = Query(default=""),
    credentials: HTTPAuthorizationCredentials | None = Depends(security),
) -> dict[str, Any]:
    return service.get_public_share_payload(
        share_token,
        public_token=credentials.credentials if credentials else None,
        group_filters={
            "current_date": group_current_date,
            "baseline_date": group_baseline_date,
            "status": group_status,
            "main_cluster": group_main_cluster,
            "tag": group_tag,
            "sort_by": group_sort_by,
            "active_scenario_id": active_scenario_id,
        },
        keyword_filters={
            "current_date": keyword_current_date,
            "search": keyword_search,
            "groups": keyword_groups,
            "clusters": keyword_clusters,
            "status": keyword_status,
            "vol_min": keyword_vol_min,
            "vol_max": keyword_vol_max,
            "rank_min": keyword_rank_min,
            "rank_max": keyword_rank_max,
            "movers_only": keyword_movers_only,
            "sort_by": keyword_sort_by,
            "sort_dir": keyword_sort_dir,
        },
    )


@app.get("/api/public/{share_token}/keywords/{keyword_id}")
def public_keyword_detail(
    share_token: str,
    keyword_id: int,
    credentials: HTTPAuthorizationCredentials | None = Depends(security),
) -> dict[str, Any]:
    return service.get_public_keyword_detail(
        share_token,
        keyword_id,
        public_token=credentials.credentials if credentials else None,
    )


@app.get("/api/projects/{project_id}/events")
def list_events(project_id: int, _: dict[str, Any] = Depends(require_auth)) -> dict[str, Any]:
    return {"events": service.refresh_anomaly_events(project_id)}


@app.post("/api/projects/{project_id}/events")
def create_event(
    project_id: int,
    payload: dict[str, Any] = Body(default={}),
    _: dict[str, Any] = Depends(require_auth),
) -> dict[str, Any]:
    if not payload.get("event_date") or not payload.get("title"):
        raise HTTPException(status_code=400, detail="Thiếu ngày hoặc tiêu đề sự kiện.")
    return service.add_manual_event(
        project_id,
        payload["event_date"],
        payload["title"],
        payload.get("description") or "",
        payload.get("impact_type") or "manual",
    )


@app.get("/api/projects/{project_id}/export")
def export_keywords(
    project_id: int,
    current_date: str | None = Query(default=None),
    search: str = Query(default=""),
    groups: str = Query(default=""),
    clusters: str = Query(default=""),
    status: str = Query(default="all"),
    vol_min: int = Query(default=0),
    vol_max: int = Query(default=1000000),
    rank_min: float = Query(default=0),
    rank_max: float = Query(default=101),
    movers_only: bool = Query(default=False),
    sort_by: str = Query(default="current_rank"),
    sort_dir: str = Query(default="asc"),
    _: dict[str, Any] = Depends(require_auth),
) -> Response:
    payload = service.export_keyword_table(
        project_id,
        {
            "current_date": current_date,
            "search": search,
            "groups": groups,
            "clusters": clusters,
            "status": status,
            "vol_min": vol_min,
            "vol_max": vol_max,
            "rank_min": rank_min,
            "rank_max": rank_max,
            "movers_only": movers_only,
            "sort_by": sort_by,
            "sort_dir": sort_dir,
        },
    )
    headers = {"Content-Disposition": 'attachment; filename="seo-keywords.xlsx"'}
    return Response(
        payload,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )


@app.exception_handler(ValueError)
def value_error_handler(_: Any, exc: ValueError) -> JSONResponse:
    return JSONResponse({"detail": str(exc)}, status_code=400)


if FRONTEND_DIST.exists():
    @app.get("/client/{share_token}")
    def client_view_page(share_token: str) -> FileResponse:
        return FileResponse(FRONTEND_DIST / "index.html")


    @app.get("/seo/{share_token}")
    def seo_view_page(share_token: str) -> FileResponse:
        return FileResponse(FRONTEND_DIST / "index.html")


    @app.get("/report/{share_token}")
    def report_view_page(share_token: str) -> FileResponse:
        return FileResponse(FRONTEND_DIST / "index.html")


if FRONTEND_DIST.exists():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIST), html=True), name="static")
