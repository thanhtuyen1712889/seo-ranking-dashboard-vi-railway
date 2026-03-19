from __future__ import annotations

import io
import json
import os
import re
import secrets
from collections import Counter, defaultdict
from datetime import datetime
from math import log
from statistics import mean
from typing import Any

import pandas as pd
from nltk.stem import PorterStemmer

from .ai import (
    call_claude,
    fallback_cluster_pattern,
    fallback_keyword_insight,
    fallback_weekly_range_note,
    fallback_weekly_summary,
)
from .auth import (
    PUBLIC_VIEW_TTL_SECONDS,
    create_public_view_token,
    hash_view_password,
    verify_public_view_token,
    verify_view_password,
)
from .db import get_connection, transaction
from .ingestion import (
    fetch_public_data_source,
    infer_sub_cluster_name,
    kpi_type_from_target,
    normalize_label,
    parse_spreadsheet_payload,
)


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def parse_iso_date(value: str) -> datetime:
    return datetime.fromisoformat(value)


def format_date_label(value: str) -> str:
    try:
        return parse_iso_date(value).strftime("%d/%m")
    except ValueError:
        return value


def client_rank_badge(rank: float | None) -> str:
    if rank is None:
        return "⚪ Chưa có dữ liệu"
    if rank <= 5:
        return "🟢 Tốt (Top 5)"
    if rank <= 10:
        return "🔵 Khá (Top 10)"
    if rank <= 20:
        return "🟡 Trung bình"
    return "🔴 Cần cải thiện"


def safe_mean(values: list[float]) -> float | None:
    if not values:
        return None
    return round(float(mean(values)), 2)


def clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


CLUSTER_SORTS = {
    "health_score": ("health_score", True),
    "trend_strength": ("rank_delta", True),
    "total_volume": ("total_volume", True),
    "avg_rank": ("avg_rank_current", False),
}

STEMMER = PorterStemmer()

TOKEN_STOPWORDS = {
    "a",
    "an",
    "and",
    "at",
    "by",
    "for",
    "from",
    "in",
    "into",
    "of",
    "on",
    "or",
    "the",
    "to",
    "via",
    "with",
    "without",
}

TAG_FAMILY_LABELS = {
    "platform": "nền tảng",
    "product": "loại sản phẩm",
    "intent": "nhu cầu",
}

TAG_LIBRARY = {
    "platform": {
        "magento_2": {
            "label": "Magento 2",
            "aliases": ["magento 2", "magento2", "for magento 2", "adobe commerce 2"],
        },
        "magento_generic": {
            "label": "Magento (generic)",
            "aliases": ["magento", "adobe commerce"],
            "suppressed_by": ["platform:magento_2"],
        },
        "shopify": {
            "label": "Shopify",
            "aliases": ["shopify"],
        },
        "woocommerce": {
            "label": "WooCommerce",
            "aliases": ["woocommerce", "woo commerce"],
        },
    },
    "product": {
        "plugin": {
            "label": "Plugin",
            "aliases": ["plugin", "plugins"],
        },
        "module": {
            "label": "Module",
            "aliases": ["module", "modules"],
        },
        "extension": {
            "label": "Extension",
            "aliases": ["extension", "extensions", "addon", "addons", "add on", "add ons"],
        },
        "suite": {
            "label": "Suite / package",
            "aliases": ["suite", "suites", "package", "packages", "bundle", "bundles"],
        },
        "theme": {
            "label": "Theme",
            "aliases": ["theme", "themes"],
        },
        "store": {
            "label": "Store / storefront",
            "aliases": ["store", "stores", "storefront", "store front"],
        },
    },
    "intent": {
        "b2b": {
            "label": "Generic B2B",
            "aliases": ["b2b", "wholesale"],
        },
        "login": {
            "label": "Login / account access",
            "aliases": ["login", "customer login", "account access", "sign in", "signin"],
        },
        "checkout": {
            "label": "Checkout & payment",
            "aliases": ["checkout", "payment", "one step checkout"],
        },
        "subscription": {
            "label": "Subscription / recurring",
            "aliases": ["subscription", "subscriptions", "recurring", "recurring billing"],
        },
        "integration": {
            "label": "Integration / connector",
            "aliases": ["integration", "integrations", "connector", "connectors", "sync"],
        },
        "seo": {
            "label": "SEO",
            "aliases": ["seo", "search engine optimization"],
        },
    },
}

TAG_PRIORITY = {
    family: [f"{family}:{tag_key}" for tag_key in family_tags]
    for family, family_tags in TAG_LIBRARY.items()
}

SUB_CLUSTER_MODE_META = {
    "auto": {
        "label": "Auto",
        "description": "Tự chọn góc nhìn phù hợp nhất theo phân bố tag trong dataset hiện tại.",
    },
    "platform_first": {
        "label": "Platform",
        "description": "Nhìn theo nền tảng trước để so sánh Magento, Shopify, WooCommerce...",
        "primary_family": "platform",
    },
    "product_first": {
        "label": "Product",
        "description": "Nhìn theo loại sản phẩm trước để tách plugin, module, extension, suite...",
        "primary_family": "product",
    },
    "intent_first": {
        "label": "Intent",
        "description": "Nhìn theo nhu cầu / use case trước để thấy login, checkout, integration, subscription...",
        "primary_family": "intent",
    },
    "custom": {
        "label": "Custom",
        "description": "Ghép cụm theo prefix tag tùy chọn, không cần viết lại engine.",
    },
}

LEGACY_SUB_CLUSTER_MODE_MAP = {
    "default": "auto",
    "platform": "platform_first",
    "product_type": "product_first",
    "intent": "intent_first",
}


def _normalize_ngram_text(value: str) -> str:
    text = normalize_label(value)
    replacements = {
        r"\bmagento2\b": "magento 2",
        r"\bwoo commerce\b": "woocommerce",
        r"\bstore front\b": "storefront",
        r"\badd ons?\b": "addon",
        r"\bcheck out\b": "checkout",
        r"\blog in\b": "login",
        r"\bsign in\b": "signin",
    }
    for pattern, replacement in replacements.items():
        text = re.sub(pattern, replacement, text)
    return re.sub(r"\s+", " ", text).strip()


def _stem_token(token: str) -> str:
    if token in {"b2b", "seo"} or token.isdigit():
        return token
    return STEMMER.stem(token)


def _signature_tokens(value: str) -> list[str]:
    normalized = _normalize_ngram_text(value)
    tokens = re.findall(r"[a-z0-9]+", normalized)
    cleaned: list[str] = []
    for token in tokens:
        if token in TOKEN_STOPWORDS:
            continue
        if len(token) == 1 and not token.isdigit():
            continue
        cleaned.append(_stem_token(token))
    return cleaned


def _display_tokens(value: str) -> list[str]:
    normalized = _normalize_ngram_text(value)
    tokens = re.findall(r"[a-z0-9]+", normalized)
    cleaned: list[str] = []
    for token in tokens:
        if token in TOKEN_STOPWORDS:
            continue
        if len(token) == 1 and not token.isdigit():
            continue
        cleaned.append(token)
    return cleaned


def _signature(value: str) -> str:
    return " ".join(_signature_tokens(value))


def _compile_tag_library() -> tuple[dict[str, dict[str, dict[str, Any]]], set[str], dict[str, str]]:
    compiled: dict[str, dict[str, dict[str, Any]]] = {}
    reserved_signatures: set[str] = set()
    labels: dict[str, str] = {}
    for family, tag_map in TAG_LIBRARY.items():
        compiled[family] = {}
        for tag_key, metadata in tag_map.items():
            tag = f"{family}:{tag_key}"
            signatures = {_signature(alias) for alias in metadata.get("aliases", [])}
            signatures.discard("")
            payload = {
                **metadata,
                "tag": tag,
                "tag_key": tag_key,
                "family": family,
                "signatures": signatures,
                "suppressed_by": metadata.get("suppressed_by", []),
            }
            compiled[family][tag] = payload
            labels[tag] = str(metadata["label"])
            reserved_signatures.update(signatures)
    return compiled, reserved_signatures, labels


COMPILED_TAG_LIBRARY, RESERVED_TAG_SIGNATURES, TAG_LABELS = _compile_tag_library()


class DashboardService:
    def __init__(self) -> None:
        self._ensure_seedless()

    def _ensure_seedless(self) -> None:
        # The database is initialized by main.py on startup.
        return None

    def _load_json_blob(self, value: Any, fallback: Any) -> Any:
        if isinstance(value, (dict, list)):
            return value
        if value in (None, "", b""):
            return fallback
        try:
            return json.loads(value)
        except (TypeError, json.JSONDecodeError):
            return fallback

    def _project_payload(self, row: Any) -> dict[str, Any]:
        project = dict(row)
        project["saved_view_state"] = self._load_json_blob(project.get("saved_view_state"), {})
        return project

    def _normalize_view_state(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = payload or {}
        group_filters = payload.get("group_filters") if isinstance(payload.get("group_filters"), dict) else {}
        keyword_filters = payload.get("keyword_filters") if isinstance(payload.get("keyword_filters"), dict) else {}
        mode = str(payload.get("mode") or "team").strip().lower()
        active_tab = str(payload.get("active_tab") or "overview").strip().lower()
        return {
            "mode": mode if mode in {"team", "client"} else "team",
            "active_tab": active_tab if active_tab in {"overview", "groups", "keywords"} else "overview",
            "weekly_note_range": {
                "from_date": str(
                    (payload.get("weekly_note_range") or {}).get("from_date")
                    if isinstance(payload.get("weekly_note_range"), dict)
                    else ""
                ).strip(),
                "to_date": str(
                    (payload.get("weekly_note_range") or {}).get("to_date")
                    if isinstance(payload.get("weekly_note_range"), dict)
                    else ""
                ).strip(),
            },
            "group_filters": {
                "current_date": str(group_filters.get("current_date") or "").strip(),
                "baseline_date": str(group_filters.get("baseline_date") or "").strip(),
                "status": str(group_filters.get("status") or "all").strip() or "all",
                "main_cluster": str(group_filters.get("main_cluster") or "").strip(),
                "tag": str(group_filters.get("tag") or "all").strip() or "all",
                "sort_by": str(group_filters.get("sort_by") or "health_score").strip() or "health_score",
                "active_scenario_id": str(
                    group_filters.get("active_scenario_id")
                    or group_filters.get("sub_cluster_mode")
                    or ""
                ).strip(),
            },
            "keyword_filters": {
                "current_date": str(keyword_filters.get("current_date") or "").strip(),
                "search": str(keyword_filters.get("search") or "").strip(),
                "groups": str(keyword_filters.get("groups") or "").strip(),
                "clusters": str(keyword_filters.get("clusters") or "").strip(),
                "status": str(keyword_filters.get("status") or "all").strip() or "all",
                "vol_min": int(keyword_filters.get("vol_min") or 0),
                "vol_max": int(keyword_filters.get("vol_max") or 1000000),
                "rank_min": float(keyword_filters.get("rank_min") or 0),
                "rank_max": float(keyword_filters.get("rank_max") or 101),
                "movers_only": bool(keyword_filters.get("movers_only")),
            },
        }

    def _merge_view_state(
        self,
        current_state: dict[str, Any] | None,
        incoming_state: dict[str, Any] | None,
    ) -> dict[str, Any]:
        current = self._normalize_view_state(current_state)
        incoming = self._normalize_view_state(incoming_state)
        return {
            "mode": incoming.get("mode") or current.get("mode") or "team",
            "active_tab": incoming.get("active_tab") or current.get("active_tab") or "overview",
            "weekly_note_range": {
                **current.get("weekly_note_range", {}),
                **incoming.get("weekly_note_range", {}),
            },
            "group_filters": {
                **current.get("group_filters", {}),
                **incoming.get("group_filters", {}),
            },
            "keyword_filters": {
                **current.get("keyword_filters", {}),
                **incoming.get("keyword_filters", {}),
            },
        }

    def _public_base_url(self) -> str:
        candidates = [
            os.getenv("PUBLIC_BASE_URL"),
            os.getenv("RENDER_EXTERNAL_URL"),
            os.getenv("RAILWAY_PUBLIC_DOMAIN"),
            os.getenv("APP_BASE_URL"),
        ]
        for candidate in candidates:
            if not candidate:
                continue
            value = candidate.strip().rstrip("/")
            if not value:
                continue
            if value.startswith(("http://", "https://")):
                return value
            return f"https://{value}"
        return "http://localhost:8000"

    def _share_url(self, share_type: str, share_token: str) -> str:
        path_map = {
            "client_view": "client",
            "seo_view": "seo",
            "report_snapshot": "report",
        }
        path = path_map.get(share_type, "client")
        return f"{self._public_base_url()}/{path}/{share_token}"

    def _share_payload(self, row: Any) -> dict[str, Any]:
        share = dict(row)
        share["state_json"] = self._load_json_blob(share.get("state_json"), {})
        share["snapshot_json"] = self._load_json_blob(share.get("snapshot_json"), None)
        share["url"] = self._share_url(share["share_type"], share["share_token"])
        return share

    def _latest_share_links(self, project_id: int) -> dict[str, str | None]:
        with get_connection() as connection:
            rows = connection.execute(
                """
                SELECT share_type, share_token
                FROM shared_views
                WHERE project_id = ?
                ORDER BY updated_at DESC, id DESC
                """,
                (project_id,),
            ).fetchall()
        latest: dict[str, str | None] = {
            "client_view_url": None,
            "seo_view_url": None,
            "report_snapshot_url": None,
        }
        for row in rows:
            share_type = row["share_type"]
            if share_type == "client_view" and not latest["client_view_url"]:
                latest["client_view_url"] = self._share_url(share_type, row["share_token"])
            if share_type == "seo_view" and not latest["seo_view_url"]:
                latest["seo_view_url"] = self._share_url(share_type, row["share_token"])
            if share_type == "report_snapshot" and not latest["report_snapshot_url"]:
                latest["report_snapshot_url"] = self._share_url(share_type, row["share_token"])
        return latest

    def _safe_public_project(self, project: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": project.get("id"),
            "name": project.get("name"),
            "source_name": project.get("source_name"),
            "source_type": project.get("source_type"),
            "last_pulled_at": project.get("last_pulled_at"),
            "created_at": project.get("created_at"),
        }

    def _sanitize_public_overview(self, overview: dict[str, Any]) -> dict[str, Any]:
        payload = dict(overview)
        payload.pop("projects", None)
        project = payload.get("project")
        if isinstance(project, dict):
            payload["project"] = self._safe_public_project(project)
        return payload

    def _public_view_mode(self, share_type: str, state: dict[str, Any] | None = None) -> str:
        if share_type == "client_view":
            return "client"
        if share_type == "seo_view":
            return "team"
        normalized_state = self._normalize_view_state(state)
        return normalized_state.get("mode") or "client"

    def _public_available_tabs(self, view_mode: str, keyword_table: dict[str, Any] | None) -> list[str]:
        tabs = ["overview", "groups"]
        if view_mode == "team" and keyword_table is not None:
            tabs.append("keywords")
        return tabs

    def list_projects(self) -> list[dict[str, Any]]:
        with get_connection() as connection:
            rows = connection.execute(
                """
                SELECT
                    p.*,
                    COUNT(DISTINCT k.id) AS keyword_count,
                    COUNT(DISTINCT r.rank_date) AS date_count
                FROM projects p
                LEFT JOIN keywords k ON k.project_id = p.id
                LEFT JOIN rankings r ON r.keyword_id = k.id
                GROUP BY p.id
                ORDER BY p.created_at DESC
                """
            ).fetchall()
        return [self._project_payload(row) for row in rows]

    def get_project(self, project_id: int) -> dict[str, Any]:
        with get_connection() as connection:
            row = connection.execute("SELECT * FROM projects WHERE id = ?", (project_id,)).fetchone()
        if not row:
            raise ValueError("Không tìm thấy project.")
        return self._project_payload(row)

    def create_project(
        self,
        name: str,
        *,
        sheet_url: str | None = None,
        source_name: str | None = None,
        source_type: str = "upload",
        refresh_interval_minutes: int = 30,
        anthropic_api_key: str | None = None,
    ) -> dict[str, Any]:
        with transaction() as connection:
            cursor = connection.execute(
                """
                INSERT INTO projects (
                    name, sheet_url, source_name, source_type,
                    refresh_interval_minutes, anthropic_api_key,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    name.strip() or "SEO Project",
                    sheet_url,
                    source_name or name.strip() or "SEO Project",
                    source_type,
                    refresh_interval_minutes,
                    anthropic_api_key,
                    now_iso(),
                ),
            )
            project_id = int(cursor.lastrowid)
        return self.get_project(project_id)

    def delete_project(self, project_id: int) -> None:
        with transaction() as connection:
            connection.execute("DELETE FROM projects WHERE id = ?", (project_id,))

    def get_project_dates(self, project_id: int) -> list[str]:
        with get_connection() as connection:
            rows = connection.execute(
                """
                SELECT DISTINCT r.rank_date
                FROM rankings r
                JOIN keywords k ON k.id = r.keyword_id
                WHERE k.project_id = ?
                ORDER BY r.rank_date ASC
                """,
                (project_id,),
            ).fetchall()
        return [row["rank_date"] for row in rows]

    def _project_api_key(self, project_id: int) -> str | None:
        project = self.get_project(project_id)
        return (project.get("anthropic_api_key") or os.getenv("ANTHROPIC_API_KEY") or "").strip() or None

    def test_google_sheet(self, sheet_url: str, sheet_gid: str | None = None) -> dict[str, Any]:
        payload, filename, gid, source_type = fetch_public_data_source(sheet_url, preferred_gid=sheet_gid)
        parsed = parse_spreadsheet_payload(filename, payload, source_name="Google Sheets")
        return {
            "ok": True,
            "source_name": parsed.source_name,
            "dates": parsed.dates,
            "header_row_index": parsed.header_row_index,
            "selected_sheet_name": parsed.selected_sheet_name,
            "warnings": parsed.warnings[:10],
            "row_count": len(parsed.rows),
            "sheet_gid": gid,
            "source_type": source_type,
        }

    def import_upload(self, project_id: int, filename: str, payload: bytes) -> dict[str, Any]:
        parsed = parse_spreadsheet_payload(filename, payload, source_name=filename)
        return self._ingest_parsed_sheet(
            project_id,
            parsed,
            source_type="upload",
            source_name=filename,
        )

    def refresh_from_google_sheet(self, project_id: int) -> dict[str, Any]:
        project = self.get_project(project_id)
        sheet_url = (project.get("sheet_url") or "").strip()
        if not sheet_url:
            raise ValueError("Project chưa có Google Sheet URL.")
        payload, filename, gid, source_type = fetch_public_data_source(
            sheet_url,
            preferred_gid=project.get("sheet_gid"),
        )
        parsed = parse_spreadsheet_payload(filename, payload, source_name="Google Sheets")
        return self._ingest_parsed_sheet(
            project_id,
            parsed,
            source_type=source_type,
            source_name="Google Sheets",
            sheet_url=sheet_url,
            sheet_gid=gid,
        )

    def _ingest_parsed_sheet(
        self,
        project_id: int,
        parsed: Any,
        *,
        source_type: str,
        source_name: str,
        sheet_url: str | None = None,
        sheet_gid: str | None = None,
    ) -> dict[str, Any]:
        existing_dates = set(self.get_project_dates(project_id))
        imported_rankings = 0
        display_source_name = source_name
        if getattr(parsed, "selected_sheet_name", None):
            display_source_name = f"{source_name} · {parsed.selected_sheet_name}"
        with transaction() as connection:
            existing_clusters = {
                (row["group_name"], row["name"]): dict(row)
                for row in connection.execute(
                    "SELECT * FROM clusters WHERE project_id = ?",
                    (project_id,),
                ).fetchall()
            }
            existing_keywords = {
                row["keyword"]: dict(row)
                for row in connection.execute(
                    """
                    SELECT id, keyword, group_name, cluster_name, kpi_target
                    FROM keywords
                    WHERE project_id = ?
                    """,
                    (project_id,),
                ).fetchall()
            }
            current_time = now_iso()
            for row in parsed.rows:
                cluster_override = existing_clusters.get((row.group_name, row.cluster_name))
                resolved_kpi_target = (
                    int(cluster_override["kpi_target"])
                    if cluster_override and cluster_override.get("kpi_target") is not None
                    else int(row.kpi_target or 10)
                )
                keyword_record = existing_keywords.get(row.keyword)
                if keyword_record:
                    keyword_id = keyword_record["id"]
                    if keyword_record.get("kpi_target") is not None:
                        resolved_kpi_target = int(keyword_record["kpi_target"])
                    connection.execute(
                        """
                        UPDATE keywords
                        SET group_name = ?, cluster_name = ?, sub_cluster_name = ?,
                            target_url = ?, found_url = ?, search_volume = ?,
                            best_rank = ?, kpi_target = ?, updated_at = ?
                        WHERE id = ?
                        """,
                        (
                            row.group_name,
                            row.cluster_name,
                            row.sub_cluster_name,
                            row.target_url,
                            row.found_url,
                            row.search_volume,
                            row.best_rank,
                            resolved_kpi_target,
                            current_time,
                            keyword_id,
                        ),
                    )
                else:
                    cursor = connection.execute(
                        """
                        INSERT INTO keywords (
                            project_id, keyword, group_name, cluster_name, sub_cluster_name,
                            target_url, found_url, search_volume, best_rank, kpi_target,
                            created_at, updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            project_id,
                            row.keyword,
                            row.group_name,
                            row.cluster_name,
                            row.sub_cluster_name,
                            row.target_url,
                            row.found_url,
                            row.search_volume,
                            row.best_rank,
                            resolved_kpi_target,
                            current_time,
                            current_time,
                        ),
                    )
                    keyword_id = int(cursor.lastrowid)
                    existing_keywords[row.keyword] = {
                        "id": keyword_id,
                        "keyword": row.keyword,
                        "kpi_target": resolved_kpi_target,
                    }

                for rank_date, position in row.rankings.items():
                    connection.execute(
                        """
                        INSERT INTO rankings (keyword_id, rank_date, position, delta_from_prev, delta_from_baseline)
                        VALUES (?, ?, ?, NULL, NULL)
                        ON CONFLICT(keyword_id, rank_date) DO UPDATE SET
                            position = excluded.position
                        """,
                        (keyword_id, rank_date, position),
                    )
                    imported_rankings += 1

            connection.execute(
                """
                UPDATE projects
                SET source_name = ?, source_type = ?, sheet_url = COALESCE(?, sheet_url),
                    sheet_gid = COALESCE(?, sheet_gid), last_pulled_at = ?
                WHERE id = ?
                """,
                (display_source_name, source_type, sheet_url, sheet_gid, current_time, project_id),
            )
            self._recalculate_deltas(connection, project_id)
            self._refresh_clusters(connection, project_id, parsed)

        new_dates = sorted(set(parsed.dates) - existing_dates)
        self.refresh_anomaly_events(project_id)
        if new_dates:
            self.generate_weekly_summary(project_id, force=False)
        return {
            "project": self.get_project(project_id),
            "imported_keywords": len(parsed.rows),
            "imported_rankings": imported_rankings,
            "dates": parsed.dates,
            "new_dates": new_dates,
            "warnings": parsed.warnings,
        }

    def _recalculate_deltas(self, connection: Any, project_id: int) -> None:
        keyword_rows = connection.execute(
            "SELECT id FROM keywords WHERE project_id = ?",
            (project_id,),
        ).fetchall()
        for keyword_row in keyword_rows:
            rows = connection.execute(
                """
                SELECT id, rank_date, position
                FROM rankings
                WHERE keyword_id = ?
                ORDER BY rank_date ASC
                """,
                (keyword_row["id"],),
            ).fetchall()
            baseline = None
            previous = None
            for row in rows:
                current_position = row["position"]
                if baseline is None:
                    baseline = current_position
                delta_prev = None if previous is None else round(float(current_position - previous), 2)
                delta_baseline = None if baseline is None else round(float(current_position - baseline), 2)
                connection.execute(
                    """
                    UPDATE rankings
                    SET delta_from_prev = ?, delta_from_baseline = ?
                    WHERE id = ?
                    """,
                    (delta_prev, delta_baseline, row["id"]),
                )
                previous = current_position

    def _refresh_clusters(self, connection: Any, project_id: int, parsed: Any) -> None:
        existing_targets = {
            (row["group_name"], row["name"]): dict(row)
            for row in connection.execute(
                "SELECT * FROM clusters WHERE project_id = ?",
                (project_id,),
            ).fetchall()
        }
        cluster_rows = connection.execute(
            """
            SELECT group_name, cluster_name, COUNT(*) AS keyword_count
            FROM keywords
            WHERE project_id = ?
            GROUP BY group_name, cluster_name
            ORDER BY group_name, cluster_name
            """,
            (project_id,),
        ).fetchall()
        connection.execute("DELETE FROM clusters WHERE project_id = ?", (project_id,))
        for row in cluster_rows:
            group_name = row["group_name"] or "Chưa phân nhóm"
            cluster_name = row["cluster_name"] or group_name
            keyword_targets = connection.execute(
                """
                SELECT kpi_target
                FROM keywords
                WHERE project_id = ? AND group_name = ? AND cluster_name = ?
                """,
                (project_id, group_name, cluster_name),
            ).fetchall()
            target_counter = Counter(value["kpi_target"] or 10 for value in keyword_targets)
            previous_cluster = existing_targets.get((group_name, cluster_name))
            kpi_target = (
                int(previous_cluster["kpi_target"])
                if previous_cluster and previous_cluster.get("kpi_target") is not None
                else int(target_counter.most_common(1)[0][0]) if target_counter else parsed.kpi_map.get(group_name, 10)
            )
            target_keywords = (
                parsed.target_keyword_map.get(group_name)
                or (previous_cluster or {}).get("target_keywords")
                or row["keyword_count"]
            )
            connection.execute(
                """
                INSERT INTO clusters (
                    project_id, name, group_name, kpi_target, kpi_type, keyword_count, target_keywords
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    project_id,
                    cluster_name,
                    group_name,
                    kpi_target,
                    kpi_type_from_target(kpi_target),
                    row["keyword_count"],
                    target_keywords,
                ),
            )

    def update_project_settings(self, project_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        project = self.get_project(project_id)
        sheet_url = (payload.get("sheet_url") or project.get("sheet_url") or "").strip() or None
        source_name = (payload.get("source_name") or project.get("source_name") or project["name"]).strip()
        refresh_interval_minutes = int(payload.get("refresh_interval_minutes") or project.get("refresh_interval_minutes") or 30)
        anthropic_api_key = (payload.get("anthropic_api_key") or project.get("anthropic_api_key") or "").strip() or None
        name = (payload.get("name") or project["name"]).strip() or project["name"]
        sheet_gid = (payload.get("sheet_gid") or project.get("sheet_gid") or "").strip() or None
        source_type = project.get("source_type") or "upload"
        if sheet_url:
            try:
                test_result = self.test_google_sheet(sheet_url, sheet_gid)
                sheet_gid = test_result.get("sheet_gid") or sheet_gid
                source_type = test_result.get("source_type") or source_type
            except Exception:
                sheet_gid = project.get("sheet_gid")
        with transaction() as connection:
            connection.execute(
                """
                UPDATE projects
                SET name = ?, sheet_url = ?, sheet_gid = ?, source_name = ?,
                    source_type = ?, refresh_interval_minutes = ?, anthropic_api_key = ?
                WHERE id = ?
                """,
                (
                    name,
                    sheet_url,
                    sheet_gid,
                    source_name,
                    source_type,
                    refresh_interval_minutes,
                    anthropic_api_key,
                    project_id,
                ),
            )
            for cluster in payload.get("clusters", []):
                connection.execute(
                    """
                    UPDATE clusters
                    SET kpi_target = ?, kpi_type = ?, target_keywords = ?
                    WHERE project_id = ? AND name = ? AND group_name = ?
                    """,
                    (
                        int(cluster.get("kpi_target") or 10),
                        kpi_type_from_target(int(cluster.get("kpi_target") or 10)),
                        int(cluster.get("target_keywords") or 0),
                        project_id,
                        cluster.get("name"),
                        cluster.get("group_name"),
                    ),
                )
                connection.execute(
                    """
                    UPDATE keywords
                    SET kpi_target = ?
                    WHERE project_id = ? AND cluster_name = ? AND group_name = ?
                    """,
                    (
                        int(cluster.get("kpi_target") or 10),
                        project_id,
                        cluster.get("name"),
                        cluster.get("group_name"),
                    ),
                )
        return self.get_settings(project_id)

    def get_settings(self, project_id: int) -> dict[str, Any]:
        project = self.get_project(project_id)
        with get_connection() as connection:
            clusters = connection.execute(
                """
                SELECT *
                FROM clusters
                WHERE project_id = ?
                ORDER BY group_name, name
                """,
                (project_id,),
            ).fetchall()
        return {
            "project": project,
            "clusters": [dict(row) for row in clusters],
            **self._latest_share_links(project_id),
            "client_view_password": None,
        }

    def update_project_view_state(self, project_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        project = self.get_project(project_id)
        merged_state = self._merge_view_state(project.get("saved_view_state"), payload)
        with transaction() as connection:
            connection.execute(
                "UPDATE projects SET saved_view_state = ? WHERE id = ?",
                (json.dumps(merged_state, ensure_ascii=True), project_id),
            )
        return {
            "project_id": str(project_id),
            "saved_view_state": merged_state,
        }

    def _load_share(self, share_token: str) -> dict[str, Any]:
        with get_connection() as connection:
            row = connection.execute(
                "SELECT * FROM shared_views WHERE share_token = ?",
                (share_token,),
            ).fetchone()
        if not row:
            raise ValueError("Không tìm thấy link chia sẻ.")
        return self._share_payload(row)

    def _share_state_payload(
        self,
        project_id: int,
        incoming_state: dict[str, Any] | None,
    ) -> dict[str, Any]:
        project = self.get_project(project_id)
        return self._merge_view_state(project.get("saved_view_state"), incoming_state)

    def _build_snapshot_bundle(self, project_id: int, view_state: dict[str, Any]) -> dict[str, Any]:
        group_filters = view_state.get("group_filters", {})
        keyword_filters = view_state.get("keyword_filters", {})
        overview = self._sanitize_public_overview(self.get_overview(project_id))
        base_group_view = self.get_group_view(
            project_id,
            current_date=group_filters.get("current_date") or None,
            baseline_date=group_filters.get("baseline_date") or None,
            status_filter=group_filters.get("status") or "all",
            main_cluster=group_filters.get("main_cluster") or None,
            tag_filter=group_filters.get("tag") or "all",
            sort_by=group_filters.get("sort_by") or "health_score",
            active_scenario_id=group_filters.get("active_scenario_id") or None,
        )
        scenario_views = {}
        for scenario in base_group_view.get("scenarios", []):
            scenario_views[scenario["scenario_id"]] = self.get_group_view(
                project_id,
                current_date=group_filters.get("current_date") or None,
                baseline_date=group_filters.get("baseline_date") or None,
                status_filter=group_filters.get("status") or "all",
                main_cluster=group_filters.get("main_cluster") or None,
                tag_filter=group_filters.get("tag") or "all",
                sort_by=group_filters.get("sort_by") or "health_score",
                active_scenario_id=scenario["scenario_id"],
            )
        keyword_table = (
            self.get_keyword_table(project_id, keyword_filters)
            if self._public_view_mode("report_snapshot", view_state) == "team"
            else None
        )
        return {
            "project": self._safe_public_project(self.get_project(project_id)),
            "overview": overview,
            "view_state": view_state,
            "group_views": scenario_views,
            "keyword_table": keyword_table,
            "created_at": now_iso(),
        }

    def _create_share(
        self,
        project_id: int,
        *,
        share_type: str,
        title: str | None = None,
        password: str | None = None,
        state: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        project = self.get_project(project_id)
        view_state = self._share_state_payload(project_id, state)
        if share_type == "client_view":
            view_state["mode"] = "client"
            if view_state.get("active_tab") == "keywords":
                view_state["active_tab"] = "overview"
        elif share_type == "seo_view":
            view_state["mode"] = "team"
        snapshot_json = None
        if share_type == "report_snapshot":
            snapshot_json = self._build_snapshot_bundle(project_id, view_state)
        share_token = secrets.token_urlsafe(18)
        current_time = now_iso()
        resolved_password = (password or "").strip()
        default_titles = {
            "client_view": f"{project['name']} · Cổng khách hàng",
            "seo_view": f"{project['name']} · Cổng SEO",
            "report_snapshot": f"{project['name']} · Report snapshot",
        }
        with transaction() as connection:
            cursor = connection.execute(
                """
                INSERT INTO shared_views (
                    project_id, share_type, share_token, title, password_hash,
                    state_json, snapshot_json, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    project_id,
                    share_type,
                    share_token,
                    (title or "").strip() or default_titles.get(share_type, f"{project['name']} · Link chia sẻ"),
                    hash_view_password(resolved_password) if resolved_password else None,
                    json.dumps(view_state, ensure_ascii=True),
                    json.dumps(snapshot_json, ensure_ascii=True) if snapshot_json is not None else None,
                    current_time,
                    current_time,
                ),
            )
            row = connection.execute(
                "SELECT * FROM shared_views WHERE id = ?",
                (int(cursor.lastrowid),),
            ).fetchone()
        share = self._share_payload(row)
        return {
            "project_id": str(project_id),
            "share_type": share_type,
            "title": share["title"],
            "client_view_url": share["url"] if share_type == "client_view" else self._latest_share_links(project_id)["client_view_url"],
            "client_view_password": (resolved_password or None) if share_type == "client_view" else None,
            "seo_view_url": share["url"] if share_type == "seo_view" else self._latest_share_links(project_id)["seo_view_url"],
            "seo_view_password": (resolved_password or None) if share_type == "seo_view" else None,
            "report_snapshot_url": share["url"] if share_type == "report_snapshot" else self._latest_share_links(project_id)["report_snapshot_url"],
            "created_at": share["created_at"],
            "active_scenario_id": view_state.get("group_filters", {}).get("active_scenario_id") or None,
        }

    def create_client_view_share(self, project_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        return self._create_share(
            project_id,
            share_type="client_view",
            title=payload.get("title"),
            password=payload.get("password"),
            state=payload.get("state"),
        )

    def create_seo_view_share(self, project_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        return self._create_share(
            project_id,
            share_type="seo_view",
            title=payload.get("title"),
            password=payload.get("password"),
            state=payload.get("state"),
        )

    def create_report_snapshot_share(self, project_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        return self._create_share(
            project_id,
            share_type="report_snapshot",
            title=payload.get("title"),
            password=payload.get("password"),
            state=payload.get("state"),
        )

    def login_public_share(self, share_token: str, password: str) -> dict[str, Any]:
        share = self._load_share(share_token)
        if not verify_view_password(password, share.get("password_hash")):
            raise ValueError("Mật khẩu link chia sẻ không đúng.")
        return {
            "token": create_public_view_token(share_token),
            "expires_in_seconds": PUBLIC_VIEW_TTL_SECONDS,
        }

    def _public_payload_base(self, share: dict[str, Any], view_state: dict[str, Any], keyword_table: dict[str, Any] | None) -> dict[str, Any]:
        view_mode = self._public_view_mode(share["share_type"], view_state)
        active_tab = view_state.get("active_tab") or "overview"
        if view_mode == "client" and active_tab == "keywords":
            active_tab = "overview"
        if view_mode == "client":
            keyword_table = None
        return {
            "requires_password": False,
            "share_type": share["share_type"],
            "title": share["title"],
            "project_id": str(share["project_id"]),
            "project_name": self.get_project(share["project_id"])["name"],
            "view_mode": view_mode,
            "available_tabs": self._public_available_tabs(view_mode, keyword_table),
            "view_state": {
                **view_state,
                "active_tab": active_tab,
                "mode": view_mode,
            },
            "snapshot_created_at": share["created_at"],
        }

    def _build_public_live_payload(
        self,
        share: dict[str, Any],
        *,
        group_filters: dict[str, Any] | None = None,
        keyword_filters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        merged_state = self._merge_view_state(
            share.get("state_json"),
            {
                "group_filters": group_filters or {},
                "keyword_filters": keyword_filters or {},
            },
        )
        overview = self._sanitize_public_overview(self.get_overview(share["project_id"]))
        current_group_filters = merged_state.get("group_filters", {})
        current_keyword_filters = merged_state.get("keyword_filters", {})
        group_view = self.get_group_view(
            share["project_id"],
            current_date=current_group_filters.get("current_date") or None,
            baseline_date=current_group_filters.get("baseline_date") or None,
            status_filter=current_group_filters.get("status") or "all",
            main_cluster=current_group_filters.get("main_cluster") or None,
            tag_filter=current_group_filters.get("tag") or "all",
            sort_by=current_group_filters.get("sort_by") or "health_score",
            active_scenario_id=current_group_filters.get("active_scenario_id") or None,
        )
        keyword_table = (
            self.get_keyword_table(share["project_id"], current_keyword_filters)
            if self._public_view_mode(share["share_type"], merged_state) == "team"
            else None
        )
        payload = self._public_payload_base(share, merged_state, keyword_table)
        payload.update(
            {
                "overview": overview,
                "group_view": group_view,
                "keyword_table": keyword_table,
            }
        )
        return payload

    def _build_public_snapshot_payload(
        self,
        share: dict[str, Any],
        *,
        active_scenario_id: str | None = None,
    ) -> dict[str, Any]:
        snapshot = share.get("snapshot_json") or {}
        view_state = self._normalize_view_state(snapshot.get("view_state") or share.get("state_json"))
        group_views = snapshot.get("group_views") or {}
        requested_scenario_id = active_scenario_id or view_state.get("group_filters", {}).get("active_scenario_id")
        selected_group_view = None
        if requested_scenario_id:
            selected_group_view = group_views.get(requested_scenario_id)
        if selected_group_view is None and group_views:
            selected_group_view = next(iter(group_views.values()))
        keyword_table = snapshot.get("keyword_table")
        if self._public_view_mode(share["share_type"], view_state) != "team":
            keyword_table = None
        payload = self._public_payload_base(share, view_state, keyword_table)
        payload.update(
            {
                "project_name": (snapshot.get("project") or {}).get("name") or payload["project_name"],
                "overview": snapshot.get("overview") or self._sanitize_public_overview(self.get_overview(share["project_id"])),
                "group_view": selected_group_view,
                "keyword_table": keyword_table,
                "snapshot_created_at": snapshot.get("created_at") or share["created_at"],
            }
        )
        return payload

    def get_public_share_payload(
        self,
        share_token: str,
        *,
        public_token: str | None = None,
        group_filters: dict[str, Any] | None = None,
        keyword_filters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        share = self._load_share(share_token)
        requires_password = bool(share.get("password_hash"))
        if requires_password:
            try:
                verify_public_view_token(public_token or "", share_token)
            except Exception:
                return {
                    "requires_password": True,
                    "share_type": share["share_type"],
                    "title": share["title"],
                    "project_id": str(share["project_id"]),
                }
        if share["share_type"] == "report_snapshot":
            requested_scenario_id = (
                (group_filters or {}).get("active_scenario_id")
                or (group_filters or {}).get("sub_cluster_mode")
                or None
            )
            return self._build_public_snapshot_payload(
                share,
                active_scenario_id=requested_scenario_id,
            )
        return self._build_public_live_payload(
            share,
            group_filters=group_filters,
            keyword_filters=keyword_filters,
        )

    def get_public_keyword_detail(
        self,
        share_token: str,
        keyword_id: int,
        *,
        public_token: str | None = None,
    ) -> dict[str, Any]:
        share = self._load_share(share_token)
        if share["share_type"] != "seo_view":
            raise ValueError("Link này không hỗ trợ mở chi tiết keyword.")
        if share.get("password_hash"):
            verify_public_view_token(public_token or "", share_token)
        return self.get_keyword_detail(share["project_id"], keyword_id)

    def recluster_keywords(self, project_id: int) -> dict[str, Any]:
        with transaction() as connection:
            keywords = connection.execute(
                "SELECT id, keyword FROM keywords WHERE project_id = ?",
                (project_id,),
            ).fetchall()
            for row in keywords:
                connection.execute(
                    "UPDATE keywords SET sub_cluster_name = ?, updated_at = ? WHERE id = ?",
                    (infer_sub_cluster_name(row["keyword"]), now_iso(), row["id"]),
                )
        return {"ok": True, "updated": len(keywords)}

    def _load_keywords_with_history(self, project_id: int) -> list[dict[str, Any]]:
        with get_connection() as connection:
            keywords = [
                dict(row)
                for row in connection.execute(
                    """
                    SELECT *
                    FROM keywords
                    WHERE project_id = ?
                    ORDER BY group_name, cluster_name, keyword
                    """,
                    (project_id,),
                ).fetchall()
            ]
            rankings = connection.execute(
                """
                SELECT r.*, k.project_id
                FROM rankings r
                JOIN keywords k ON k.id = r.keyword_id
                WHERE k.project_id = ?
                ORDER BY r.rank_date ASC
                """,
                (project_id,),
            ).fetchall()
        histories: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for row in rankings:
            histories[row["keyword_id"]].append(
                {
                    "rank_date": row["rank_date"],
                    "position": float(row["position"]) if row["position"] is not None else None,
                    "delta_from_prev": row["delta_from_prev"],
                    "delta_from_baseline": row["delta_from_baseline"],
                }
            )
        for keyword in keywords:
            keyword["history"] = histories.get(keyword["id"], [])
        return keywords

    def _load_events(self, project_id: int) -> list[dict[str, Any]]:
        with get_connection() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM events
                WHERE project_id = ?
                ORDER BY event_date DESC, id DESC
                """,
                (project_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def _load_latest_insights(self, project_id: int) -> dict[str, dict[str, Any]]:
        with get_connection() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM ai_insights
                WHERE project_id = ?
                ORDER BY generated_at DESC
                """,
                (project_id,),
            ).fetchall()
        latest: dict[str, dict[str, Any]] = {}
        for row in rows:
            insight = dict(row)
            latest.setdefault(insight["insight_type"], insight)
        return latest

    def _load_pinned_date_notes(self, project_id: int) -> dict[str, dict[str, Any]]:
        with get_connection() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM ai_insights
                WHERE project_id = ? AND insight_type = 'pinned_daily_note'
                ORDER BY generated_at DESC
                """,
                (project_id,),
            ).fetchall()
        pinned: dict[str, dict[str, Any]] = {}
        for row in rows:
            insight = dict(row)
            pinned.setdefault(insight["insight_date"], insight)
        return pinned

    def _load_pinned_weekly_range_note(
        self,
        project_id: int,
        from_date: str,
        to_date: str,
    ) -> dict[str, Any] | None:
        with get_connection() as connection:
            row = connection.execute(
                """
                SELECT *
                FROM ai_insights
                WHERE project_id = ?
                  AND insight_type = 'weekly_range_note'
                  AND insight_date = ?
                  AND COALESCE(range_end, '') = ?
                  AND is_pinned = 1
                ORDER BY generated_at DESC, id DESC
                LIMIT 1
                """,
                (project_id, from_date, to_date),
            ).fetchone()
        return dict(row) if row else None

    def _resolve_weekly_note_range(
        self,
        dates: list[str],
        from_date: str | None,
        to_date: str | None,
    ) -> dict[str, Any]:
        if not dates:
            raise ValueError("Project chưa có dữ liệu để tạo nhận xét.")
        resolved_to = to_date if to_date in dates else dates[-1]
        default_from = dates[max(0, len(dates) - 7)]
        resolved_from = from_date if from_date in dates else default_from
        start_index = dates.index(resolved_from)
        end_index = dates.index(resolved_to)
        if start_index > end_index:
            start_index, end_index = end_index, start_index
            resolved_from, resolved_to = dates[start_index], dates[end_index]
        current_dates = dates[start_index : end_index + 1]
        period_length = len(current_dates)
        previous_dates = dates[max(0, start_index - period_length) : start_index]
        long_term_dates = dates[:start_index]
        return {
            "from_date": resolved_from,
            "to_date": resolved_to,
            "current_dates": current_dates,
            "compare_from_date": previous_dates[0] if previous_dates else None,
            "compare_to_date": previous_dates[-1] if previous_dates else None,
            "compare_dates": previous_dates,
            "baseline_from_date": long_term_dates[0] if long_term_dates else None,
            "baseline_to_date": long_term_dates[-1] if long_term_dates else None,
            "baseline_dates": long_term_dates,
        }

    def _build_weekly_note_context(
        self,
        project_id: int,
        from_date: str | None,
        to_date: str | None,
    ) -> dict[str, Any]:
        dates = self.get_project_dates(project_id)
        resolved_range = self._resolve_weekly_note_range(dates, from_date, to_date)
        keywords = self._load_keywords_with_history(project_id)
        current_dates = resolved_range["current_dates"]
        compare_dates = resolved_range["compare_dates"]
        baseline_dates = resolved_range["baseline_dates"]

        metrics: dict[str, dict[str, Any]] = {}
        for keyword in keywords:
            group_name = keyword.get("group_name") or "Chưa phân nhóm"
            current_positions = [
                float(position)
                for rank_date in current_dates
                if (position := self._history_position(keyword["history"], rank_date)) is not None
            ]
            if not current_positions:
                continue
            compare_positions = [
                float(position)
                for rank_date in compare_dates
                if (position := self._history_position(keyword["history"], rank_date)) is not None
            ]
            baseline_positions = [
                float(position)
                for rank_date in baseline_dates
                if (position := self._history_position(keyword["history"], rank_date)) is not None
            ]
            entry = metrics.setdefault(
                group_name,
                {
                    "name": group_name,
                    "keyword_count": 0,
                    "kpi_targets": [],
                    "volumes": [],
                    "current_positions": [],
                    "compare_positions": [],
                    "baseline_positions": [],
                    "latest_ranks": [],
                },
            )
            entry["keyword_count"] += 1
            entry["current_positions"].extend(current_positions)
            entry["compare_positions"].extend(compare_positions)
            entry["baseline_positions"].extend(baseline_positions)
            entry["kpi_targets"].append(int(keyword.get("kpi_target") or 10))
            if keyword.get("search_volume") is not None:
                entry["volumes"].append(float(keyword["search_volume"]))
            latest_rank = self._history_position(keyword["history"], current_dates[-1])
            if latest_rank is not None:
                entry["latest_ranks"].append(float(latest_rank))

        max_total_volume = max((sum(entry["volumes"]) for entry in metrics.values()), default=1.0)
        group_rows: list[dict[str, Any]] = []
        for entry in metrics.values():
            avg_rank_current = safe_mean(entry["current_positions"])
            avg_rank_previous = safe_mean(entry["compare_positions"])
            long_term_avg = safe_mean(entry["baseline_positions"])
            compare_reference = avg_rank_previous if avg_rank_previous is not None else long_term_avg
            rank_delta = (
                round(float(compare_reference - avg_rank_current), 2)
                if avg_rank_current is not None and compare_reference is not None
                else 0.0
            )
            if rank_delta >= 2:
                trend_status = "rising"
            elif rank_delta <= -2:
                trend_status = "declining"
            else:
                trend_status = "stable"
            total_volume = int(sum(entry["volumes"]))
            volume_component = clamp((total_volume / max_total_volume) * 100 if max_total_volume else 0, 0, 100)
            rank_component = (
                clamp((101 - avg_rank_current) / 100 * 100, 0, 100)
                if avg_rank_current is not None
                else 0
            )
            trend_component = clamp(50 + rank_delta * 12, 0, 100)
            health_score = int(round((rank_component * 0.5) + (trend_component * 0.3) + (volume_component * 0.2)))
            group_rows.append(
                {
                    "name": entry["name"],
                    "keyword_count": entry["keyword_count"],
                    "total_volume": total_volume,
                    "avg_volume": round(total_volume / entry["keyword_count"], 1) if entry["keyword_count"] else 0,
                    "avg_rank_current": avg_rank_current,
                    "avg_rank_previous": avg_rank_previous,
                    "long_term_avg": long_term_avg,
                    "rank_delta": rank_delta,
                    "trend_status": trend_status,
                    "health_score": health_score,
                    "latest_avg_rank": safe_mean(entry["latest_ranks"]),
                    "kpi_target": Counter(entry["kpi_targets"]).most_common(1)[0][0] if entry["kpi_targets"] else 10,
                }
            )

        group_rows.sort(
            key=lambda item: (
                -(item.get("health_score") or 0),
                -(item.get("rank_delta") or 0),
                item.get("avg_rank_current") or 999,
            )
        )

        opportunities = sorted(
            [
                item
                for item in group_rows
                if item["trend_status"] == "rising" and (item.get("avg_rank_current") or 999) > 5
            ],
            key=lambda item: (-(item.get("rank_delta") or 0), -(item.get("health_score") or 0)),
        )
        watchlist = sorted(
            [
                item
                for item in group_rows
                if item["trend_status"] == "declining" or (item.get("health_score") or 0) < 55
            ],
            key=lambda item: ((item.get("rank_delta") or 0), item.get("health_score") or 0),
        )

        overall_delta = safe_mean([float(item["rank_delta"]) for item in group_rows]) or 0.0
        compare_label = (
            f"{format_date_label(resolved_range['compare_from_date'])} - {format_date_label(resolved_range['compare_to_date'])}"
            if resolved_range["compare_from_date"] and resolved_range["compare_to_date"]
            else "kỳ trước tương đương"
        )
        baseline_label = (
            f"baseline {format_date_label(resolved_range['baseline_from_date'])} - {format_date_label(resolved_range['baseline_to_date'])}"
            if resolved_range["baseline_from_date"] and resolved_range["baseline_to_date"]
            else "baseline dài hạn"
        )

        return {
            **resolved_range,
            "project_id": str(project_id),
            "from_label": format_date_label(resolved_range["from_date"]),
            "to_label": format_date_label(resolved_range["to_date"]),
            "compare_label": compare_label,
            "baseline_label": baseline_label,
            "overall_delta": overall_delta,
            "groups": group_rows,
            "opportunities": opportunities[:3],
            "watchlist": watchlist[:3],
            "group_breakdowns": self._build_weekly_group_breakdowns(
                project_id,
                [item["name"] for item in group_rows],
                resolved_range["to_date"],
                resolved_range["compare_to_date"] or resolved_range["to_date"],
            ),
        }

    def _build_weekly_group_breakdowns(
        self,
        project_id: int,
        group_names: list[str],
        current_date: str,
        baseline_date: str,
    ) -> list[dict[str, Any]]:
        breakdowns: list[dict[str, Any]] = []
        for group_name in group_names:
            group_view = self._build_cluster_view(
                project_id,
                current_date=current_date,
                baseline_date=baseline_date,
                selected_group=group_name,
                status_filter="all",
                tag_filter="all",
                sort_by="health_score",
                active_scenario_id=None,
            )
            cluster_list = group_view.get("cluster_list") or []
            bright_clusters = [
                {
                    "name": cluster["cluster_name"],
                    "rank_delta": cluster["rank_delta"],
                    "health_score": cluster["health_score"],
                }
                for cluster in cluster_list
                if cluster["trend_status"] == "rising"
            ][:2]
            watch_clusters = [
                {
                    "name": cluster["cluster_name"],
                    "rank_delta": cluster["rank_delta"],
                    "health_score": cluster["health_score"],
                }
                for cluster in cluster_list
                if cluster["trend_status"] == "declining"
            ][:2]
            if not watch_clusters:
                watch_clusters = [
                    {
                        "name": cluster["cluster_name"],
                        "rank_delta": cluster["rank_delta"],
                        "health_score": cluster["health_score"],
                    }
                    for cluster in sorted(
                        cluster_list,
                        key=lambda item: (item.get("health_score") or 999, item.get("rank_delta") or 0),
                    )[:2]
                ]
            all_keywords = [
                {
                    **keyword,
                    "cluster_name": cluster["cluster_name"],
                }
                for cluster in cluster_list
                for keyword in cluster.get("keywords", [])
            ]
            best_keyword = None
            if all_keywords:
                best_keyword = max(
                    all_keywords,
                    key=lambda item: (
                        float(item.get("rank_delta") or 0),
                        -(float(item.get("current_rank") or 999)),
                    ),
                )
            worst_keyword = None
            if all_keywords:
                worst_keyword = min(
                    all_keywords,
                    key=lambda item: (
                        float(item.get("rank_delta") or 0),
                        float(item.get("current_rank") or 999),
                    ),
                )
            breakdowns.append(
                {
                    "group_name": group_name,
                    "scenario_label": group_view.get("scenarios", [{}])[0].get("scenario_label") if group_view.get("scenarios") else None,
                    "bright_clusters": bright_clusters,
                    "watch_clusters": watch_clusters,
                    "best_keyword": (
                        {
                            "keyword": best_keyword["keyword"],
                            "cluster_name": best_keyword["cluster_name"],
                            "rank_delta": best_keyword["rank_delta"],
                            "previous_rank": best_keyword["previous_rank"],
                            "current_rank": best_keyword["current_rank"],
                            "from_date": baseline_date,
                            "to_date": current_date,
                        }
                        if best_keyword and (best_keyword.get("rank_delta") or 0) > 0
                        else None
                    ),
                    "worst_keyword": (
                        {
                            "keyword": worst_keyword["keyword"],
                            "cluster_name": worst_keyword["cluster_name"],
                            "rank_delta": worst_keyword["rank_delta"],
                            "previous_rank": worst_keyword["previous_rank"],
                            "current_rank": worst_keyword["current_rank"],
                            "from_date": baseline_date,
                            "to_date": current_date,
                        }
                        if worst_keyword and (worst_keyword.get("rank_delta") or 0) < 0
                        else None
                    ),
                }
            )
        return breakdowns

    def _weekly_note_payload(
        self,
        *,
        context: dict[str, Any],
        content: str,
        generated_at: str,
        is_pinned: bool,
        author: str,
        source: str,
    ) -> dict[str, Any]:
        return {
            "project_id": context["project_id"],
            "insight_type": "weekly_range_note",
            "from_date": context["from_date"],
            "to_date": context["to_date"],
            "compare_from_date": context["compare_from_date"],
            "compare_to_date": context["compare_to_date"],
            "baseline_from_date": context["baseline_from_date"],
            "baseline_to_date": context["baseline_to_date"],
            "content_vi": content,
            "generated_at": generated_at,
            "saved_at": generated_at if is_pinned else None,
            "author": author,
            "is_pinned": is_pinned,
            "source": source,
            "summary": {
                "groups": context["groups"][:5],
                "opportunities": context["opportunities"],
                "watchlist": context["watchlist"],
            },
        }

    def generate_weekly_range_note(
        self,
        project_id: int,
        *,
        from_date: str | None = None,
        to_date: str | None = None,
        seo_input: str = "",
        force: bool = False,
        allow_ai: bool = True,
    ) -> dict[str, Any]:
        context = self._build_weekly_note_context(project_id, from_date, to_date)
        if not force:
            pinned = self._load_pinned_weekly_range_note(
                project_id,
                context["from_date"],
                context["to_date"],
            )
            if pinned:
                return self._weekly_note_payload(
                    context=context,
                    content=str(pinned.get("content_vi") or ""),
                    generated_at=str(pinned.get("generated_at") or now_iso()),
                    is_pinned=True,
                    author=str(pinned.get("author") or "AI"),
                    source="saved",
                )

        data_payload = {
            "current_range": {
                "from_date": context["from_date"],
                "to_date": context["to_date"],
            },
            "compare_range": {
                "from_date": context["compare_from_date"],
                "to_date": context["compare_to_date"],
            },
            "baseline_range": {
                "from_date": context["baseline_from_date"],
                "to_date": context["baseline_to_date"],
            },
            "groups": context["groups"][:6],
            "group_breakdowns": context["group_breakdowns"],
            "opportunities": context["opportunities"],
            "watchlist": context["watchlist"],
            "seo_input": seo_input.strip() or None,
        }
        prompt = (
            "Bạn là SEO analyst đang đọc dữ liệu từ dashboard SEO.\n"
            f"Dữ liệu hiện tại: {data_payload}\n"
            "Hãy viết nhận xét bằng tiếng Việt, ngắn gọn, bám sát dữ liệu và chia đúng 3 phần:\n"
            "Tổng quan:\n"
            "- 1-2 câu tóm tắt toàn cảnh trong khoảng ngày đang chọn.\n"
            "Các điểm sáng:\n"
            "- Phải nhắc đủ từng cluster chính trong dữ liệu (ví dụ B2B, M2 Extensions, SEO).\n"
            "- Với mỗi cluster chính, nêu rõ sub-cluster đang sáng nhất.\n"
            "- Nếu nói tăng/phục hồi thì cố gắng ghi rõ từ top nào lên top nào, từ ngày nào đến ngày nào.\n"
            "- Nếu có thể, thêm 1 trường hợp đặc biệt là keyword hiệu suất tốt.\n"
            "Các điểm cần chú ý:\n"
            "- Cũng phải nhắc đủ từng cluster chính.\n"
            "- Với mỗi cluster chính, nêu sub-cluster cần theo dõi nhất.\n"
            "- Nếu nói giảm thì cố gắng ghi rõ từ top nào xuống top nào, từ ngày nào đến ngày nào.\n"
            "- Nếu có thể, thêm 1 trường hợp đặc biệt là keyword giảm mạnh / rất không tốt.\n"
            "Yêu cầu:\n"
            "- Không bỏ sót cluster nào.\n"
            "- Không dùng markdown bullet ký hiệu; chỉ viết plain text thành từng đoạn rõ ràng.\n"
            "- Không viết chung chung kiểu dữ liệu ổn. Phải bám tên cluster/sub-cluster thật trong dữ liệu."
        )
        api_key = self._project_api_key(project_id) if allow_ai else None
        content = call_claude(prompt, api_key) or fallback_weekly_range_note(context)
        return self._weekly_note_payload(
            context=context,
            content=content,
            generated_at=now_iso(),
            is_pinned=False,
            author="AI",
            source="generated",
        )

    def save_weekly_range_note(
        self,
        project_id: int,
        *,
        from_date: str | None,
        to_date: str | None,
        content: str,
        author: str = "AI",
    ) -> dict[str, Any]:
        context = self._build_weekly_note_context(project_id, from_date, to_date)
        note = content.strip()
        if not note:
            raise ValueError("Nội dung nhận xét không được để trống.")
        resolved_author = author.strip() or "AI"
        generated_at = now_iso()
        with transaction() as connection:
            connection.execute(
                """
                DELETE FROM ai_insights
                WHERE project_id = ?
                  AND insight_type = 'weekly_range_note'
                  AND insight_date = ?
                  AND COALESCE(range_end, '') = ?
                """,
                (project_id, context["from_date"], context["to_date"]),
            )
            connection.execute(
                """
                INSERT INTO ai_insights (
                    project_id, insight_date, range_end, insight_type, cluster_name,
                    keyword, author, is_pinned, content_vi, generated_at
                )
                VALUES (?, ?, ?, 'weekly_range_note', NULL, NULL, ?, 1, ?, ?)
                """,
                (
                    project_id,
                    context["from_date"],
                    context["to_date"],
                    resolved_author,
                    note,
                    generated_at,
                ),
            )
        return self._weekly_note_payload(
            context=context,
            content=note,
            generated_at=generated_at,
            is_pinned=True,
            author=resolved_author,
            source="saved",
        )

    def _history_position(self, history: list[dict[str, Any]], rank_date: str) -> float | None:
        for item in history:
            if item["rank_date"] == rank_date:
                return item["position"]
        return None

    def _history_previous_position(self, history: list[dict[str, Any]], rank_date: str) -> float | None:
        previous = None
        for item in history:
            if item["rank_date"] == rank_date:
                return previous
            previous = item["position"]
        return None

    def _keyword_tags(self, history: list[dict[str, Any]], kpi_target: int, current_date: str) -> list[str]:
        upto_current = [item for item in history if item["rank_date"] <= current_date]
        if not upto_current:
            return []
        tags: list[str] = []
        latest = upto_current[-1]
        previous = upto_current[-2] if len(upto_current) >= 2 else None
        recent_deltas = [item["delta_from_prev"] for item in upto_current[-3:] if item["delta_from_prev"] is not None]
        avg_delta = safe_mean([float(value) for value in recent_deltas]) if recent_deltas else None
        if avg_delta is not None:
            if avg_delta < -3:
                tags.append("rising")
            elif abs(avg_delta) <= 1:
                tags.append("stable")
            elif avg_delta > 3:
                tags.append("declining")
        if latest["position"] is not None and latest["position"] >= 101:
            tags.append("lost")
        if previous and latest["position"] is not None and previous["position"] is not None:
            if latest["position"] < previous["position"]:
                tags.append("up")
            elif latest["position"] > previous["position"]:
                tags.append("down")
            for threshold in (10, 5, 3):
                if latest["position"] <= threshold < previous["position"]:
                    tags.append(f"milestone_top_{threshold}")
        if latest["position"] is not None and latest["position"] <= kpi_target:
            tags.append("kpi_met")
        return sorted(set(tags))

    def _build_group_metrics(
        self,
        keywords: list[dict[str, Any]],
        current_date: str,
        previous_date: str | None,
    ) -> dict[str, dict[str, Any]]:
        metrics: dict[str, dict[str, Any]] = {}
        for keyword in keywords:
            group_name = keyword.get("group_name") or "Chưa phân nhóm"
            current_rank = self._history_position(keyword["history"], current_date)
            if current_rank is None:
                continue
            previous_rank = self._history_position(keyword["history"], previous_date) if previous_date else None
            entry = metrics.setdefault(
                group_name,
                {
                    "name": group_name,
                    "keyword_count": 0,
                    "achieved": 0,
                    "current_positions": [],
                    "previous_positions": [],
                    "kpi_targets": [],
                },
            )
            entry["keyword_count"] += 1
            entry["current_positions"].append(current_rank)
            entry["kpi_targets"].append(int(keyword.get("kpi_target") or 10))
            if current_rank <= int(keyword.get("kpi_target") or 10):
                entry["achieved"] += 1
            if previous_rank is not None:
                entry["previous_positions"].append(previous_rank)
        for group_name, entry in metrics.items():
            counter = Counter(entry["kpi_targets"])
            kpi_target = int(counter.most_common(1)[0][0]) if counter else 10
            entry["kpi_target"] = kpi_target
            entry["avg_rank"] = safe_mean(entry["current_positions"])
            entry["avg_prev_rank"] = safe_mean(entry["previous_positions"])
            if entry["avg_rank"] is not None and entry["avg_prev_rank"] is not None:
                entry["avg_delta"] = round(entry["avg_rank"] - entry["avg_prev_rank"], 2)
            else:
                entry["avg_delta"] = None
            entry["percent"] = round((entry["achieved"] / entry["keyword_count"]) * 100, 1) if entry["keyword_count"] else 0
            entry["status"] = "đạt" if entry["achieved"] == entry["keyword_count"] else "chưa đạt"
        return metrics

    def _date_note_context(self, project_id: int, insight_date: str) -> dict[str, Any]:
        dates = self.get_project_dates(project_id)
        if not dates:
            raise ValueError("Project chưa có dữ liệu để tạo nhận xét.")
        if insight_date not in dates:
            raise ValueError("Ngày nhận xét không nằm trong dataset hiện tại.")
        date_index = dates.index(insight_date)
        previous_date = dates[date_index - 1] if date_index > 0 else None
        keywords = self._load_keywords_with_history(project_id)
        group_metrics = self._build_group_metrics(keywords, insight_date, previous_date)
        group_rows = [
            {
                "name": name,
                "keyword_count": metric["keyword_count"],
                "achieved": metric["achieved"],
                "kpi_target": metric["kpi_target"],
                "avg_rank_current": metric["avg_rank"],
                "avg_rank_previous": metric["avg_prev_rank"],
                "avg_delta": metric["avg_delta"],
                "percent": metric["percent"],
            }
            for name, metric in group_metrics.items()
        ]
        movers_up: list[dict[str, Any]] = []
        movers_down: list[dict[str, Any]] = []
        kpi_hits: list[str] = []
        rank_distribution = {"Top 1-3": 0, "Top 4-5": 0, "Top 6-10": 0, "Top 11-20": 0, "Top 21+": 0}
        for keyword in keywords:
            current_rank = self._history_position(keyword["history"], insight_date)
            previous_rank = self._history_position(keyword["history"], previous_date) if previous_date else None
            if current_rank is None:
                continue
            if current_rank <= 3:
                rank_distribution["Top 1-3"] += 1
            elif current_rank <= 5:
                rank_distribution["Top 4-5"] += 1
            elif current_rank <= 10:
                rank_distribution["Top 6-10"] += 1
            elif current_rank <= 20:
                rank_distribution["Top 11-20"] += 1
            else:
                rank_distribution["Top 21+"] += 1
            if previous_rank is None:
                continue
            delta = round(float(previous_rank - current_rank), 2)
            mover_payload = {
                "keyword": keyword["keyword"],
                "group_name": keyword.get("group_name") or "Chưa phân nhóm",
                "delta": delta,
                "current_rank": current_rank,
                "previous_rank": previous_rank,
            }
            if delta >= 1:
                movers_up.append(mover_payload)
            elif delta <= -1:
                movers_down.append(mover_payload)
            kpi_target = int(keyword.get("kpi_target") or 10)
            if current_rank <= kpi_target < previous_rank:
                kpi_hits.append(keyword["keyword"])
        movers_up.sort(key=lambda item: (item["delta"], -float(item["current_rank"])), reverse=True)
        movers_down.sort(key=lambda item: item["delta"])
        events = [event for event in self._load_events(project_id) if event["event_date"] == insight_date][:5]
        return {
            "insight_date": insight_date,
            "compare_date": previous_date,
            "group_rows": group_rows,
            "movers_up": movers_up[:5],
            "movers_down": movers_down[:5],
            "kpi_hits": kpi_hits[:10],
            "events": events,
            "rank_distribution": rank_distribution,
        }

    def _fallback_date_note(self, context: dict[str, Any], seo_input: str = "") -> str:
        group_rows = context["group_rows"]
        if not group_rows:
            return "Tổng quan: Chưa đủ dữ liệu để tạo nhận xét cho ngày này."
        strongest = min(
            group_rows,
            key=lambda item: item["avg_delta"] if item["avg_delta"] is not None else 999,
        )
        weakest = max(
            group_rows,
            key=lambda item: item["avg_delta"] if item["avg_delta"] is not None else -999,
        )
        brightest = context["movers_up"][0]["keyword"] if context["movers_up"] else strongest["name"]
        risky = context["movers_down"][0]["keyword"] if context["movers_down"] else weakest["name"]
        kpi_text = ", ".join(context["kpi_hits"][:3]) if context["kpi_hits"] else "chưa có từ khóa mới vượt KPI"
        event_text = context["events"][0]["title"] if context["events"] else "chưa có cảnh báo bất thường"
        extra = f" Ghi chú thêm từ SEO team: {seo_input.strip()}." if seo_input.strip() else ""
        return (
            f"Tổng quan: Ngày {format_date_label(context['insight_date'])}, {strongest['name']} là nhóm giữ nhịp tích cực hơn mặt bằng chung, còn {weakest['name']} cần theo dõi thêm.\n"
            f"Điểm sáng: {brightest} là tín hiệu tốt nhất ở mốc này với mức cải thiện rõ rệt hơn kỳ so sánh.\n"
            f"Điểm cần chú ý: {risky} hoặc nhóm {weakest['name']} đang kéo hiệu suất đi xuống; đồng thời {event_text.lower()}.\n"
            f"Nhận định: Dữ liệu hiện cho thấy {kpi_text} và xu hướng đang phân hoá khá rõ theo từng bộ từ khóa.{extra}"
        )

    def _extract_candidate_ngram_pairs(self, text: str) -> list[tuple[str, str]]:
        display_tokens = _display_tokens(text)
        pairs: list[tuple[str, str]] = []
        seen_signatures: set[str] = set()
        for size in (1, 2, 3):
            for index in range(len(display_tokens) - size + 1):
                raw_tokens = display_tokens[index : index + size]
                signature = " ".join(_stem_token(token) for token in raw_tokens)
                if signature in seen_signatures:
                    continue
                pairs.append((signature, " ".join(raw_tokens)))
                seen_signatures.add(signature)
        return pairs

    def _extract_candidate_ngrams(self, text: str) -> set[str]:
        return {signature for signature, _ in self._extract_candidate_ngram_pairs(text)}

    def _match_semantic_family_tags(
        self,
        keyword_ngrams: set[str],
        context_ngrams: set[str],
        family: str,
    ) -> list[str]:
        matched: list[str] = []
        for tag in TAG_PRIORITY[family]:
            signatures = COMPILED_TAG_LIBRARY[family][tag]["signatures"]
            if signatures & keyword_ngrams:
                matched.append(tag)
        if not matched:
            for tag in TAG_PRIORITY[family]:
                signatures = COMPILED_TAG_LIBRARY[family][tag]["signatures"]
                if signatures & context_ngrams:
                    matched.append(tag)
        matched_set = set(matched)
        cleaned = [
            tag
            for tag in matched
            if not any(suppressor in matched_set for suppressor in COMPILED_TAG_LIBRARY[family][tag].get("suppressed_by", []))
        ]
        return cleaned

    def _should_include_topic_signature(self, signature: str) -> bool:
        if not signature or signature in RESERVED_TAG_SIGNATURES:
            return False
        tokens = signature.split()
        if not tokens or all(token.isdigit() for token in tokens):
            return False
        if len(tokens) == 1 and len(tokens[0]) < 4:
            return False
        return True

    def _build_dataset_topic_tags(self, keywords: list[dict[str, Any]]) -> dict[int, list[str]]:
        if len(keywords) < 3:
            return {}
        keyword_signatures: dict[int, set[str]] = {}
        signature_display: dict[str, str] = {}
        counter: Counter[str] = Counter()
        for keyword in keywords:
            pairs = self._extract_candidate_ngram_pairs(keyword.get("keyword") or "")
            signatures = {
                signature
                for signature, _ in pairs
                if self._should_include_topic_signature(signature)
            }
            for signature, display in pairs:
                if signature in signatures and signature not in signature_display:
                    signature_display[signature] = display
            keyword_signatures[int(keyword["id"])] = signatures
            counter.update(signatures)
        minimum_hits = max(2, round(len(keywords) * 0.18))
        selected_signatures = [
            signature
            for signature, hits in counter.items()
            if hits >= minimum_hits
        ]
        selected_signatures.sort(
            key=lambda signature: (counter[signature], len(signature.split()), len(signature)),
            reverse=True,
        )
        selected_signatures = selected_signatures[:12]
        topic_tags: dict[int, list[str]] = defaultdict(list)
        for signature in selected_signatures:
            display_value = signature_display.get(signature, signature).replace(" ", "_")
            tag = f"topic:{display_value}"
            for keyword_id, signatures in keyword_signatures.items():
                if signature in signatures:
                    topic_tags[keyword_id].append(tag)
        return {keyword_id: sorted(tags) for keyword_id, tags in topic_tags.items()}

    def _keyword_tag_profile(
        self,
        keyword: dict[str, Any],
        dataset_topic_tags: dict[int, list[str]],
    ) -> dict[str, Any]:
        keyword_ngrams = self._extract_candidate_ngrams(keyword.get("keyword") or "")
        context_ngrams = self._extract_candidate_ngrams(
            " ".join(
                [
                    keyword.get("group_name") or "",
                    keyword.get("cluster_name") or "",
                ]
            )
        )
        family_tags = {
            family: self._match_semantic_family_tags(keyword_ngrams, context_ngrams, family)
            for family in TAG_PRIORITY
        }
        topic_tags = dataset_topic_tags.get(int(keyword["id"]), [])
        all_tags = sorted(
            set(topic_tags)
            | set(tag for tags in family_tags.values() for tag in tags)
        )
        return {
            "keyword_ngrams": sorted(keyword_ngrams),
            "context_ngrams": sorted(context_ngrams),
            "family_tags": family_tags,
            "tags": all_tags,
        }

    def _normalize_sub_cluster_mode(self, requested_mode: str | None) -> str:
        mode = (requested_mode or "auto").strip().lower()
        mode = LEGACY_SUB_CLUSTER_MODE_MAP.get(mode, mode)
        return mode if mode in SUB_CLUSTER_MODE_META else "auto"

    def _family_distribution(self, tag_profiles: dict[int, dict[str, Any]], family: str) -> dict[str, Any]:
        total_keywords = len(tag_profiles)
        primary_tags = [
            profile["family_tags"][family][0]
            for profile in tag_profiles.values()
            if profile["family_tags"][family]
        ]
        counter = Counter(primary_tags)
        covered = sum(counter.values())
        coverage = covered / total_keywords if total_keywords else 0.0
        bucket_count = len(counter)
        dominant_share = max((count / covered) for count in counter.values()) if covered else 1.0
        entropy = 0.0
        if covered and bucket_count > 1:
            entropy = -sum(
                (count / covered) * log(count / covered)
                for count in counter.values()
            ) / log(bucket_count)
        segmentation_score = min(bucket_count, 4) / 4 if bucket_count else 0.0
        balance_score = 0.0 if bucket_count <= 1 else 1 - dominant_share
        score = round((coverage * 0.6) + (segmentation_score * 0.25) + (balance_score * 0.15), 4)
        return {
            "coverage": coverage,
            "bucket_count": bucket_count,
            "dominant_share": dominant_share,
            "entropy": entropy,
            "score": score,
            "counter": counter,
        }

    def _resolve_sub_cluster_mode(
        self,
        requested_mode: str,
        tag_profiles: dict[int, dict[str, Any]],
        custom_config: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        normalized_mode = self._normalize_sub_cluster_mode(requested_mode)
        family_distribution = {
            family: self._family_distribution(tag_profiles, family)
            for family in TAG_FAMILY_LABELS
        }
        default_secondary = {
            "platform": "product",
            "product": "platform",
            "intent": "product",
        }
        if normalized_mode == "custom":
            primary_family = (custom_config or {}).get("primary_tag_prefix") or "platform"
            secondary_family = (custom_config or {}).get("secondary_tag_prefix") or None
            primary_family = primary_family if primary_family in TAG_FAMILY_LABELS else "platform"
            secondary_family = secondary_family if secondary_family in TAG_FAMILY_LABELS else None
            note = (
                f"Custom mode đang gom theo {TAG_FAMILY_LABELS[primary_family]} trước"
                + (f", sau đó nối thêm {TAG_FAMILY_LABELS[secondary_family]}." if secondary_family else ".")
            )
            return {
                "requested_mode": normalized_mode,
                "resolved_mode": normalized_mode,
                "primary_family": primary_family,
                "secondary_family": secondary_family,
                "note": note,
                "family_distribution": family_distribution,
            }
        if normalized_mode == "auto":
            best_family, best_stats = max(
                family_distribution.items(),
                key=lambda item: (item[1]["score"], item[1]["coverage"], item[1]["bucket_count"]),
            )
            coverage_pct = int(round(best_stats["coverage"] * 100))
            note = (
                f"Auto mode chọn gom theo {TAG_FAMILY_LABELS[best_family]} vì {coverage_pct}% keyword "
                f"có tag {TAG_FAMILY_LABELS[best_family]} rõ ràng"
            )
            if best_stats["bucket_count"] > 1:
                note += f", đồng thời tách được {best_stats['bucket_count']} cụm con dễ đọc."
            else:
                note += "."
            return {
                "requested_mode": normalized_mode,
                "resolved_mode": f"{best_family}_first",
                "primary_family": best_family,
                "secondary_family": default_secondary.get(best_family),
                "note": note,
                "family_distribution": family_distribution,
            }
        primary_family = SUB_CLUSTER_MODE_META[normalized_mode]["primary_family"]
        return {
            "requested_mode": normalized_mode,
            "resolved_mode": normalized_mode,
            "primary_family": primary_family,
            "secondary_family": default_secondary.get(primary_family),
            "note": SUB_CLUSTER_MODE_META[normalized_mode]["description"],
            "family_distribution": family_distribution,
        }

    def _topic_distribution(self, tag_profiles: dict[int, dict[str, Any]]) -> dict[str, Any]:
        total_keywords = len(tag_profiles)
        per_keyword_topics: list[str] = []
        topic_counter: Counter[str] = Counter()
        for profile in tag_profiles.values():
            topics = [tag for tag in profile["tags"] if tag.startswith("topic:")]
            if topics:
                per_keyword_topics.append(topics[0])
                topic_counter.update(topics)
        covered = len(per_keyword_topics)
        coverage = covered / total_keywords if total_keywords else 0.0
        bucket_counter = Counter(per_keyword_topics)
        bucket_count = len(bucket_counter)
        dominant_share = max((count / covered) for count in bucket_counter.values()) if covered else 1.0
        segmentation_score = min(bucket_count, 4) / 4 if bucket_count else 0.0
        balance_score = 0.0 if bucket_count <= 1 else 1 - dominant_share
        score = round((coverage * 0.55) + (segmentation_score * 0.3) + (balance_score * 0.15), 4)
        return {
            "coverage": coverage,
            "bucket_count": bucket_count,
            "dominant_share": dominant_share,
            "score": score,
            "counter": topic_counter,
            "primary_counter": bucket_counter,
        }

    def _scenario_label_for_family(self, family: str, is_default: bool = False) -> str:
        if is_default:
            return "Góc nhìn mặc định"
        mapping = {
            "platform": "Theo brand / nền tảng",
            "product": "Theo dạng giải pháp",
            "intent": "Theo nhu cầu / use case",
        }
        return mapping.get(family, "Theo nhóm chính")

    def _scenario_description_for_family(self, family: str, secondary_family: str | None, is_default: bool = False) -> str:
        if is_default:
            return "Cách gom nhóm rõ nhất và dễ đọc nhất cho bộ dữ liệu hiện tại."
        mapping = {
            "platform": "Nhóm keyword theo nền tảng hoặc brand chính; vẫn giữ ngữ cảnh giải pháp trong drill-down.",
            "product": "Nhóm keyword theo loại giải pháp / dạng sản phẩm, phù hợp cho sales nhìn nhanh các dòng chủ lực.",
            "intent": "Nhóm keyword theo nhu cầu hoặc use case để nhìn thấy insight theo mục đích tìm kiếm.",
        }
        text = mapping.get(family, "Nhóm keyword theo chiều dữ liệu mạnh nhất ở dataset này.")
        if secondary_family and secondary_family != family:
            text += f" Khi drill-down vẫn giữ tag {TAG_FAMILY_LABELS[secondary_family]} để đọc ngữ cảnh."
        return text

    def _build_view_scenarios(
        self,
        selected_group: str | None,
        tag_profiles: dict[int, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        family_distribution = {
            family: self._family_distribution(tag_profiles, family)
            for family in TAG_FAMILY_LABELS
        }
        topic_distribution = self._topic_distribution(tag_profiles)
        default_secondary = {
            "platform": "product",
            "product": "platform",
            "intent": "product",
        }
        candidates: list[dict[str, Any]] = []
        for family, stats in family_distribution.items():
            if stats["coverage"] < 0.35 or stats["bucket_count"] < 2:
                continue
            candidates.append(
                {
                    "kind": "family",
                    "family": family,
                    "score": stats["score"],
                    "coverage": stats["coverage"],
                    "bucket_count": stats["bucket_count"],
                    "secondary_family": default_secondary.get(family),
                }
            )
        if topic_distribution["coverage"] >= 0.35 and topic_distribution["bucket_count"] >= 2:
            candidates.append(
                {
                    "kind": "topic",
                    "family": "topic",
                    "score": topic_distribution["score"],
                    "coverage": topic_distribution["coverage"],
                    "bucket_count": topic_distribution["bucket_count"],
                    "secondary_family": None,
                }
            )
        if not candidates:
            candidates.append(
                {
                    "kind": "family",
                    "family": "product",
                    "score": 0.1,
                    "coverage": 0.0,
                    "bucket_count": 1,
                    "secondary_family": "platform",
                }
            )
        candidates.sort(key=lambda item: (item["score"], item["coverage"], item["bucket_count"]), reverse=True)
        scenarios: list[dict[str, Any]] = []
        for index, candidate in enumerate(candidates[:3], start=1):
            is_default = index == 1
            scenario_id = f"scenario_{index}"
            if candidate["kind"] == "topic":
                top_topics = [
                    self._tag_label(tag)
                    for tag, _ in topic_distribution["primary_counter"].most_common(3)
                ]
                topic_examples = " / ".join(top_topics[:2]) if top_topics else "chủ đề"
                scenarios.append(
                    {
                        "scenario_id": scenario_id,
                        "scenario_label": "Theo chủ đề nổi bật" if not is_default else "Góc nhìn mặc định",
                        "scenario_description": (
                            "Nhóm keyword theo các n-gram lặp lại mạnh nhất trong dataset"
                            + (f", nổi bật như {topic_examples}." if topic_examples else ".")
                        ),
                        "visible_tag_types": [f"type:{scenario_id}"],
                        "filter_prefixes": ["topic:"],
                        "strategy": "topic",
                        "primary_family": "topic",
                        "secondary_family": None,
                    }
                )
            else:
                family = candidate["family"]
                secondary_family = candidate["secondary_family"]
                family_examples = [
                    self._tag_label(tag)
                    for tag, _ in family_distribution[family]["counter"].most_common(2)
                ]
                description = self._scenario_description_for_family(
                    family,
                    secondary_family,
                    is_default=is_default,
                )
                if family_examples:
                    description += f" Gợi ý nổi bật: {', '.join(family_examples)}."
                scenarios.append(
                    {
                        "scenario_id": scenario_id,
                        "scenario_label": self._scenario_label_for_family(family, is_default=is_default),
                        "scenario_description": description,
                        "visible_tag_types": [f"type:{scenario_id}"],
                        "filter_prefixes": [f"{family}:"],
                        "strategy": "family",
                        "primary_family": family,
                        "secondary_family": secondary_family,
                    }
                )
        return scenarios

    def _resolve_active_scenario(
        self,
        scenarios: list[dict[str, Any]],
        requested_scenario_id: str | None,
        legacy_mode: str | None = None,
    ) -> dict[str, Any] | None:
        if not scenarios:
            return None
        if requested_scenario_id:
            explicit = next(
                (scenario for scenario in scenarios if scenario["scenario_id"] == requested_scenario_id),
                None,
            )
            if explicit:
                return explicit
        normalized_mode = self._normalize_sub_cluster_mode(legacy_mode)
        family_by_mode = {
            "platform_first": "platform",
            "product_first": "product",
            "intent_first": "intent",
        }
        preferred_family = family_by_mode.get(normalized_mode)
        if preferred_family:
            matched = next(
                (
                    scenario
                    for scenario in scenarios
                    if scenario.get("primary_family") == preferred_family
                ),
                None,
            )
            if matched:
                return matched
        return scenarios[0]

    def _tag_label(self, tag: str) -> str:
        if tag in TAG_LABELS:
            return TAG_LABELS[tag]
        if tag.startswith("topic:"):
            return tag.removeprefix("topic:").replace("_", " ").title()
        if ":" in tag:
            return tag.split(":", 1)[1].replace("_", " ").title()
        return tag.replace("_", " ").title()

    def _subcluster_descriptor(
        self,
        keyword: dict[str, Any],
        tag_profile: dict[str, Any],
        scenario: dict[str, Any],
    ) -> tuple[str, str, list[str]]:
        primary_family = scenario["primary_family"]
        secondary_family = scenario.get("secondary_family")
        if scenario["strategy"] == "topic":
            topic_tag = next((tag for tag in tag_profile["tags"] if tag.startswith("topic:")), None)
            if topic_tag:
                return topic_tag.replace(":", "__"), self._tag_label(topic_tag), tag_profile["tags"]
            return "topic__other", "Chủ đề khác", tag_profile["tags"]

        primary_tags = tag_profile["family_tags"].get(primary_family, [])
        secondary_tags = tag_profile["family_tags"].get(secondary_family, []) if secondary_family else []
        primary_tag = primary_tags[0] if primary_tags else None
        secondary_tag = secondary_tags[0] if secondary_tags else None

        if primary_tag:
            cluster_id = primary_tag.replace(":", "__")
            cluster_label = self._tag_label(primary_tag)
            if secondary_tag and scenario.get("secondary_family"):
                if primary_family == "platform":
                    cluster_label = f"{cluster_label}"
                elif primary_family == "product" and secondary_family == "platform" and primary_tag.startswith("product:"):
                    cluster_label = f"{cluster_label}"
            return cluster_id, cluster_label, tag_profile["tags"]

        fallback_by_family = {
            "platform": ("platform__other", "Khác / chưa rõ"),
            "product": ("product__other", "Khác / chưa rõ"),
            "intent": ("intent__generic", "Nhu cầu chung"),
        }
        if primary_family in fallback_by_family:
            fallback_id, fallback_label = fallback_by_family[primary_family]
            return fallback_id, fallback_label, tag_profile["tags"]

        fallback_name = (keyword.get("cluster_name") or keyword.get("group_name") or "Khác / chưa rõ").strip()
        fallback_slug = normalize_label(fallback_name).replace(" ", "_") or "other"
        return f"fallback__{fallback_slug}", fallback_name, tag_profile["tags"]

    def _filter_tags_for_scenario(self, tags: list[str], filter_prefixes: list[str]) -> list[str]:
        if not filter_prefixes:
            return tags
        filtered = [
            tag
            for tag in tags
            if any(tag.startswith(prefix) for prefix in filter_prefixes)
        ]
        return filtered or tags[:1]

    def _cluster_keyword_row(
        self,
        keyword: dict[str, Any],
        tag_profile: dict[str, Any],
        current_date: str,
        baseline_date: str | None,
        mode_meta: dict[str, Any],
    ) -> dict[str, Any] | None:
        current_rank = self._history_position(keyword["history"], current_date)
        if current_rank is None:
            return None
        previous_rank = self._history_position(keyword["history"], baseline_date) if baseline_date else None
        rank_delta = None if previous_rank is None else round(float(previous_rank - current_rank), 2)
        trend_status = "stable"
        if rank_delta is not None:
            if rank_delta >= 2:
                trend_status = "rising"
            elif rank_delta <= -2:
                trend_status = "declining"
        cluster_id, cluster_name, tags = self._subcluster_descriptor(keyword, tag_profile, mode_meta)
        return {
            "keyword_id": keyword["id"],
            "keyword": keyword["keyword"],
            "cluster_id": cluster_id,
            "cluster_name": cluster_name,
            "group_name": keyword.get("group_name") or "Chưa phân nhóm",
            "cluster_origin": keyword.get("cluster_name") or keyword.get("group_name") or "Chưa phân nhóm",
            "tags": tags,
            "volume": keyword.get("search_volume") or 0,
            "current_rank": current_rank,
            "previous_rank": previous_rank,
            "rank_delta": rank_delta,
            "trend_status": trend_status,
            "history": keyword["history"],
            "kpi_target": int(keyword.get("kpi_target") or 10),
        }

    def _cluster_priority_label(
        self,
        *,
        avg_rank_current: float | None,
        trend_status: str,
        health_score: int,
        avg_rank_previous: float | None,
    ) -> str:
        if avg_rank_current is not None and avg_rank_current < 5 and trend_status == "rising" and health_score >= 70:
            return "Rising & strong"
        if trend_status == "rising" and (avg_rank_current is None or avg_rank_current >= 5):
            return "Rising & opportunity"
        if trend_status == "declining":
            return "Declining"
        if (
            avg_rank_current is not None
            and avg_rank_previous is not None
            and avg_rank_current > 10
            and avg_rank_previous <= 10
        ):
            return "Declining"
        return "Stable"

    def _cluster_insight_note(self, cluster: dict[str, Any]) -> str:
        improved = sum(1 for item in cluster["keywords"] if (item["rank_delta"] or 0) >= 2)
        declined = sum(1 for item in cluster["keywords"] if (item["rank_delta"] or 0) <= -2)
        if cluster["priority_label"] == "Rising & strong":
            return (
                f"Cụm '{cluster['cluster_name']}' đang giữ đà tốt: {improved} keyword tiếp tục cải thiện "
                "và đã đứng ở vùng dễ chuyển đổi. Nên giữ nhịp tối ưu và mở rộng thêm landing page liên quan."
            )
        if cluster["priority_label"] == "Rising & opportunity":
            return (
                f"Cụm '{cluster['cluster_name']}' đang đi lên nhưng vẫn còn dư địa lớn. "
                f"Có {improved} keyword tăng rõ, phù hợp để đẩy thêm content, internal link và backlink."
            )
        if cluster["priority_label"] == "Declining":
            return (
                f"Cụm '{cluster['cluster_name']}' đang cần xử lý: {declined} keyword giảm đáng kể "
                "và kéo tụt hiệu suất chung. Nên kiểm tra intent, đối thủ và thay đổi kỹ thuật gần đây."
            )
        return (
            f"Cụm '{cluster['cluster_name']}' hiện khá ổn định, chưa có biến động đủ lớn để đổi ưu tiên. "
            "Phù hợp để tiếp tục theo dõi và tối ưu định kỳ."
        )

    def _build_cluster_view(
        self,
        project_id: int,
        *,
        current_date: str,
        baseline_date: str,
        selected_group: str | None,
        status_filter: str,
        tag_filter: str,
        sort_by: str,
        active_scenario_id: str | None,
        legacy_mode: str | None = None,
        custom_config: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        keywords = self._load_keywords_with_history(project_id)
        dates = self.get_project_dates(project_id)
        groups = sorted({(keyword.get("group_name") or "Chưa phân nhóm") for keyword in keywords})
        selected_group = selected_group or (groups[0] if groups else None)
        group_keywords = [
            keyword
            for keyword in keywords
            if not selected_group or (keyword.get("group_name") or "Chưa phân nhóm") == selected_group
        ]
        dataset_topic_tags = self._build_dataset_topic_tags(group_keywords)
        tag_profiles = {
            int(keyword["id"]): self._keyword_tag_profile(keyword, dataset_topic_tags)
            for keyword in group_keywords
        }
        scenarios = self._build_view_scenarios(selected_group, tag_profiles)
        active_scenario = self._resolve_active_scenario(
            scenarios,
            active_scenario_id,
            legacy_mode=legacy_mode,
        )
        if active_scenario is None:
            return {
                "project_id": str(project_id),
                "project_name": self.get_project(project_id)["name"],
                "main_cluster": selected_group,
                "active_scenario_id": None,
                "scenarios": [],
                "insight_note_global": "Chưa đủ dữ liệu để dựng sub-cluster cho bộ lọc hiện tại.",
                "dates": dates,
                "current_date": current_date,
                "baseline_date": baseline_date,
                "selected_main_cluster": selected_group,
                "main_clusters": groups,
                "available_tags": [],
                "cluster_overview": {
                    "main_cluster": selected_group,
                    "total_keywords": 0,
                    "total_volume": 0,
                    "generated_at": now_iso(),
                },
                "cluster_list": [],
                "trend_panel": None,
                "drilldown_tables": [],
                **self._latest_share_links(project_id),
                "client_view_password": None,
            }
        relevant_rows = []
        for keyword in group_keywords:
            row = self._cluster_keyword_row(
                keyword,
                tag_profiles[int(keyword["id"])],
                current_date,
                baseline_date,
                active_scenario,
            )
            if row is None:
                continue
            visible_raw_tags = self._filter_tags_for_scenario(
                row["tags"],
                active_scenario.get("filter_prefixes", []),
            )
            row["visible_tags"] = [self._tag_label(tag) for tag in visible_raw_tags]
            relevant_rows.append(row)

        cluster_map: dict[str, dict[str, Any]] = {}
        for row in relevant_rows:
            cluster = cluster_map.setdefault(
                row["cluster_id"],
                {
                    "cluster_id": row["cluster_id"],
                    "cluster_name": row["cluster_name"],
                    "tags": sorted(set(row["visible_tags"])),
                    "keywords": [],
                    "current_ranks": [],
                    "previous_ranks": [],
                    "volumes": [],
                },
            )
            cluster["keywords"].append(row)
            cluster["current_ranks"].append(row["current_rank"])
            if row["previous_rank"] is not None:
                cluster["previous_ranks"].append(row["previous_rank"])
            if row["volume"] is not None:
                cluster["volumes"].append(float(row["volume"]))
            cluster["tags"] = sorted(set(cluster["tags"]) | set(row["visible_tags"]))

        max_total_volume = max((sum(cluster["volumes"]) for cluster in cluster_map.values()), default=1.0)
        recent_dates = dates[-30:] if len(dates) > 30 else dates
        cluster_list: list[dict[str, Any]] = []
        drilldown_tables: list[dict[str, Any]] = []

        for cluster in cluster_map.values():
            avg_rank_current = safe_mean(cluster["current_ranks"])
            avg_rank_previous = safe_mean(cluster["previous_ranks"])
            rank_delta = (
                round(float(avg_rank_previous - avg_rank_current), 2)
                if avg_rank_current is not None and avg_rank_previous is not None
                else 0.0
            )
            if rank_delta >= 2:
                trend_status = "rising"
            elif rank_delta <= -2:
                trend_status = "declining"
            else:
                trend_status = "stable"

            total_volume = int(sum(cluster["volumes"]))
            avg_volume = round(total_volume / len(cluster["keywords"]), 1) if cluster["keywords"] else 0
            rank_component = 0 if avg_rank_current is None else clamp((101 - avg_rank_current) / 100 * 100, 0, 100)
            trend_component = clamp(50 + rank_delta * 12, 0, 100)
            volume_component = clamp((total_volume / max_total_volume) * 100 if max_total_volume else 0, 0, 100)
            health_score = int(round((rank_component * 0.5) + (trend_component * 0.25) + (volume_component * 0.25)))
            priority_label = self._cluster_priority_label(
                avg_rank_current=avg_rank_current,
                trend_status=trend_status,
                health_score=health_score,
                avg_rank_previous=avg_rank_previous,
            )

            sorted_keywords = sorted(
                cluster["keywords"],
                key=lambda item: (
                    -float(item["volume"] or 0),
                    -abs(float(item["rank_delta"] or 0)),
                    item["current_rank"],
                ),
            )
            top_keywords = [
                {
                    "keyword": item["keyword"],
                    "tags": item["visible_tags"],
                    "volume": item["volume"],
                    "current_rank": item["current_rank"],
                    "previous_rank": item["previous_rank"],
                    "rank_delta": item["rank_delta"] or 0,
                    "trend_status": item["trend_status"],
                }
                for item in sorted_keywords[:5]
            ]

            points = []
            for rank_date in recent_dates:
                values = [
                    self._history_position(item["history"], rank_date)
                    for item in cluster["keywords"]
                ]
                valid_values = [float(value) for value in values if value is not None]
                if not valid_values:
                    continue
                points.append({"date": rank_date, "value": round(sum(valid_values) / len(valid_values), 2)})
            previous_window = [point["value"] for point in points[-14:-7]]
            current_window = [point["value"] for point in points[-7:]]
            previous_avg = safe_mean(previous_window)
            current_avg = safe_mean(current_window)
            delta_vs_previous_period = 0
            if previous_avg and current_avg:
                delta_vs_previous_period = round(((previous_avg - current_avg) / previous_avg) * 100, 1)

            cluster_row = {
                "cluster_id": cluster["cluster_id"],
                "cluster_name": cluster["cluster_name"],
                "tags": cluster["tags"],
                "keyword_count": len(cluster["keywords"]),
                "total_volume": total_volume,
                "avg_volume": avg_volume,
                "avg_rank_current": avg_rank_current or 0,
                "avg_rank_previous": avg_rank_previous or 0,
                "rank_delta": rank_delta,
                "trend_status": trend_status,
                "health_score": health_score,
                "priority_label": priority_label,
                "top_keywords": top_keywords,
                "sparkline": {
                    "metric": "avg_rank",
                    "time_range": "last_30_days",
                    "points": points,
                    "delta_vs_previous_period": delta_vs_previous_period,
                },
                "insight_note": "",
                "keywords": sorted(
                    [
                        {
                            "keyword": item["keyword"],
                            "tags": item["visible_tags"],
                            "volume": item["volume"],
                            "current_rank": item["current_rank"],
                            "previous_rank": item["previous_rank"],
                            "rank_delta": item["rank_delta"] or 0,
                            "trend_status": item["trend_status"],
                            "clicks": 0,
                            "impressions": 0,
                        }
                        for item in cluster["keywords"]
                    ],
                    key=lambda item: (item["current_rank"], -float(item["volume"] or 0), item["keyword"].lower()),
                ),
            }
            cluster_row["insight_note"] = self._cluster_insight_note(cluster_row)
            cluster_list.append(cluster_row)
            drilldown_tables.append(
                {
                    "cluster_id": cluster_row["cluster_id"],
                    "keywords": cluster_row["keywords"],
                }
            )

        available_tags = sorted({tag for cluster in cluster_list for tag in cluster["tags"]})
        if tag_filter != "all":
            cluster_list = [cluster for cluster in cluster_list if tag_filter in cluster["tags"]]
            drilldown_tables = [table for table in drilldown_tables if any(cluster["cluster_id"] == table["cluster_id"] for cluster in cluster_list)]
        if status_filter != "all":
            cluster_list = [cluster for cluster in cluster_list if cluster["trend_status"] == status_filter]
            drilldown_tables = [table for table in drilldown_tables if any(cluster["cluster_id"] == table["cluster_id"] for cluster in cluster_list)]

        sort_field, descending = CLUSTER_SORTS.get(sort_by, CLUSTER_SORTS["health_score"])
        cluster_list.sort(
            key=lambda item: (item.get(sort_field) is None, item.get(sort_field, 0)),
            reverse=descending,
        )
        selected_cluster = cluster_list[0] if cluster_list else None
        trend_panel = {
            "selected_cluster_id": selected_cluster["cluster_id"] if selected_cluster else None,
            "kpis": {
                "total_volume": selected_cluster["total_volume"] if selected_cluster else 0,
                "avg_rank_current": selected_cluster["avg_rank_current"] if selected_cluster else 0,
                "rank_delta": selected_cluster["rank_delta"] if selected_cluster else 0,
                "health_score": selected_cluster["health_score"] if selected_cluster else 0,
                "trend_status": selected_cluster["trend_status"] if selected_cluster else "stable",
            },
            "sparkline": selected_cluster["sparkline"] if selected_cluster else {"metric": "avg_rank", "time_range": "last_30_days", "points": [], "delta_vs_previous_period": 0},
            "top_keywords_table": selected_cluster["top_keywords"] if selected_cluster else [],
            "insight_note": selected_cluster["insight_note"] if selected_cluster else "Chưa có cụm phù hợp với bộ lọc hiện tại.",
        }
        total_volume = sum(item["volume"] for item in relevant_rows)
        share_links = self._latest_share_links(project_id)
        return {
            "project_id": str(project_id),
            "project_name": self.get_project(project_id)["name"],
            "main_cluster": selected_group,
            "active_scenario_id": active_scenario["scenario_id"],
            "scenarios": [
                {
                    "scenario_id": scenario["scenario_id"],
                    "scenario_label": scenario["scenario_label"],
                    "scenario_description": scenario["scenario_description"],
                    "visible_tag_types": scenario["visible_tag_types"],
                }
                for scenario in scenarios
            ],
            "insight_note_global": active_scenario["scenario_description"],
            "dates": dates,
            "current_date": current_date,
            "baseline_date": baseline_date,
            "selected_main_cluster": selected_group,
            "main_clusters": groups,
            "available_tags": available_tags,
            "cluster_overview": {
                "main_cluster": selected_group,
                "total_keywords": len(relevant_rows),
                "total_volume": total_volume,
                "generated_at": now_iso(),
            },
            "cluster_list": cluster_list,
            "trend_panel": trend_panel,
            "drilldown_tables": drilldown_tables,
            **share_links,
            "client_view_password": None,
        }

    def get_overview(self, project_id: int) -> dict[str, Any]:
        project = self.get_project(project_id)
        projects = self.list_projects()
        keywords = self._load_keywords_with_history(project_id)
        dates = self.get_project_dates(project_id)
        latest_insights = self._load_latest_insights(project_id)
        pinned_notes = self._load_pinned_date_notes(project_id)
        events = self._load_events(project_id)
        if not dates:
            return {
                "project": project,
                "projects": projects,
                "dates": [],
                "kpi_chips": [],
                "summary_cards": [],
                "timeline": [],
                "donut": [],
                "distribution": [],
                "avg_trend": [],
                "events": events,
                "latest_insight": latest_insights.get("weekly_summary"),
                "pinned_notes": {},
            }
        saved_weekly_range = (
            project.get("saved_view_state", {}).get("weekly_note_range", {})
            if isinstance(project.get("saved_view_state"), dict)
            else {}
        )
        latest_note = latest_insights.get("weekly_summary")
        try:
            latest_note = self.generate_weekly_range_note(
                project_id,
                from_date=saved_weekly_range.get("from_date") or None,
                to_date=saved_weekly_range.get("to_date") or None,
                force=False,
                allow_ai=False,
            )
        except Exception:
            latest_note = latest_note
        latest_date = dates[-1]
        previous_date = dates[-2] if len(dates) >= 2 else None
        group_metrics = self._build_group_metrics(keywords, latest_date, previous_date)
        group_names = sorted(group_metrics)
        kpi_chips = [
            {
                "name": group_name,
                "keyword_count": data["keyword_count"],
                "kpi_target": data["kpi_target"],
                "achieved": data["achieved"],
                "status": data["status"],
                "label": f"{group_name} · {data['keyword_count']} KW · KPI Top{data['kpi_target']}",
            }
            for group_name, data in group_metrics.items()
        ]
        achieved_latest = sum(data["achieved"] for data in group_metrics.values())
        achieved_prev = 0
        if previous_date:
            prev_metrics = self._build_group_metrics(keywords, previous_date, dates[-3] if len(dates) >= 3 else None)
            achieved_prev = sum(data["achieved"] for data in prev_metrics.values())
        summary_cards = [
            {
                "name": "Tổng keyword",
                "value": len(keywords),
                "kpi_target": sum(data["keyword_count"] for data in group_metrics.values()),
                "achieved": achieved_latest,
                "percent": round((achieved_latest / len(keywords)) * 100, 1) if keywords else 0,
                "trend": achieved_latest - achieved_prev if previous_date else 0,
            }
        ]
        for group_name in group_names[:3]:
            data = group_metrics[group_name]
            summary_cards.append(
                {
                    "name": group_name,
                    "value": data["achieved"],
                    "kpi_target": data["keyword_count"],
                    "percent": data["percent"],
                    "trend": data["avg_delta"],
                    "threshold": data["kpi_target"],
                    "subtitle": f"{data['achieved']}/{data['keyword_count']} đạt",
                }
            )

        timeline = []
        events_by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for event in events:
            events_by_date[event["event_date"]].append(event)
        for rank_date in dates:
            item = {"date": rank_date, "label": format_date_label(rank_date), "groups": {}, "averages": {}, "events": events_by_date.get(rank_date, [])}
            day_metrics = self._build_group_metrics(
                keywords,
                rank_date,
                dates[dates.index(rank_date) - 1] if dates.index(rank_date) > 0 else None,
            )
            for group_name in group_names:
                data = day_metrics.get(group_name)
                item["groups"][group_name] = data["achieved"] if data else 0
                item["averages"][group_name] = data["avg_rank"] if data else None
            timeline.append(item)

        distribution_buckets = {"Top 1-3": 0, "Top 4-5": 0, "Top 6-10": 0, "Top 11-20": 0, "Top 21+": 0}
        for keyword in keywords:
            current_rank = self._history_position(keyword["history"], latest_date)
            if current_rank is None:
                continue
            if current_rank <= 3:
                distribution_buckets["Top 1-3"] += 1
            elif current_rank <= 5:
                distribution_buckets["Top 4-5"] += 1
            elif current_rank <= 10:
                distribution_buckets["Top 6-10"] += 1
            elif current_rank <= 20:
                distribution_buckets["Top 11-20"] += 1
            else:
                distribution_buckets["Top 21+"] += 1

        donut = [
            {"name": group_name, "value": data["percent"], "achieved": data["achieved"], "total": data["keyword_count"]}
            for group_name, data in group_metrics.items()
        ]
        avg_trend = [
            {
                "date": item["date"],
                "label": item["label"],
                **{group_name: item["averages"].get(group_name) for group_name in group_names},
            }
            for item in timeline
        ]
        source_name = project.get("source_name") or project["name"]
        subtitle = (
            f"Nguồn: {source_name} · {len(dates)} ngày · "
            f"{format_date_label(dates[0])} - {format_date_label(dates[-1])} · "
            "Dữ liệu đã xác minh khớp 100% file gốc"
        )
        return {
            "project": project,
            "projects": projects,
            "dates": dates,
            "date_labels": {value: format_date_label(value) for value in dates},
            "subtitle": subtitle,
            "kpi_chips": kpi_chips,
            "summary_cards": summary_cards,
            "latest_insight": latest_note,
            "events": events[:20],
            "timeline": timeline,
            "group_names": group_names,
            "donut": donut,
            "distribution": [{"name": key, "value": value} for key, value in distribution_buckets.items()],
            "avg_trend": avg_trend,
            "pinned_notes": pinned_notes,
        }

    def get_group_view(
        self,
        project_id: int,
        *,
        current_date: str | None = None,
        baseline_date: str | None = None,
        status_filter: str = "all",
        main_cluster: str | None = None,
        tag_filter: str = "all",
        sort_by: str = "health_score",
        active_scenario_id: str | None = None,
        legacy_mode: str | None = None,
        custom_config: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        dates = self.get_project_dates(project_id)
        if not dates:
            return {
                "project_id": str(project_id),
                "project_name": self.get_project(project_id)["name"],
                "main_cluster": None,
                "active_scenario_id": None,
                "scenarios": [],
                "insight_note_global": "Chưa có dữ liệu để sinh sub-cluster.",
                "dates": [],
                "current_date": None,
                "baseline_date": None,
                "selected_main_cluster": None,
                "main_clusters": [],
                "available_tags": [],
                "cluster_overview": None,
                "cluster_list": [],
                "trend_panel": None,
                "drilldown_tables": [],
                **self._latest_share_links(project_id),
                "client_view_password": None,
            }
        current_date = current_date or dates[-1]
        baseline_date = baseline_date or (dates[-2] if len(dates) >= 2 else dates[0])
        return self._build_cluster_view(
            project_id,
            current_date=current_date,
            baseline_date=baseline_date,
            selected_group=main_cluster,
            status_filter=status_filter,
            tag_filter=tag_filter,
            sort_by=sort_by,
            active_scenario_id=active_scenario_id,
            legacy_mode=legacy_mode,
            custom_config=custom_config,
        )

    def get_keyword_table(self, project_id: int, filters: dict[str, Any]) -> dict[str, Any]:
        keywords = self._load_keywords_with_history(project_id)
        dates = self.get_project_dates(project_id)
        current_date = filters.get("current_date") or (dates[-1] if dates else None)
        search = (filters.get("search") or "").strip().lower()
        groups_filter = {item for item in (filters.get("groups") or "").split(",") if item}
        clusters_filter = {item for item in (filters.get("clusters") or "").split(",") if item}
        status_filter = (filters.get("status") or "all").strip()
        sort_by = filters.get("sort_by") or "current_rank"
        sort_dir = filters.get("sort_dir") or "asc"
        vol_min = int(filters.get("vol_min") or 0)
        vol_max = int(filters.get("vol_max") or 1000000)
        rank_min = float(filters.get("rank_min") or 0)
        rank_max = float(filters.get("rank_max") or 101)
        movers_only = str(filters.get("movers_only") or "").lower() in {"1", "true", "yes"}

        rows = []
        for index, keyword in enumerate(keywords, start=1):
            current_rank = self._history_position(keyword["history"], current_date) if current_date else None
            if current_rank is None:
                continue
            previous_rank = self._history_previous_position(keyword["history"], current_date)
            delta_prev = None if previous_rank is None else round(float(current_rank - previous_rank), 2)
            tags = self._keyword_tags(keyword["history"], int(keyword.get("kpi_target") or 10), current_date)
            if search and search not in keyword["keyword"].lower():
                continue
            if groups_filter and (keyword.get("group_name") or "Chưa phân nhóm") not in groups_filter:
                continue
            if clusters_filter and (keyword.get("cluster_name") or keyword.get("group_name") or "Chưa phân nhóm") not in clusters_filter:
                continue
            if status_filter != "all" and status_filter not in tags:
                continue
            if keyword.get("search_volume") is not None and not (vol_min <= keyword["search_volume"] <= vol_max):
                continue
            if not (rank_min <= current_rank <= rank_max):
                continue
            if movers_only and (delta_prev is None or abs(delta_prev) < 5):
                continue
            rows.append(
                {
                    "index": index,
                    "id": keyword["id"],
                    "group_name": keyword.get("group_name") or "Chưa phân nhóm",
                    "cluster_name": keyword.get("cluster_name") or keyword.get("group_name") or "Chưa phân nhóm",
                    "keyword": keyword["keyword"],
                    "search_volume": keyword.get("search_volume"),
                    "best_rank": keyword.get("best_rank"),
                    "current_rank": current_rank,
                    "delta_prev": delta_prev,
                    "kpi_status": "Đạt KPI" if current_rank <= int(keyword.get("kpi_target") or 10) else "Chưa đạt",
                    "kpi_target": int(keyword.get("kpi_target") or 10),
                    "status_tags": tags,
                    "notes": keyword.get("notes") or "",
                    "positions": {item["rank_date"]: item["position"] for item in keyword["history"]},
                    "client_badge": client_rank_badge(current_rank),
                }
            )

        reverse = sort_dir == "desc"
        rows.sort(key=lambda item: (item.get(sort_by) is None, item.get(sort_by)), reverse=reverse)
        return {
            "dates": dates,
            "rows": rows,
            "groups": sorted({row["group_name"] for row in rows}),
            "clusters": sorted({row["cluster_name"] for row in rows}),
            "current_date": current_date,
        }

    def export_keyword_table(self, project_id: int, filters: dict[str, Any]) -> bytes:
        table = self.get_keyword_table(project_id, filters)
        rows = []
        for row in table["rows"]:
            export_row = {
                "#": row["index"],
                "Bộ": row["group_name"],
                "Cụm": row["cluster_name"],
                "Keyword": row["keyword"],
                "Vol": row["search_volume"],
                "Best Rank": row["best_rank"],
            }
            for rank_date in table["dates"]:
                export_row[format_date_label(rank_date)] = row["positions"].get(rank_date)
            export_row["Thay Đổi"] = row["delta_prev"]
            export_row["KPI Status"] = row["kpi_status"]
            rows.append(export_row)
        frame = pd.DataFrame(rows)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            frame.to_excel(writer, index=False, sheet_name="Keyword Detail")
        return output.getvalue()

    def get_keyword_detail(self, project_id: int, keyword_id: int) -> dict[str, Any]:
        keywords = self._load_keywords_with_history(project_id)
        keyword = next((item for item in keywords if item["id"] == keyword_id), None)
        if not keyword:
            raise ValueError("Không tìm thấy keyword.")
        latest_insights = self._load_latest_insights(project_id)
        keyword_insight = latest_insights.get("keyword_detail") if latest_insights.get("keyword_detail", {}).get("keyword") == keyword["keyword"] else None
        return {
            "keyword": keyword,
            "history": keyword["history"],
            "client_badge": client_rank_badge(keyword["history"][-1]["position"] if keyword["history"] else None),
            "latest_insight": keyword_insight,
        }

    def save_keyword_notes(self, project_id: int, keyword_id: int, notes: str) -> dict[str, Any]:
        with transaction() as connection:
            connection.execute(
                """
                UPDATE keywords
                SET notes = ?, updated_at = ?
                WHERE id = ? AND project_id = ?
                """,
                (notes.strip(), now_iso(), keyword_id, project_id),
            )
        return self.get_keyword_detail(project_id, keyword_id)

    def add_manual_event(self, project_id: int, event_date: str, title: str, description: str, impact_type: str) -> dict[str, Any]:
        with transaction() as connection:
            cursor = connection.execute(
                """
                INSERT INTO events (project_id, event_date, title, description, impact_type, is_manual)
                VALUES (?, ?, ?, ?, ?, 1)
                """,
                (project_id, event_date, title.strip(), description.strip(), impact_type),
            )
            event_id = int(cursor.lastrowid)
            row = connection.execute("SELECT * FROM events WHERE id = ?", (event_id,)).fetchone()
        return dict(row)

    def refresh_anomaly_events(self, project_id: int) -> list[dict[str, Any]]:
        keywords = self._load_keywords_with_history(project_id)
        auto_events: list[dict[str, Any]] = []
        cluster_drops: dict[tuple[str, str], list[float]] = defaultdict(list)
        for keyword in keywords:
            history = keyword["history"]
            for index in range(1, len(history)):
                previous = history[index - 1]
                current = history[index]
                if previous["position"] is None or current["position"] is None:
                    continue
                delta = float(current["position"] - previous["position"])
                if delta > 10:
                    auto_events.append(
                        {
                            "event_date": current["rank_date"],
                            "title": "⚠️ Biến động mạnh",
                            "description": (
                                f"{keyword['keyword']} giảm từ Top {int(previous['position'])} xuống Top {int(current['position'])} "
                                f"trong giai đoạn {format_date_label(previous['rank_date'])} - {format_date_label(current['rank_date'])} "
                                f"({int(delta)} bậc)."
                            ),
                            "impact_type": "warning",
                        }
                    )
                if current["position"] <= 3 < previous["position"]:
                    auto_events.append(
                        {
                            "event_date": current["rank_date"],
                            "title": "🎉 Vào top 3",
                            "description": (
                                f"{keyword['keyword']} đi từ Top {int(previous['position'])} lên Top {int(current['position'])} "
                                f"trong giai đoạn {format_date_label(previous['rank_date'])} - {format_date_label(current['rank_date'])}."
                            ),
                            "impact_type": "positive",
                        }
                    )
                if index >= 2:
                    old = history[index - 2]
                    if (
                        old["position"] is not None
                        and history[index - 1]["position"] - old["position"] > 10
                        and current["position"] <= old["position"] + 2
                    ):
                        auto_events.append(
                            {
                                "event_date": current["rank_date"],
                                "title": "↩️ Phục hồi",
                                "description": (
                                    f"{keyword['keyword']} đã phục hồi từ Top {int(history[index - 1]['position'])} về Top {int(current['position'])} "
                                    f"trong giai đoạn {format_date_label(history[index - 1]['rank_date'])} - {format_date_label(current['rank_date'])}, "
                                    f"sau khi từng rơi khỏi Top {int(old['position'])}."
                                ),
                                "impact_type": "recovery",
                            }
                        )
                cluster_drops[(keyword.get("group_name") or "Chưa phân nhóm", current["rank_date"])].append(delta)

        for (group_name, rank_date), deltas in cluster_drops.items():
            if deltas and sum(1 for delta in deltas if delta > 3) / len(deltas) > 0.5:
                auto_events.append(
                    {
                        "event_date": rank_date,
                        "title": "⚠️ Có thể Google update",
                        "description": (
                            f"Hơn 50% keyword của {group_name} giảm cùng ngày ở mốc {format_date_label(rank_date)}. "
                            "Nên đối chiếu thêm thay đổi SERP và kỹ thuật."
                        ),
                        "impact_type": "warning",
                    }
                )

        with transaction() as connection:
            connection.execute("DELETE FROM events WHERE project_id = ? AND is_manual = 0", (project_id,))
            for event in auto_events:
                connection.execute(
                    """
                    INSERT INTO events (project_id, event_date, title, description, impact_type, is_manual)
                    VALUES (?, ?, ?, ?, ?, 0)
                    """,
                    (
                        project_id,
                        event["event_date"],
                        event["title"],
                        event["description"],
                        event["impact_type"],
                    ),
                )
        return self._load_events(project_id)

    def save_weekly_note(self, project_id: int, content: str) -> dict[str, Any]:
        dates = self.get_project_dates(project_id)
        insight_date = dates[-1] if dates else now_iso().split("T")[0]
        note = content.strip()
        if not note:
            raise ValueError("Nội dung nhận xét không được để trống.")
        with transaction() as connection:
            cursor = connection.execute(
                """
                INSERT INTO ai_insights (
                    project_id, insight_date, insight_type, cluster_name, keyword, content_vi, generated_at
                )
                VALUES (?, ?, 'weekly_summary', NULL, NULL, ?, ?)
                """,
                (project_id, insight_date, note, now_iso()),
            )
            row = connection.execute("SELECT * FROM ai_insights WHERE id = ?", (int(cursor.lastrowid),)).fetchone()
        return dict(row)

    def generate_daily_note(self, project_id: int, insight_date: str, seo_input: str = "") -> dict[str, Any]:
        context = self._date_note_context(project_id, insight_date)
        prompt = (
            "Bạn là SEO analyst. Dưới đây là snapshot dữ liệu dashboard SEO theo ngày:\n"
            f"{context}\n"
            f"Ghi chú thêm từ team SEO (nếu có): {seo_input.strip() or 'không có'}\n"
            "Hãy viết nhận xét ngắn gọn bằng tiếng Việt theo format:\n"
            "- Tổng quan: [1 câu tóm tắt cho ngày đang chọn]\n"
            "- Điểm sáng: [group/keyword tăng tốt nhất theo dữ liệu]\n"
            "- Điểm cần chú ý: [group/keyword giảm hoặc cảnh báo]\n"
            "- Nhận định: [mẫu xu hướng hoặc hành động nên ưu tiên]\n"
            "Ngắn gọn, đọc dễ, bám sát dữ liệu thật."
        )
        api_key = self._project_api_key(project_id)
        content = call_claude(prompt, api_key) or self._fallback_date_note(context, seo_input)
        return {
            "insight_date": insight_date,
            "compare_date": context["compare_date"],
            "content_vi": content,
            "generated_at": now_iso(),
            "is_pinned": False,
            "seo_input": seo_input.strip(),
        }

    def save_pinned_daily_note(
        self,
        project_id: int,
        insight_date: str,
        content: str,
        seo_input: str = "",
    ) -> dict[str, Any]:
        if insight_date not in self.get_project_dates(project_id):
            raise ValueError("Ngày cần ghim không nằm trong dataset hiện tại.")
        note = content.strip()
        if not note:
            raise ValueError("Nội dung ghi chú không được để trống.")
        with transaction() as connection:
            connection.execute(
                """
                DELETE FROM ai_insights
                WHERE project_id = ? AND insight_type = 'pinned_daily_note' AND insight_date = ?
                """,
                (project_id, insight_date),
            )
            cursor = connection.execute(
                """
                INSERT INTO ai_insights (
                    project_id, insight_date, insight_type, cluster_name, keyword, content_vi, generated_at
                )
                VALUES (?, ?, 'pinned_daily_note', NULL, ?, ?, ?)
                """,
                (
                    project_id,
                    insight_date,
                    seo_input.strip() or None,
                    note,
                    now_iso(),
                ),
            )
            row = connection.execute("SELECT * FROM ai_insights WHERE id = ?", (int(cursor.lastrowid),)).fetchone()
        result = dict(row)
        result["is_pinned"] = True
        return result

    def remove_pinned_daily_note(self, project_id: int, insight_date: str) -> dict[str, bool]:
        with transaction() as connection:
            connection.execute(
                """
                DELETE FROM ai_insights
                WHERE project_id = ? AND insight_type = 'pinned_daily_note' AND insight_date = ?
                """,
                (project_id, insight_date),
            )
        return {"ok": True}

    def generate_weekly_summary(self, project_id: int, *, force: bool = True) -> dict[str, Any]:
        dates = self.get_project_dates(project_id)
        if not dates:
            raise ValueError("Project chưa có dữ liệu để tạo insight.")
        latest_date = dates[-1]
        existing = self._load_latest_insights(project_id).get("weekly_summary")
        if existing and existing["insight_date"] == latest_date and not force:
            return existing
        previous_date = dates[-2] if len(dates) >= 2 else None
        keywords = self._load_keywords_with_history(project_id)
        group_metrics = self._build_group_metrics(keywords, latest_date, previous_date)
        cluster_rows = []
        for name, metric in group_metrics.items():
            cluster_rows.append(
                {
                    "name": name,
                    "keyword_count": metric["keyword_count"],
                    "avg_rank": metric["avg_rank"],
                    "avg_prev_rank": metric["avg_prev_rank"],
                    "avg_delta": metric["avg_delta"] or 0,
                    "kpi_status": f"{metric['achieved']}/{metric['keyword_count']}",
                    "kpi_target": metric["kpi_target"],
                }
            )
        movers = []
        kpi_hits = []
        for keyword in keywords:
            latest_rank = self._history_position(keyword["history"], latest_date)
            previous_rank = self._history_position(keyword["history"], previous_date) if previous_date else None
            if latest_rank is None or previous_rank is None:
                continue
            delta = round(float(latest_rank - previous_rank), 2)
            movers.append({"keyword": keyword["keyword"], "delta": delta, "latest_rank": latest_rank})
            kpi_target = int(keyword.get("kpi_target") or 10)
            if latest_rank <= kpi_target < previous_rank:
                kpi_hits.append(keyword["keyword"])
        movers_up = sorted([item for item in movers if item["delta"] < 0], key=lambda item: item["delta"])[:5]
        movers_down = sorted([item for item in movers if item["delta"] > 0], key=lambda item: item["delta"], reverse=True)[:5]

        data_payload = {
            "clusters": cluster_rows,
            "movers_up": movers_up,
            "movers_down": movers_down,
            "kpi_hits": kpi_hits[:10],
        }
        prompt = (
            "Bạn là SEO analyst. Dưới đây là dữ liệu ranking tuần này vs tuần trước:\n"
            f"{data_payload}\n"
            "Hãy viết nhận xét ngắn gọn (3-5 câu) theo format:\n"
            "- Tổng quan: [1 câu tóm tắt tuần]\n"
            "- Điểm sáng: [cụm/keyword tăng tốt nhất]\n"
            "- Điểm cần chú ý: [cụm/keyword giảm hoặc lo ngại]\n"
            "- Nhận định: [pattern hoặc xu hướng đáng chú ý]\n"
            "Viết tiếng Việt, ngắn gọn, dành cho khách hàng đọc."
        )
        api_key = self._project_api_key(project_id)
        content = call_claude(prompt, api_key) or fallback_weekly_summary(cluster_rows, movers_up, movers_down, kpi_hits)
        with transaction() as connection:
            cursor = connection.execute(
                """
                INSERT INTO ai_insights (
                    project_id, insight_date, insight_type, cluster_name, keyword, content_vi, generated_at
                )
                VALUES (?, ?, 'weekly_summary', NULL, NULL, ?, ?)
                """,
                (project_id, latest_date, content, now_iso()),
            )
            row = connection.execute("SELECT * FROM ai_insights WHERE id = ?", (int(cursor.lastrowid),)).fetchone()
        return dict(row)

    def generate_cluster_pattern_insight(self, project_id: int, cluster_name: str) -> dict[str, Any]:
        keywords = [item for item in self._load_keywords_with_history(project_id) if item.get("cluster_name") == cluster_name or item.get("group_name") == cluster_name]
        if not keywords:
            raise ValueError("Không tìm thấy cụm cần phân tích.")
        latest_date = self.get_project_dates(project_id)[-1]
        latest_deltas = []
        volumes = []
        stable_keywords = []
        volatile_keywords = []
        for keyword in keywords:
            if keyword.get("search_volume") is not None:
                volumes.append(float(keyword["search_volume"]))
            delta = keyword["history"][-1]["delta_from_prev"] if keyword["history"] else None
            if delta is not None:
                latest_deltas.append(float(delta))
            recent_positions = [item["position"] for item in keyword["history"][-5:] if item["position"] is not None]
            if len(recent_positions) >= 3:
                volatility = max(recent_positions) - min(recent_positions)
                if volatility <= 3:
                    stable_keywords.append(keyword["keyword"])
                elif volatility >= 10:
                    volatile_keywords.append(keyword["keyword"])
        correlation_text = "chưa đủ dữ liệu để đo tương quan volume và biến động rank"
        if len(volumes) >= 2 and len(latest_deltas) >= 2 and len(volumes) == len(latest_deltas):
            correlation = pd.Series(volumes).corr(pd.Series(latest_deltas))
            if pd.notna(correlation):
                if correlation <= -0.3:
                    correlation_text = "volume cao đang có xu hướng cải thiện tốt hơn"
                elif correlation >= 0.3:
                    correlation_text = "volume cao chưa mang lại lợi thế rank rõ rệt"
                else:
                    correlation_text = "tương quan volume và biến động rank hiện khá trung tính"
        seasonal_text = "Chưa đủ chu kỳ dài để khẳng định seasonality rõ ràng."
        dates = self.get_project_dates(project_id)
        if len(dates) >= 8:
            seasonal_text = "Dữ liệu đủ dài để theo dõi mẫu lặp theo tuần, nhưng chưa thấy nhịp mùa vụ quá rõ."
        prompt = (
            "Phân tích cụm SEO sau bằng tiếng Việt, 2-3 câu ngắn:\n"
            f"Cụm: {cluster_name}\n"
            f"Số keyword: {len(keywords)}\n"
            f"Tương quan: {correlation_text}\n"
            f"Ổn định: {stable_keywords[:5]}\n"
            f"Biến động: {volatile_keywords[:5]}\n"
            f"Seasonal: {seasonal_text}"
        )
        api_key = self._project_api_key(project_id)
        content = call_claude(prompt, api_key) or fallback_cluster_pattern(
            cluster_name,
            len(keywords),
            correlation_text,
            stable_keywords,
            volatile_keywords,
            seasonal_text,
        )
        with transaction() as connection:
            cursor = connection.execute(
                """
                INSERT INTO ai_insights (
                    project_id, insight_date, insight_type, cluster_name, keyword, content_vi, generated_at
                )
                VALUES (?, ?, 'cluster_pattern', ?, NULL, ?, ?)
                """,
                (project_id, latest_date, cluster_name, content, now_iso()),
            )
            row = connection.execute("SELECT * FROM ai_insights WHERE id = ?", (int(cursor.lastrowid),)).fetchone()
        return dict(row)

    def generate_keyword_insight(self, project_id: int, keyword_id: int) -> dict[str, Any]:
        detail = self.get_keyword_detail(project_id, keyword_id)
        keyword = detail["keyword"]
        latest = keyword["history"][-1] if keyword["history"] else None
        prompt = (
            "Viết 2-3 câu insight SEO ngắn gọn bằng tiếng Việt cho keyword sau:\n"
            f"Keyword: {keyword['keyword']}\n"
            f"Lịch sử: {keyword['history']}\n"
            "Tập trung vào xu hướng gần đây, rủi ro và khuyến nghị ngắn."
        )
        api_key = self._project_api_key(project_id)
        content = call_claude(prompt, api_key) or fallback_keyword_insight(
            keyword["keyword"],
            latest["position"] if latest else None,
            keyword.get("best_rank"),
            latest["delta_from_prev"] if latest else None,
        )
        latest_date = latest["rank_date"] if latest else now_iso().split("T")[0]
        with transaction() as connection:
            cursor = connection.execute(
                """
                INSERT INTO ai_insights (
                    project_id, insight_date, insight_type, cluster_name, keyword, content_vi, generated_at
                )
                VALUES (?, ?, 'keyword_detail', NULL, ?, ?, ?)
                """,
                (project_id, latest_date, keyword["keyword"], content, now_iso()),
            )
            row = connection.execute("SELECT * FROM ai_insights WHERE id = ?", (int(cursor.lastrowid),)).fetchone()
        return dict(row)

    def refresh_due_projects(self) -> list[int]:
        refreshed: list[int] = []
        now_value = datetime.now()
        for project in self.list_projects():
            sheet_url = (project.get("sheet_url") or "").strip()
            if not sheet_url:
                continue
            interval = int(project.get("refresh_interval_minutes") or 30)
            last_pulled_at = project.get("last_pulled_at")
            if last_pulled_at:
                elapsed = now_value - datetime.fromisoformat(last_pulled_at)
                if elapsed.total_seconds() < interval * 60:
                    continue
            try:
                self.refresh_from_google_sheet(int(project["id"]))
                refreshed.append(int(project["id"]))
            except Exception:
                continue
        return refreshed
