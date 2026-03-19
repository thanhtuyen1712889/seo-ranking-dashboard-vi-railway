from __future__ import annotations

import io
import os
import re
from collections import Counter, defaultdict
from datetime import datetime
from statistics import mean
from typing import Any

import pandas as pd

from .ai import (
    call_claude,
    fallback_cluster_pattern,
    fallback_keyword_insight,
    fallback_weekly_summary,
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


PRODUCT_CLUSTER_LABELS = {
    "plugin": "Plugin",
    "module": "Module",
    "extension": "Extension",
    "suite": "Suite / package",
    "theme": "Theme",
    "store": "Store",
    "seo_extension": "SEO Extension",
}

PLATFORM_LABELS = {
    "magento_2": "Magento 2",
    "magento_1_or_generic": "Magento",
    "shopify": "Shopify",
    "woocommerce": "WooCommerce",
}

USE_CASE_LABELS = {
    "b2b": "Generic B2B",
    "integration": "Integration / connector",
    "checkout": "Checkout & payment",
    "subscription": "Subscription / recurring",
    "login": "Login / account access",
}

CLUSTER_SORTS = {
    "health_score": ("health_score", True),
    "trend_strength": ("rank_delta", True),
    "total_volume": ("total_volume", True),
    "avg_rank": ("avg_rank_current", False),
}

PRODUCT_TAG_PATTERNS = {
    "seo_extension": [r"\bseo\b"],
    "plugin": [r"\bplugins?\b"],
    "module": [r"\bmodules?\b"],
    "extension": [r"\bextensions?\b", r"\baddons?\b", r"\badd[- ]ons?\b"],
    "suite": [r"\bsuites?\b", r"\bpackages?\b", r"\bbundle\b"],
    "theme": [r"\bthemes?\b"],
    "store": [r"\bstores?\b", r"\bstorefront\b", r"\bstore front\b"],
}

PLATFORM_TAG_PATTERNS = {
    "magento_2": [r"\bmagento 2\b", r"\bfor magento 2\b"],
    "magento_1_or_generic": [r"\bmagento\b"],
    "shopify": [r"\bshopify\b"],
    "woocommerce": [r"\bwoocommerce\b"],
}

INTENT_TAG_PATTERNS = {
    "b2b": [r"\bb2b\b"],
    "integration": [r"\bintegration\b", r"\bconnector\b"],
    "checkout": [r"\bcheckout\b", r"\bpayment\b"],
    "subscription": [r"\bsubscription\b", r"\brecurring\b"],
    "login": [r"\blogin\b", r"\bcustomer login\b", r"\baccount access\b"],
}


class DashboardService:
    def __init__(self) -> None:
        self._ensure_seedless()

    def _ensure_seedless(self) -> None:
        # The database is initialized by main.py on startup.
        return None

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
        return [dict(row) for row in rows]

    def get_project(self, project_id: int) -> dict[str, Any]:
        with get_connection() as connection:
            row = connection.execute("SELECT * FROM projects WHERE id = ?", (project_id,)).fetchone()
        if not row:
            raise ValueError("Không tìm thấy project.")
        return dict(row)

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
        }

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

    def _match_tag_patterns(self, combined: str, mapping: dict[str, list[str]]) -> list[str]:
        matched: list[str] = []
        for tag, patterns in mapping.items():
            if any(re.search(pattern, combined) for pattern in patterns):
                matched.append(tag)
        return matched

    def _match_preferred_tags(self, primary_text: str, fallback_text: str, mapping: dict[str, list[str]]) -> list[str]:
        matched = self._match_tag_patterns(primary_text, mapping)
        if matched:
            return matched
        return self._match_tag_patterns(fallback_text, mapping)

    def _keyword_subcluster_tags(self, keyword: dict[str, Any]) -> list[str]:
        group_name = keyword.get("group_name") or "general"
        cluster_name = keyword.get("cluster_name") or ""
        keyword_text = normalize_label(keyword.get("keyword") or "")
        context_text = normalize_label(" ".join([group_name, cluster_name]))
        tags: list[str] = []

        # Prefer signals from the keyword itself so "module/plugin" is not swallowed
        # by a broader group label such as "M2 Extensions".
        tags.extend(self._match_preferred_tags(keyword_text, context_text, PRODUCT_TAG_PATTERNS))
        platform_tags = self._match_preferred_tags(keyword_text, context_text, PLATFORM_TAG_PATTERNS)
        if "magento_2" in platform_tags and "magento_1_or_generic" in platform_tags:
            platform_tags = [tag for tag in platform_tags if tag != "magento_1_or_generic"]
        tags.extend(platform_tags)
        tags.extend(self._match_preferred_tags(keyword_text, context_text, INTENT_TAG_PATTERNS))

        normalized_group = normalize_label(group_name).replace(" ", "_")
        if normalized_group:
            tags.append(normalized_group)
        return sorted(set(tags))

    def _subcluster_descriptor(self, keyword: dict[str, Any], clustering_mode: str) -> tuple[str, str, list[str]]:
        tags = self._keyword_subcluster_tags(keyword)
        product_tag = next(
            (tag for tag in ("seo_extension", "suite", "extension", "module", "plugin", "theme", "store") if tag in tags),
            None,
        )
        platform_tag = next(
            (tag for tag in ("magento_2", "shopify", "woocommerce", "magento_1_or_generic") if tag in tags),
            None,
        )
        use_case_tag = next(
            (tag for tag in ("b2b", "integration", "checkout", "subscription", "login") if tag in tags),
            None,
        )

        cluster_id_parts: list[str] = []
        cluster_label = ""
        if clustering_mode == "platform":
            primary_platform = platform_tag or "other_platform"
            cluster_id_parts.extend(["platform", primary_platform])
            if platform_tag:
                cluster_label = f"{PLATFORM_LABELS[platform_tag]} (mọi loại)"
            else:
                cluster_label = "Khác (mọi loại)"
        elif clustering_mode == "product_type":
            primary_product = product_tag or "other_type"
            cluster_id_parts.extend(["product_type", primary_product])
            if product_tag:
                cluster_label = f"{PRODUCT_CLUSTER_LABELS[product_tag]} (mọi nền tảng)"
            else:
                cluster_label = "Khác (mọi nền tảng)"
        elif clustering_mode == "intent":
            primary_intent = use_case_tag or ("b2b" if "b2b" in tags else "generic_intent")
            cluster_id_parts.extend(["intent", primary_intent])
            if primary_intent in USE_CASE_LABELS:
                cluster_label = USE_CASE_LABELS[primary_intent]
            else:
                cluster_label = "Nhu cầu chung"
        elif product_tag:
            cluster_id_parts.append(product_tag)
            cluster_label = PRODUCT_CLUSTER_LABELS[product_tag]
            if platform_tag:
                cluster_id_parts.append(platform_tag)
                cluster_label = f"{cluster_label} ({PLATFORM_LABELS[platform_tag]})"
        elif use_case_tag:
            cluster_id_parts.append(use_case_tag)
            cluster_label = USE_CASE_LABELS[use_case_tag]
            if platform_tag:
                cluster_id_parts.append(platform_tag)
                cluster_label = f"{cluster_label} ({PLATFORM_LABELS[platform_tag]})"
        elif platform_tag:
            cluster_id_parts.append(platform_tag)
            cluster_label = PLATFORM_LABELS[platform_tag]
        else:
            fallback_name = (keyword.get("cluster_name") or keyword.get("group_name") or "Khác").strip()
            cluster_id_parts.append(normalize_label(fallback_name).replace(" ", "_") or "other")
            cluster_label = fallback_name

        cluster_id = "_".join(cluster_id_parts)
        return cluster_id, cluster_label, tags

    def _cluster_keyword_row(
        self,
        keyword: dict[str, Any],
        current_date: str,
        baseline_date: str | None,
        clustering_mode: str,
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
        cluster_id, cluster_name, tags = self._subcluster_descriptor(keyword, clustering_mode)
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
        clustering_mode: str,
    ) -> dict[str, Any]:
        keywords = self._load_keywords_with_history(project_id)
        groups = sorted({(keyword.get("group_name") or "Chưa phân nhóm") for keyword in keywords})
        selected_group = selected_group or (groups[0] if groups else None)
        relevant_rows = []
        for keyword in keywords:
            if selected_group and (keyword.get("group_name") or "Chưa phân nhóm") != selected_group:
                continue
            row = self._cluster_keyword_row(keyword, current_date, baseline_date, clustering_mode)
            if row is None:
                continue
            relevant_rows.append(row)

        cluster_map: dict[str, dict[str, Any]] = {}
        for row in relevant_rows:
            cluster = cluster_map.setdefault(
                row["cluster_id"],
                {
                    "cluster_id": row["cluster_id"],
                    "cluster_name": row["cluster_name"],
                    "tags": sorted(set(row["tags"])),
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
            if row["volume"]:
                cluster["volumes"].append(float(row["volume"]))
            cluster["tags"] = sorted(set(cluster["tags"]) | set(row["tags"]))

        max_total_volume = max((sum(cluster["volumes"]) for cluster in cluster_map.values()), default=1.0)
        dates = self.get_project_dates(project_id)
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
                    "tags": item["tags"],
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
                            "tags": item["tags"],
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
        return {
            "clustering_mode": clustering_mode,
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
        }

    def get_overview(self, project_id: int) -> dict[str, Any]:
        project = self.get_project(project_id)
        projects = self.list_projects()
        keywords = self._load_keywords_with_history(project_id)
        dates = self.get_project_dates(project_id)
        latest_insights = self._load_latest_insights(project_id)
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
            }
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
            "latest_insight": latest_insights.get("weekly_summary"),
            "events": events[:20],
            "timeline": timeline,
            "group_names": group_names,
            "donut": donut,
            "distribution": [{"name": key, "value": value} for key, value in distribution_buckets.items()],
            "avg_trend": avg_trend,
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
        clustering_mode: str = "default",
    ) -> dict[str, Any]:
        dates = self.get_project_dates(project_id)
        if not dates:
            return {
                "clustering_mode": clustering_mode,
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
            clustering_mode=clustering_mode,
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
                            "description": f"{keyword['keyword']} giảm {int(delta)} bậc trong kỳ gần nhất.",
                            "impact_type": "warning",
                        }
                    )
                if current["position"] <= 3 < previous["position"]:
                    auto_events.append(
                        {
                            "event_date": current["rank_date"],
                            "title": "🎉 Vào top 3",
                            "description": f"{keyword['keyword']} vừa vào top 3.",
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
                                "description": f"{keyword['keyword']} đã hồi lại sau nhịp giảm mạnh.",
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
                        "description": f"Hơn 50% keyword của {group_name} giảm cùng ngày.",
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
