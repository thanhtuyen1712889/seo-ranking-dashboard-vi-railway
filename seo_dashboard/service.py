from __future__ import annotations

import io
import os
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
    fetch_google_sheet,
    infer_sub_cluster_name,
    kpi_type_from_target,
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

    def test_google_sheet(self, sheet_url: str) -> dict[str, Any]:
        payload, filename, gid = fetch_google_sheet(sheet_url)
        parsed = parse_spreadsheet_payload(filename, payload, source_name="Google Sheets")
        return {
            "ok": True,
            "source_name": parsed.source_name,
            "dates": parsed.dates,
            "header_row_index": parsed.header_row_index,
            "warnings": parsed.warnings[:10],
            "row_count": len(parsed.rows),
            "sheet_gid": gid,
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
        payload, filename, gid = fetch_google_sheet(sheet_url)
        parsed = parse_spreadsheet_payload(filename, payload, source_name="Google Sheets")
        return self._ingest_parsed_sheet(
            project_id,
            parsed,
            source_type="google_sheet",
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
        with transaction() as connection:
            existing_keywords = {
                row["keyword"]: dict(row)
                for row in connection.execute(
                    "SELECT id, keyword FROM keywords WHERE project_id = ?",
                    (project_id,),
                ).fetchall()
            }
            current_time = now_iso()
            for row in parsed.rows:
                keyword_record = existing_keywords.get(row.keyword)
                if keyword_record:
                    keyword_id = keyword_record["id"]
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
                            row.kpi_target,
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
                            row.kpi_target,
                            current_time,
                            current_time,
                        ),
                    )
                    keyword_id = int(cursor.lastrowid)
                    existing_keywords[row.keyword] = {"id": keyword_id, "keyword": row.keyword}

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
                (source_name, source_type, sheet_url, sheet_gid, current_time, project_id),
            )
            self._recalculate_deltas(connection, project_id)
            self._refresh_clusters(connection, project_id, parsed)

        new_dates = sorted(set(parsed.dates) - existing_dates)
        self.refresh_anomaly_events(project_id)
        if new_dates:
            self.generate_weekly_summary(project_id)
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
            kpi_target = int(target_counter.most_common(1)[0][0]) if target_counter else parsed.kpi_map.get(group_name, 10)
            previous_cluster = existing_targets.get((group_name, cluster_name))
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
        sheet_gid = project.get("sheet_gid")
        if sheet_url:
            try:
                test_result = self.test_google_sheet(sheet_url)
                sheet_gid = test_result.get("sheet_gid")
            except Exception:
                sheet_gid = project.get("sheet_gid")
        with transaction() as connection:
            connection.execute(
                """
                UPDATE projects
                SET name = ?, sheet_url = ?, sheet_gid = ?, source_name = ?,
                    refresh_interval_minutes = ?, anthropic_api_key = ?
                WHERE id = ?
                """,
                (
                    name,
                    sheet_url,
                    sheet_gid,
                    source_name,
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
    ) -> dict[str, Any]:
        keywords = self._load_keywords_with_history(project_id)
        dates = self.get_project_dates(project_id)
        if not dates:
            return {"groups": [], "dates": [], "baseline_date": None, "current_date": None}
        current_date = current_date or dates[-1]
        baseline_date = baseline_date or (dates[-2] if len(dates) >= 2 else dates[0])
        groups: dict[str, dict[str, Any]] = {}
        timeline_peak = self.get_overview(project_id)["timeline"]
        for keyword in keywords:
            current_rank = self._history_position(keyword["history"], current_date)
            if current_rank is None:
                continue
            previous_rank = self._history_previous_position(keyword["history"], current_date)
            baseline_rank = self._history_position(keyword["history"], baseline_date)
            delta_prev = None if previous_rank is None else round(float(current_rank - previous_rank), 2)
            delta_baseline = None if baseline_rank is None else round(float(current_rank - baseline_rank), 2)
            tags = self._keyword_tags(keyword["history"], int(keyword.get("kpi_target") or 10), current_date)
            if status_filter != "all" and status_filter not in tags:
                continue
            progress = min(100, round((int(keyword.get("kpi_target") or 10) / max(current_rank, 1)) * 100, 1))
            group_name = keyword.get("group_name") or "Chưa phân nhóm"
            entry = groups.setdefault(
                group_name,
                {
                    "name": group_name,
                    "kpi_target": int(keyword.get("kpi_target") or 10),
                    "keywords": [],
                },
            )
            entry["keywords"].append(
                {
                    "id": keyword["id"],
                    "keyword": keyword["keyword"],
                    "cluster_name": keyword.get("cluster_name") or group_name,
                    "sub_cluster_name": keyword.get("sub_cluster_name") or "",
                    "search_volume": keyword.get("search_volume"),
                    "current_rank": current_rank,
                    "baseline_rank": baseline_rank,
                    "best_rank": keyword.get("best_rank"),
                    "delta_prev": delta_prev,
                    "delta_baseline": delta_baseline,
                    "progress": progress,
                    "kpi_target": int(keyword.get("kpi_target") or 10),
                    "tags": tags,
                    "client_badge": client_rank_badge(current_rank),
                }
            )
        for group_name, entry in groups.items():
            entry["keywords"].sort(key=lambda item: (item["current_rank"], item["keyword"].lower()))
            entry["keyword_count"] = len(entry["keywords"])
            entry["achieved"] = sum(1 for item in entry["keywords"] if item["current_rank"] <= item["kpi_target"])
            peaks = [timeline_item["groups"].get(group_name, 0) for timeline_item in timeline_peak]
            if peaks:
                peak_value = max(peaks)
                peak_index = peaks.index(peak_value)
                peak_label = format_date_label(timeline_peak[peak_index]["date"])
            else:
                peak_value = 0
                peak_label = "-"
            entry["peak_info"] = f"Đỉnh {peak_value}/{entry['keyword_count']} đạt KPI vào {peak_label}"
        ordered_groups = [groups[name] for name in sorted(groups)]
        return {
            "groups": ordered_groups,
            "dates": dates,
            "current_date": current_date,
            "baseline_date": baseline_date,
        }

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

    def generate_weekly_summary(self, project_id: int) -> dict[str, Any]:
        dates = self.get_project_dates(project_id)
        if not dates:
            raise ValueError("Project chưa có dữ liệu để tạo insight.")
        latest_date = dates[-1]
        existing = self._load_latest_insights(project_id).get("weekly_summary")
        if existing and existing["insight_date"] == latest_date:
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

