from __future__ import annotations

import os
from typing import Any

try:
    from anthropic import Anthropic
except ModuleNotFoundError:  # pragma: no cover - depends on environment
    Anthropic = None


DEFAULT_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-20241022")


def call_claude(prompt: str, api_key: str | None) -> str | None:
    if not api_key or not Anthropic:
        return None
    client = Anthropic(api_key=api_key)
    response = client.messages.create(
        model=DEFAULT_MODEL,
        max_tokens=300,
        temperature=0.2,
        messages=[{"role": "user", "content": prompt}],
    )
    parts = []
    for block in response.content:
        text = getattr(block, "text", "")
        if text:
            parts.append(text.strip())
    return "\n".join(part for part in parts if part).strip() or None


def fallback_weekly_summary(
    cluster_rows: list[dict[str, Any]],
    movers_up: list[dict[str, Any]],
    movers_down: list[dict[str, Any]],
    kpi_hits: list[str],
) -> str:
    if not cluster_rows:
        return "Tổng quan: Chưa đủ dữ liệu để tạo nhận xét tuần này."
    strongest = min(cluster_rows, key=lambda item: item["avg_delta"])
    weakest = max(cluster_rows, key=lambda item: item["avg_delta"])
    brightest = movers_up[0]["keyword"] if movers_up else strongest["name"]
    risky = movers_down[0]["keyword"] if movers_down else weakest["name"]
    hit_text = ", ".join(kpi_hits[:3]) if kpi_hits else "chưa có từ khóa mới vượt KPI"
    return (
        f"Tổng quan: Tuần này {strongest['name']} đang dẫn nhịp tăng, trong khi {weakest['name']} cần theo dõi thêm.\n"
        f"Điểm sáng: {brightest} là tín hiệu tích cực nhất với mức cải thiện rõ rệt trong kỳ gần nhất.\n"
        f"Điểm cần chú ý: {risky} hoặc nhóm {weakest['name']} đang giảm nhanh hơn mặt bằng chung.\n"
        f"Nhận định: Xu hướng hiện tại cho thấy {hit_text} và hiệu suất đang phân hóa theo từng bộ từ khóa."
    )


def fallback_cluster_pattern(
    cluster_name: str,
    keyword_count: int,
    correlation_text: str,
    stable_keywords: list[str],
    volatile_keywords: list[str],
    seasonal_text: str,
) -> str:
    stable_text = ", ".join(stable_keywords[:2]) if stable_keywords else "chưa có keyword ổn định rõ rệt"
    volatile_text = ", ".join(volatile_keywords[:2]) if volatile_keywords else "biến động hiện còn thấp"
    return (
        f"Cụm {cluster_name} hiện có {keyword_count} keyword, {correlation_text}. "
        f"Nhóm ổn định nhất là {stable_text}, trong khi {volatile_text} cần theo dõi sát hơn. "
        f"{seasonal_text}"
    )


def fallback_keyword_insight(keyword: str, current_rank: float | None, best_rank: float | None, delta: float | None) -> str:
    if current_rank is None:
        return f"Keyword {keyword} hiện chưa có dữ liệu mới để nhận xét."
    movement = "ổn định"
    if delta is not None and delta < -2:
        movement = "đang cải thiện khá nhanh"
    elif delta is not None and delta > 2:
        movement = "đang giảm và cần kiểm tra"
    best_text = f"Top tốt nhất từng đạt là {int(best_rank)}" if best_rank is not None else "chưa có mốc tốt nhất rõ ràng"
    return f"Keyword {keyword} hiện ở vị trí {int(current_rank)} và {movement}. {best_text}."

