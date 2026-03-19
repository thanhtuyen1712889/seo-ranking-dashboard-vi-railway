from __future__ import annotations

import io
import re
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import pandas as pd
import requests
from rapidfuzz import fuzz, process


GROUP_ALIASES = ["group", "bo", "bộ", "nhom", "nhóm"]
CLUSTER_ALIASES = ["cluster", "cum", "cụm", "topic", "category"]
KEYWORD_ALIASES = ["keyword", "tu khoa", "từ khóa", "kw"]
VOLUME_ALIASES = [
    "vol",
    "volume",
    "voulume",
    "keyword volume",
    "search volume",
    "search voulume",
    "searchvol",
]
AVG_ALIASES = ["avg", "average", "avg ranking", "average ranking"]
BEST_ALIASES = ["best rank", "best ranking", "best"]
KPI_ALIASES = ["kpi", "target", "kpi target"]
DELTA_ALIASES = ["delta", "thay doi", "thay đổi", "change", "Δ"]
TARGET_URL_ALIASES = ["url target", "target url"]
FOUND_URL_ALIASES = ["url found", "landing url", "url"]
BRAND_MODIFIERS = [
    "magento",
    "shopify",
    "wordpress",
    "woocommerce",
    "salesforce",
    "hubspot",
    "google",
    "amazon",
    "adobe",
]
COMMERCIAL_TERMS = ["buy", "best", "extension", "plugin", "module", "addon", "tool", "software", "dịch vụ", "giá"]
INFORMATIONAL_TERMS = ["what is", "guide", "how to", "la gi", "là gì", "huong dan", "hướng dẫn", "cach", "cách"]
MODIFIER_TERMS = ["module", "plugin", "extension", "addon", "tool", "software"]


@dataclass(slots=True)
class ParsedRankingRow:
    keyword: str
    group_name: str
    cluster_name: str
    sub_cluster_name: str
    target_url: str | None
    found_url: str | None
    search_volume: int | None
    best_rank: float | None
    kpi_target: int | None
    rankings: dict[str, float]


@dataclass(slots=True)
class ParsedSheet:
    source_name: str
    header_row_index: int
    selected_sheet_name: str | None
    kpi_map: dict[str, int]
    target_keyword_map: dict[str, int]
    rows: list[ParsedRankingRow]
    dates: list[str]
    warnings: list[str]


def normalize_label(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(character for character in text if not unicodedata.combining(character))
    text = re.sub(r"[_\-]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def clean_text(value: Any) -> str:
    text = str(value or "").strip()
    text = re.sub(r"\s+", " ", text)
    if text.lower() == "nan":
        return ""
    return text


def clean_optional_text(value: Any) -> str | None:
    text = clean_text(value)
    return text or None


def kpi_type_from_target(target: int | None) -> str:
    target_value = target or 10
    if target_value <= 3:
        return "top3"
    if target_value <= 5:
        return "top5"
    return "top10"


def parse_search_volume(value: Any) -> int | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = clean_text(value)
    if not text or text in {"—", "-", "N/A", "n/a"}:
        return None
    if text.startswith(">"):
        digits = re.findall(r"\d+", text)
        if digits:
            return int(digits[0]) + 1
    text = text.replace(",", "").replace(".", "")
    digits = re.findall(r"\d+", text)
    if not digits:
        return None
    return int(digits[0])


def parse_rank_value(value: Any) -> float | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
        if numeric > 100:
            return 101.0
        return numeric
    text = clean_text(value)
    if not text or text in {"—", "-", "N/A", "n/a"}:
        return None
    if text.startswith(">"):
        digits = re.findall(r"\d+", text)
        if digits:
            return float(int(digits[0]) + 1)
    digits = re.findall(r"\d+(?:\.\d+)?", text.replace(",", ""))
    if not digits:
        return None
    numeric = float(digits[0])
    if numeric > 100:
        return 101.0
    return numeric


def parse_kpi_target(value: Any) -> int | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (int, float)):
        numeric = int(value)
        return numeric if numeric > 0 else None
    text = clean_text(value)
    if not text:
        return None
    match = re.search(r"top\s*(\d+)", text, re.IGNORECASE)
    if match:
        return int(match.group(1))
    digits = re.findall(r"\d+", text)
    if digits:
        return int(digits[0])
    return None


def infer_sub_cluster_name(keyword: str) -> str:
    lowered = normalize_label(keyword)
    parts: list[str] = []
    if any(term in lowered for term in INFORMATIONAL_TERMS):
        parts.append("Thông tin")
    elif any(term in lowered for term in COMMERCIAL_TERMS):
        parts.append("Thương mại")
    else:
        parts.append("Khám phá")
    modifier = next((term.title() for term in MODIFIER_TERMS if term in lowered), None)
    if modifier:
        parts.append(modifier)
    brand = next((brand.title() for brand in BRAND_MODIFIERS if brand in lowered), None)
    if brand:
        parts.append(brand)
    if len(lowered.split()) >= 4:
        parts.append("Long-tail")
    else:
        parts.append("Head term")
    return " · ".join(parts)


def _best_header_match(columns: list[str], aliases: list[str]) -> str | None:
    normalized_columns = {column: normalize_label(column) for column in columns}
    exact = [
        column
        for column, normalized in normalized_columns.items()
        if normalized in {normalize_label(alias) for alias in aliases}
    ]
    if exact:
        return exact[0]
    contains = [
        column
        for column, normalized in normalized_columns.items()
        if any(alias in normalized for alias in {normalize_label(alias) for alias in aliases})
    ]
    if contains:
        return contains[0]
    choice = process.extractOne(
        " ".join(aliases),
        list(normalized_columns.values()),
        scorer=fuzz.token_set_ratio,
    )
    if choice and choice[1] >= 65:
        for column, normalized in normalized_columns.items():
            if normalized == choice[0]:
                return column
    return None


def _parse_date_header(value: Any, *, dayfirst: bool) -> date | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = clean_text(value)
    if not text:
        return None
    normalized = normalize_label(text)
    if normalized.startswith("kpi"):
        return None
    if not re.search(r"\d", normalized):
        return None
    separators = re.split(r"[\/\-.]", text)
    numeric_parts = [part for part in separators if part.isdigit()]
    if len(numeric_parts) == 2:
        first, second = (int(part) for part in numeric_parts)
        if first > 31 or second > 31:
            return None
        year = datetime.now().year
        if first > 12:
            day, month = first, second
        elif second > 12:
            month, day = first, second
        elif dayfirst:
            day, month = first, second
        else:
            month, day = first, second
        try:
            return date(year, month, day)
        except ValueError:
            return None
    if len(numeric_parts) == 3:
        first, second, third = (int(part) for part in numeric_parts)
        if len(numeric_parts[0]) == 4 or first > 31:
            year, month, day = first, second, third
        elif len(numeric_parts[2]) == 4 or third > 31:
            year = third
            if first > 12:
                day, month = first, second
            elif second > 12:
                month, day = first, second
            elif dayfirst:
                day, month = first, second
            else:
                month, day = first, second
        else:
            year = datetime.now().year
            if first > 12:
                day, month = first, second
            elif second > 12:
                month, day = first, second
            elif dayfirst:
                day, month = first, second
            else:
                month, day = first, second
        try:
            return date(year, month, day)
        except ValueError:
            return None
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        try:
            return datetime.fromisoformat(text).date()
        except ValueError:
            return None
    try:
        parsed = pd.to_datetime(text, errors="coerce", dayfirst=dayfirst)
    except Exception:
        parsed = pd.NaT
    if pd.isna(parsed):
        return None
    parsed_date = parsed.date()
    if re.search(r"\d{4}", text):
        return parsed_date
    return date(datetime.now().year, parsed_date.month, parsed_date.day)


def detect_date_columns(columns: list[str]) -> list[tuple[str, str]]:
    options: list[tuple[int, list[tuple[str, date]]]] = []
    for dayfirst in (True, False):
        detected: list[tuple[str, date]] = []
        penalty = 0
        previous: date | None = None
        for column in columns:
            parsed = _parse_date_header(column, dayfirst=dayfirst)
            if not parsed:
                continue
            candidate = parsed
            if previous and candidate < previous - timedelta(days=120) and re.search(r"^\d{1,2}[\/\-.]\d{1,2}$", str(column)):
                candidate = date(previous.year + 1, candidate.month, candidate.day)
            if previous and candidate < previous:
                penalty += 1
            detected.append((column, candidate))
            previous = candidate
        options.append((penalty, detected))
    best = min(options, key=lambda item: (item[0], -len(item[1])))[1]
    deduplicated: list[tuple[str, str]] = []
    seen: set[str] = set()
    for column, parsed in sorted(best, key=lambda item: item[1]):
        iso_value = parsed.isoformat()
        if iso_value in seen:
            continue
        seen.add(iso_value)
        deduplicated.append((column, iso_value))
    return deduplicated


def _stringify_header(value: Any, index: int) -> str:
    if isinstance(value, datetime):
        base = value.date().isoformat()
    elif isinstance(value, date):
        base = value.isoformat()
    else:
        base = clean_text(value)
    if not base:
        return f"column_{index + 1}"
    return base


def _make_unique_headers(values: list[Any]) -> list[str]:
    headers: list[str] = []
    seen: dict[str, int] = {}
    for index, value in enumerate(values):
        base = _stringify_header(value, index)
        count = seen.get(base, 0) + 1
        seen[base] = count
        headers.append(base if count == 1 else f"{base}_{count}")
    return headers


def detect_header_row(frame: pd.DataFrame, max_rows: int = 20) -> int:
    best_score = -1
    best_index = 0
    for row_index in range(min(len(frame), max_rows)):
        row_values = frame.iloc[row_index].tolist()
        normalized = [normalize_label(value) for value in row_values if clean_text(value)]
        if not normalized:
            continue
        score = 0
        if any(value == "keyword" or "keyword" in value or "tu khoa" in value for value in normalized):
            score += 5
        if any(value == "group" or value == "bo" for value in normalized):
            score += 3
        if any(_parse_date_header(value, dayfirst=True) for value in row_values):
            score += 2
        if any("volume" in value or value == "vol" for value in normalized):
            score += 1
        if score > best_score:
            best_score = score
            best_index = row_index
    return best_index


def _sheet_candidate_score(frame: pd.DataFrame) -> tuple[int, int, int]:
    normalized_frame = frame.dropna(axis=1, how="all")
    if normalized_frame.empty:
        return (-1, 0, 0)
    header_row_index = detect_header_row(normalized_frame)
    header_values = normalized_frame.iloc[header_row_index].tolist()
    headers = _make_unique_headers(header_values)
    keyword_column = _best_header_match(headers, KEYWORD_ALIASES)
    group_column = _best_header_match(headers, GROUP_ALIASES)
    volume_column = _best_header_match(headers, VOLUME_ALIASES)
    date_columns = detect_date_columns(headers)
    populated_rows = 0
    for row_index in range(header_row_index + 1, min(len(normalized_frame), header_row_index + 31)):
        row_values = normalized_frame.iloc[row_index].tolist()
        if any(clean_text(value) for value in row_values):
            populated_rows += 1
    score = 0
    if keyword_column:
        score += 80
    if group_column:
        score += 10
    if volume_column:
        score += 4
    score += min(len(date_columns), 12) * 7
    score += min(populated_rows, 10)
    return (score, -header_row_index, len(date_columns))


def _read_raw_frame(filename: str, payload: bytes) -> tuple[pd.DataFrame, str, str | None]:
    suffix = Path(filename).suffix.lower()
    if suffix == ".csv":
        frame = pd.read_csv(io.BytesIO(payload), header=None)
        return frame, "csv", None
    if suffix in {".xlsx", ".xls"}:
        sheets = pd.read_excel(io.BytesIO(payload), sheet_name=None, header=None)
        if not sheets:
            raise ValueError("File Excel không có sheet nào để đọc.")
        best_sheet_name = None
        best_frame = None
        best_score = (-1, 0, 0)
        for sheet_name, frame in sheets.items():
            score = _sheet_candidate_score(frame)
            if score > best_score:
                best_score = score
                best_sheet_name = sheet_name
                best_frame = frame
        if best_frame is None:
            best_sheet_name, best_frame = next(iter(sheets.items()))
        return best_frame, "excel", best_sheet_name
    raise ValueError("Định dạng file chưa được hỗ trợ.")


def _extract_summary_kpis(frame: pd.DataFrame, header_row_index: int) -> tuple[dict[str, int], dict[str, int]]:
    kpi_map: dict[str, int] = {}
    target_keyword_map: dict[str, int] = {}
    for row_index in range(header_row_index):
        row = frame.iloc[row_index].tolist()
        group_value = clean_text(row[0]) if row else ""
        if group_value:
            keyword_count = parse_search_volume(row[1]) if len(row) > 1 else None
            if keyword_count is not None:
                target_keyword_map[group_value] = keyword_count
        row_text = " ".join(clean_text(cell) for cell in row if clean_text(cell))
        target = parse_kpi_target(row_text)
        if target and group_value:
            kpi_map[group_value] = target
    return kpi_map, target_keyword_map


def parse_spreadsheet_payload(filename: str, payload: bytes, source_name: str | None = None) -> ParsedSheet:
    raw_frame, _, selected_sheet_name = _read_raw_frame(filename, payload)
    raw_frame = raw_frame.dropna(axis=1, how="all")
    header_row_index = detect_header_row(raw_frame)
    kpi_map, target_keyword_map = _extract_summary_kpis(raw_frame, header_row_index)

    header_values = raw_frame.iloc[header_row_index].tolist()
    headers = _make_unique_headers(header_values)
    frame = raw_frame.iloc[header_row_index + 1 :].copy()
    frame.columns = headers
    frame = frame.dropna(axis=0, how="all").reset_index(drop=True)

    keyword_column = _best_header_match(headers, KEYWORD_ALIASES)
    if not keyword_column:
        raise ValueError("Không tìm thấy cột Keyword trong file.")

    group_column = _best_header_match(headers, GROUP_ALIASES)
    cluster_column = _best_header_match(headers, CLUSTER_ALIASES)
    volume_column = _best_header_match(headers, VOLUME_ALIASES)
    best_column = _best_header_match(headers, BEST_ALIASES)
    kpi_column = _best_header_match(headers, KPI_ALIASES)
    target_url_column = _best_header_match(headers, TARGET_URL_ALIASES)
    found_url_column = _best_header_match(headers, FOUND_URL_ALIASES)
    date_columns = detect_date_columns(headers)
    if not date_columns:
        raise ValueError("Không phát hiện được cột ngày ranking.")

    rows: list[ParsedRankingRow] = []
    warnings: list[str] = []
    for _, record in frame.iterrows():
        keyword = clean_text(record.get(keyword_column))
        if not keyword:
            continue
        group_name = clean_text(record.get(group_column)) if group_column else "Chưa phân nhóm"
        cluster_name = clean_text(record.get(cluster_column)) if cluster_column else group_name
        search_volume = parse_search_volume(record.get(volume_column)) if volume_column else None
        row_best_rank = parse_rank_value(record.get(best_column)) if best_column else None
        row_kpi = parse_kpi_target(record.get(kpi_column)) if kpi_column else None
        kpi_target = row_kpi or kpi_map.get(group_name) or 10
        rankings: dict[str, float] = {}
        for column_name, iso_date in date_columns:
            rank = parse_rank_value(record.get(column_name))
            if rank is not None:
                rankings[iso_date] = rank
        if not rankings:
            warnings.append(f"Bỏ qua keyword '{keyword}' vì không có dữ liệu ngày.")
            continue
        best_rank = row_best_rank or min(rankings.values())
        rows.append(
            ParsedRankingRow(
                keyword=keyword,
                group_name=group_name or "Chưa phân nhóm",
                cluster_name=cluster_name or group_name or "Chưa phân nhóm",
                sub_cluster_name=infer_sub_cluster_name(keyword),
                target_url=clean_optional_text(record.get(target_url_column)) if target_url_column else None,
                found_url=clean_optional_text(record.get(found_url_column)) if found_url_column else None,
                search_volume=search_volume,
                best_rank=best_rank,
                kpi_target=kpi_target,
                rankings=rankings,
            )
        )

    return ParsedSheet(
        source_name=source_name or filename,
        header_row_index=header_row_index,
        selected_sheet_name=selected_sheet_name,
        kpi_map=kpi_map,
        target_keyword_map=target_keyword_map,
        rows=rows,
        dates=[item[1] for item in date_columns],
        warnings=warnings,
    )


def _extract_query_or_fragment_gid(sheet_url: str) -> str | None:
    parsed = urlparse(sheet_url)
    query = parse_qs(parsed.query)
    gid = query.get("gid", [None])[0]
    if gid:
        return gid
    fragment_match = re.search(r"gid=([0-9]+)", parsed.fragment or "")
    if fragment_match:
        return fragment_match.group(1)
    return None


def extract_google_sheet_identifiers(sheet_url: str, preferred_gid: str | None = None) -> tuple[str, str | None]:
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", sheet_url)
    if not match:
        raise ValueError("Google Sheet URL không hợp lệ.")
    sheet_id = match.group(1)
    gid = (preferred_gid or "").strip() or _extract_query_or_fragment_gid(sheet_url)
    return sheet_id, gid


def _is_google_sheet_url(source_url: str) -> bool:
    parsed = urlparse(source_url)
    return "docs.google.com" in parsed.netloc and "/spreadsheets/" in parsed.path


def _extract_google_drive_file_id(source_url: str) -> str | None:
    patterns = [
        r"/file/d/([a-zA-Z0-9_-]+)",
        r"[?&]id=([a-zA-Z0-9_-]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, source_url)
        if match:
            return match.group(1)
    return None


def _extension_from_content_type(content_type: str) -> str:
    normalized = content_type.lower()
    if "csv" in normalized:
        return ".csv"
    if "spreadsheetml" in normalized or "excel" in normalized:
        return ".xlsx"
    return ""


def _filename_from_response(url: str, response: requests.Response, fallback_stem: str) -> str:
    content_disposition = response.headers.get("content-disposition", "")
    filename_match = re.search(r'filename="?([^";]+)"?', content_disposition)
    if filename_match:
        filename = filename_match.group(1).strip()
        if Path(filename).suffix.lower() in {".csv", ".xlsx", ".xls"}:
            return filename
    parsed = urlparse(url)
    path_name = Path(parsed.path).name
    if Path(path_name).suffix.lower() in {".csv", ".xlsx", ".xls"}:
        return path_name
    extension = _extension_from_content_type(response.headers.get("content-type", ""))
    return f"{fallback_stem}{extension or '.xlsx'}"


def _download_bytes(url: str) -> tuple[bytes, str]:
    response = requests.get(url, timeout=30)
    if response.status_code >= 400:
        raise ValueError(f"Không tải được dữ liệu từ Google Sheets ({response.status_code}).")
    content_type = response.headers.get("content-type", "")
    return response.content, content_type


def fetch_google_sheet(sheet_url: str, preferred_gid: str | None = None) -> tuple[bytes, str, str | None]:
    sheet_id, gid = extract_google_sheet_identifiers(sheet_url, preferred_gid=preferred_gid)
    csv_candidates = []
    if gid:
        csv_candidates.append(
            f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
        )
        csv_candidates.append(
            f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&gid={gid}"
        )
    else:
        csv_candidates.append(
            f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0"
        )
        csv_candidates.append(
            f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&gid=0"
        )

    last_error: Exception | None = None
    for candidate in csv_candidates:
        try:
            payload, content_type = _download_bytes(candidate)
            if payload and "text/csv" in content_type:
                return payload, "google-sheet.csv", gid
        except Exception as exc:  # pragma: no cover - network dependent
            last_error = exc

    xlsx_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"
    try:
        payload, _ = _download_bytes(xlsx_url)
        return payload, "google-sheet.xlsx", gid
    except Exception as exc:  # pragma: no cover - network dependent
        raise ValueError(str(last_error or exc)) from exc


def fetch_public_data_source(source_url: str, preferred_gid: str | None = None) -> tuple[bytes, str, str | None, str]:
    normalized_url = source_url.strip()
    if not normalized_url:
        raise ValueError("Vui lòng nhập link dữ liệu public.")
    if _is_google_sheet_url(normalized_url):
        payload, filename, gid = fetch_google_sheet(normalized_url, preferred_gid=preferred_gid)
        return payload, filename, gid, "google_sheet"

    drive_file_id = _extract_google_drive_file_id(normalized_url)
    download_url = normalized_url
    fallback_stem = "remote-source"
    if drive_file_id and "drive.google.com" in normalized_url:
        download_url = f"https://drive.google.com/uc?export=download&id={drive_file_id}"
        fallback_stem = "google-drive-file"

    response = requests.get(download_url, timeout=45, allow_redirects=True)
    if response.status_code >= 400:
        raise ValueError(f"Không tải được dữ liệu từ link public ({response.status_code}).")
    filename = _filename_from_response(normalized_url, response, fallback_stem)
    suffix = Path(filename).suffix.lower()
    content_type = (response.headers.get("content-type") or "").lower()
    is_supported = suffix in {".csv", ".xlsx", ".xls"} or any(
        token in content_type for token in ("csv", "spreadsheetml", "excel")
    )
    if not is_supported:
        raise ValueError(
            "Link hiện chưa phải Google Sheet public hoặc file CSV/XLSX/XLS tải trực tiếp."
        )
    return response.content, filename, None, "public_link"
