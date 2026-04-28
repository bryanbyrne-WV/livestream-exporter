# -*- coding: utf-8 -*-
import csv
import io
import re
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, time as dt_time, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
import streamlit as st


# =========================================================
# APP CONFIG
# =========================================================
st.set_page_config(
    page_title="Workvivo Livestream Exporter",
    page_icon="🎥",
    layout="wide",
)

DEFAULT_API_BASE_URL = "https://api.workvivo.com/v1"
DEFAULT_TAKE = 100
DEFAULT_REQUEST_TIMEOUT = 60
DEFAULT_SLEEP_BETWEEN_REQUESTS = 0.2
DEFAULT_CHUNK_SIZE = 1024 * 256
DEFAULT_SPACE_BATCH_SIZE = 50

MANIFEST_COLUMNS = [
    "livestream_id",
    "title",
    "description",
    "host_name",
    "started_at",
    "ended_at",
    "created_at",
    "audience_type",
    "audience_names",
    "viewers_count",
    "recording_url",
    "resolved_playlist_url",
    "playlist_path",
    "media_playlist_path",
    "saved_path",
    "output_type",
    "segment_count",
    "status",
    "permalink",
    "source_scope",
]

DEFAULT_ADMIN_USERNAME = "admin"
DEFAULT_ADMIN_PASSWORD = "Cops123!"


# =========================================================
# HELPERS
# =========================================================
def get_secret(name: str, default: str = "") -> str:
    try:
        if name in st.secrets:
            return str(st.secrets[name])
    except Exception:
        pass
    return default


def sanitize_filename(filename: str) -> str:
    filename = (filename or "").strip().replace("\n", " ").replace("\r", " ")
    filename = re.sub(r'[<>:"/\\|?*]', "_", filename)
    return filename[:180] if filename else "livestream"


def iso_to_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def datetime_to_unix_seconds(value: datetime | None, end_of_day: bool = False) -> int | None:
    if value is None:
        return None

    if end_of_day:
        value = value.replace(hour=23, minute=59, second=59, microsecond=0)

    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)

    return int(value.timestamp())


def within_date_range(
    iso_value: str,
    date_from: datetime | None,
    date_to: datetime | None,
) -> bool:
    dt = iso_to_datetime(iso_value)
    if dt is None:
        return False

    dt_naive = dt.replace(tzinfo=None)

    if date_from and dt_naive < date_from:
        return False

    if date_to and dt_naive > date_to.replace(hour=23, minute=59, second=59):
        return False

    return True


def build_filename_base(livestream_id: str, title: str, timestamp: str) -> str:
    safe_title = sanitize_filename(title) or f"livestream_{livestream_id}"
    safe_timestamp = sanitize_filename((timestamp or "").replace(":", "-"))
    if safe_timestamp:
        return f"{livestream_id}_{safe_timestamp}_{safe_title}"
    return f"{livestream_id}_{safe_title}"


def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    return buffer.getvalue().encode("utf-8")


def get_next_page(payload: dict[str, Any]):
    return payload.get("meta", {}).get("pagination", {}).get("next_page")


def get_api_url_from_workvivo_id(wv_id: str) -> str:
    if not wv_id or len(str(wv_id).strip()) < 3:
        return DEFAULT_API_BASE_URL

    prefix = str(wv_id).strip()[:3]

    if prefix == "100":
        return "https://api.workvivo.us/v1"
    if prefix == "300":
        return "https://api.eu2.workvivo.com/v1"
    if prefix == "400":
        return "https://api.us2.workvivo.us/v1"

    return DEFAULT_API_BASE_URL


def chunk_list(values: list[str], size: int) -> list[list[str]]:
    if size <= 0:
        return [values]
    return [values[i:i + size] for i in range(0, len(values), size)]


# =========================================================
# DATA MODEL
# =========================================================
@dataclass
class ExportConfig:
    api_base_url: str
    api_token: str
    workvivo_id: str
    date_from: datetime | None
    date_to: datetime | None
    take: int = DEFAULT_TAKE
    request_timeout: int = DEFAULT_REQUEST_TIMEOUT
    sleep_between_requests: float = DEFAULT_SLEEP_BETWEEN_REQUESTS
    chunk_size: int = DEFAULT_CHUNK_SIZE
    force_manual_api_url: bool = False
    include_global_livestreams: bool = True
    include_space_livestreams: bool = True
    spaces_endpoint: str = "/spaces"
    space_batch_size: int = DEFAULT_SPACE_BATCH_SIZE


# =========================================================
# SESSION / REQUESTS
# =========================================================
def build_session(config: ExportConfig) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {config.api_token}",
            "Accept": "application/json",
            "Workvivo-id": config.workvivo_id,
            "User-Agent": "Mozilla/5.0",
        }
    )
    return session


def validate_config(config: ExportConfig) -> None:
    if not config.api_base_url:
        raise ValueError("Set API Base URL.")
    if not config.workvivo_id:
        raise ValueError("Set Workvivo ID.")
    if not config.api_token:
        raise ValueError("Set API token.")


# =========================================================
# LIVESTREAM HELPERS
# =========================================================
def get_recording_url(livestream: dict[str, Any]) -> str:
    video = livestream.get("video") or {}
    if isinstance(video, dict):
        return video.get("url") or ""
    return ""


def get_host_name(livestream: dict[str, Any]) -> str:
    host = livestream.get("host") or {}
    return host.get("display_name") or host.get("name") or ""


def get_audience_names(livestream: dict[str, Any]) -> str:
    audience = livestream.get("audience") or {}
    names: list[str] = []

    for space in audience.get("spaces", []):
        if isinstance(space, dict) and space.get("name"):
            names.append(space["name"])

    for team in audience.get("teams", []):
        if isinstance(team, dict) and team.get("name"):
            names.append(team["name"])

    return " | ".join(names)


def get_audience_type(livestream: dict[str, Any]) -> str:
    audience = livestream.get("audience") or {}
    if audience.get("is_global"):
        return "global"
    if audience.get("spaces"):
        return "spaces"
    if audience.get("teams"):
        return "teams"
    return "unknown"


def get_timestamp_for_filter(livestream: dict[str, Any]) -> str:
    return livestream.get("started_at") or livestream.get("created_at") or ""


def matches_filters(livestream: dict[str, Any], config: ExportConfig) -> bool:
    if not livestream.get("is_recorded"):
        return False

    if livestream.get("recording_status") != "done":
        return False

    timestamp = get_timestamp_for_filter(livestream)
    if not timestamp:
        return False

    return within_date_range(timestamp, config.date_from, config.date_to)


def livestream_to_manifest_row(
    livestream: dict[str, Any],
    source_scope: str = "global",
) -> dict[str, Any]:
    return {
        "livestream_id": str(livestream.get("id", "")),
        "title": livestream.get("title") or "",
        "description": livestream.get("description") or "",
        "host_name": get_host_name(livestream),
        "started_at": livestream.get("started_at") or "",
        "ended_at": livestream.get("ended_at") or "",
        "created_at": livestream.get("created_at") or "",
        "audience_type": get_audience_type(livestream),
        "audience_names": get_audience_names(livestream),
        "viewers_count": livestream.get("viewers_count", ""),
        "recording_url": get_recording_url(livestream),
        "resolved_playlist_url": "",
        "playlist_path": "",
        "media_playlist_path": "",
        "saved_path": "",
        "output_type": "",
        "segment_count": "",
        "status": "pending",
        "permalink": livestream.get("permalink", ""),
        "source_scope": source_scope,
    }


# =========================================================
# HLS / DOWNLOAD HELPERS
# =========================================================
def is_m3u8_url(url: str) -> bool:
    return ".m3u8" in (url or "").lower()


def fetch_text(session: requests.Session, url: str, timeout: int) -> str:
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    return response.text


def download_binary(
    session: requests.Session,
    url: str,
    destination: Path,
    timeout: int,
    chunk_size: int,
) -> None:
    with session.get(url, stream=True, timeout=timeout) as response:
        response.raise_for_status()
        with open(destination, "wb") as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)


def save_text_file(destination: Path, content: str) -> None:
    with open(destination, "w", encoding="utf-8") as f:
        f.write(content)


def parse_m3u8_lines(content: str) -> list[str]:
    return [line.strip() for line in content.splitlines() if line.strip()]


def is_master_playlist(lines: list[str]) -> bool:
    return any(line.startswith("#EXT-X-STREAM-INF") for line in lines)


def resolve_playlist_target(base_url: str, line: str) -> str:
    return urljoin(base_url, line)


def get_variant_playlist_url(playlist_url: str, content: str) -> str:
    lines = parse_m3u8_lines(content)

    if not is_master_playlist(lines):
        return playlist_url

    for i, line in enumerate(lines):
        if line.startswith("#EXT-X-STREAM-INF") and i + 1 < len(lines):
            next_line = lines[i + 1]
            if not next_line.startswith("#"):
                return resolve_playlist_target(playlist_url, next_line)

    return playlist_url


def get_media_segment_urls(playlist_url: str, content: str) -> list[str]:
    lines = parse_m3u8_lines(content)
    segment_urls: list[str] = []

    for line in lines:
        if line.startswith("#"):
            continue
        segment_urls.append(resolve_playlist_target(playlist_url, line))

    return segment_urls


def guess_segment_extension(segment_urls: list[str]) -> str:
    if not segment_urls:
        return ".bin"

    first_path = urlparse(segment_urls[0]).path.lower()

    if first_path.endswith(".ts"):
        return ".ts"
    if first_path.endswith(".m4s"):
        return ".m4s"
    if first_path.endswith(".mp4"):
        return ".mp4"

    suffix = Path(first_path).suffix
    return suffix if suffix else ".bin"


def export_hls_assets(
    session: requests.Session,
    recording_url: str,
    file_base: str,
    export_folder: Path,
    timeout: int,
    chunk_size: int,
    progress_callback=None,
) -> dict[str, Any]:
    first_playlist = fetch_text(session, recording_url, timeout)
    first_playlist_path = export_folder / f"{file_base}_master.m3u8"
    save_text_file(first_playlist_path, first_playlist)

    media_playlist_url = get_variant_playlist_url(recording_url, first_playlist)

    if media_playlist_url != recording_url:
        media_playlist = fetch_text(session, media_playlist_url, timeout)
        media_playlist_path = export_folder / f"{file_base}_media.m3u8"
        save_text_file(media_playlist_path, media_playlist)
    else:
        media_playlist = first_playlist
        media_playlist_path = first_playlist_path

    segment_urls = get_media_segment_urls(media_playlist_url, media_playlist)
    if not segment_urls:
        raise ValueError("No media segments found in playlist.")

    segment_ext = guess_segment_extension(segment_urls)
    destination = export_folder / f"{file_base}{segment_ext}"

    with open(destination, "wb") as outfile:
        total = len(segment_urls)

        for index, segment_url in enumerate(segment_urls, start=1):
            with session.get(segment_url, stream=True, timeout=timeout) as response:
                response.raise_for_status()

                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        outfile.write(chunk)

            if progress_callback:
                progress_callback(index, total)

    return {
        "saved_path": str(destination),
        "output_type": segment_ext.lstrip("."),
        "segment_count": len(segment_urls),
        "playlist_path": str(first_playlist_path),
        "media_playlist_path": str(media_playlist_path),
        "playlist_url": media_playlist_url,
    }


# =========================================================
# API CALLS
# =========================================================
def fetch_json(
    session: requests.Session,
    url: str,
    params: dict[str, Any] | None,
    timeout: int,
) -> dict[str, Any]:
    response = session.get(url, params=params, timeout=timeout)

    if not response.ok:
        raise RuntimeError(
            f"Request failed with status {response.status_code}. "
            f"URL: {response.url}. "
            f"Body: {response.text}"
        )

    return response.json()


def build_livestream_params(
    config: ExportConfig,
    skip: int,
    take: int,
    in_spaces: str | None = None,
    is_global: bool | None = None,
    is_recorded: bool | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "skip": skip,
        "take": take,
    }

    unix_from = datetime_to_unix_seconds(config.date_from, end_of_day=False)
    unix_to = datetime_to_unix_seconds(config.date_to, end_of_day=True)

    if unix_from is not None:
        params["from"] = unix_from

    if unix_to is not None:
        params["to"] = unix_to

    if in_spaces:
        params["in_spaces"] = in_spaces

    if is_global is not None:
        params["is_global"] = str(is_global).lower()

    if is_recorded is not None:
        params["is_recorded"] = str(is_recorded).lower()

    return params


def fetch_livestreams_page(
    session: requests.Session,
    config: ExportConfig,
    skip: int,
    take: int,
    in_spaces: str | None = None,
    is_global: bool | None = None,
    is_recorded: bool | None = None,
) -> dict[str, Any]:
    url = f"{config.api_base_url.rstrip('/')}/livestreams"
    params = build_livestream_params(
        config=config,
        skip=skip,
        take=take,
        in_spaces=in_spaces,
        is_global=is_global,
        is_recorded=is_recorded,
    )
    return fetch_json(session, url, params, config.request_timeout)


def fetch_spaces_page(
    session: requests.Session,
    config: ExportConfig,
    skip: int,
    take: int,
) -> dict[str, Any]:
    url = f"{config.api_base_url.rstrip('/')}{config.spaces_endpoint}"
    params = {"skip": skip, "take": take}
    return fetch_json(session, url, params, config.request_timeout)


def extract_spaces_list(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = payload.get("data", [])
    return data if isinstance(data, list) else []


def extract_livestreams_list(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = payload.get("data", [])
    return data if isinstance(data, list) else []


def collect_all_spaces(
    session: requests.Session,
    config: ExportConfig,
    status_box,
    progress_bar,
) -> list[dict[str, Any]]:
    skip = 0
    collected: list[dict[str, Any]] = []
    page_number = 0

    while True:
        page_number += 1
        payload = fetch_spaces_page(session, config, skip=skip, take=config.take)
        spaces = extract_spaces_list(payload)

        if not spaces:
            break

        collected.extend(spaces)
        status_box.info(
            f"Fetched spaces page {page_number}: {len(spaces)} spaces "
            f"(total {len(collected)})"
        )

        next_page = get_next_page(payload)
        if next_page is None:
            break

        skip += config.take
        progress_bar.progress(min(0.25 + (page_number * 0.02), 0.40))
        time.sleep(config.sleep_between_requests)

    return collected


def collect_livestreams_by_query(
    session: requests.Session,
    config: ExportConfig,
    status_box,
    progress_bar,
    label: str,
    in_spaces: str | None = None,
    is_global: bool | None = None,
    progress_start: float = 0.0,
    progress_end: float = 0.0,
) -> list[dict[str, Any]]:
    skip = 0
    page_number = 0
    collected: list[dict[str, Any]] = []

    while True:
        page_number += 1
        payload = fetch_livestreams_page(
            session=session,
            config=config,
            skip=skip,
            take=config.take,
            in_spaces=in_spaces,
            is_global=is_global,
            is_recorded=True,
        )

        livestreams = extract_livestreams_list(payload)

        if not livestreams:
            break

        collected.extend(livestreams)
        status_box.info(
            f"Fetched {label} page {page_number}: {len(livestreams)} livestreams "
            f"(total {len(collected)})"
        )

        next_page = get_next_page(payload)
        if next_page is None:
            break

        skip += config.take

        if progress_end > progress_start:
            progress_bar.progress(min(progress_start + (page_number * 0.03), progress_end))

        time.sleep(config.sleep_between_requests)

    return collected


def deduplicate_livestreams(livestreams: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: dict[str, dict[str, Any]] = {}

    for item in livestreams:
        livestream_id = str(item.get("id", "")).strip()

        if not livestream_id:
            continue

        existing = seen.get(livestream_id)

        if existing is None:
            seen[livestream_id] = item
            continue

        existing_scope = existing.get("_source_scope", "global")
        new_scope = item.get("_source_scope", "global")

        if existing_scope == "global" and new_scope == "spaces":
            seen[livestream_id] = item

    return list(seen.values())


def collect_all_livestreams(
    session: requests.Session,
    config: ExportConfig,
    status_box,
    progress_bar,
) -> tuple[list[dict[str, Any]], list[str]]:
    collected: list[dict[str, Any]] = []
    warnings: list[str] = []

    if config.include_global_livestreams:
        try:
            global_items = collect_livestreams_by_query(
                session=session,
                config=config,
                status_box=status_box,
                progress_bar=progress_bar,
                label="global livestreams",
                is_global=True,
                progress_start=0.05,
                progress_end=0.25,
            )

            for item in global_items:
                enriched = dict(item)
                enriched["_source_scope"] = "global"
                collected.append(enriched)

        except Exception as exc:
            warnings.append(f"Global livestream fetch failed: {exc}")

    if config.include_space_livestreams:
        try:
            spaces = collect_all_spaces(
                session=session,
                config=config,
                status_box=status_box,
                progress_bar=progress_bar,
            )

            space_ids = [
                str(space.get("id")).strip()
                for space in spaces
                if str(space.get("id", "")).strip()
            ]

            if not space_ids:
                warnings.append("No spaces were returned, so no space livestreams could be queried.")
            else:
                space_batches = chunk_list(space_ids, config.space_batch_size)
                total_batches = max(len(space_batches), 1)

                for batch_index, batch_ids in enumerate(space_batches, start=1):
                    in_spaces = "|".join(batch_ids)

                    batch_items = collect_livestreams_by_query(
                        session=session,
                        config=config,
                        status_box=status_box,
                        progress_bar=progress_bar,
                        label=f"space livestreams batch {batch_index}/{len(space_batches)}",
                        in_spaces=in_spaces,
                        progress_start=0.40 + ((batch_index - 1) / total_batches) * 0.40,
                        progress_end=0.40 + (batch_index / total_batches) * 0.40,
                    )

                    for item in batch_items:
                        enriched = dict(item)
                        enriched["_source_scope"] = "spaces"
                        collected.append(enriched)

        except Exception as exc:
            warnings.append(f"Space livestream fetch failed: {exc}")

    deduped = deduplicate_livestreams(collected)
    progress_bar.progress(0.95)

    return deduped, warnings


def test_connection(session: requests.Session, config: ExportConfig) -> tuple[bool, str]:
    messages: list[str] = []

    try:
        payload = fetch_livestreams_page(
            session=session,
            config=config,
            skip=0,
            take=1,
            is_recorded=True,
        )
        count = len(payload.get("data", []))
        messages.append(f"Livestream endpoint OK ({count} row(s) returned).")
    except Exception as exc:
        messages.append(f"Livestream endpoint failed: {exc}")

    if config.include_space_livestreams:
        try:
            payload = fetch_spaces_page(session, config, skip=0, take=1)
            count = len(payload.get("data", []))
            messages.append(f"Spaces endpoint OK ({count} row(s) returned).")
        except Exception as exc:
            messages.append(f"Spaces endpoint failed: {exc}")

    ok = any("OK" in msg for msg in messages)
    prefix = "Success" if ok else "Error"

    return ok, f"{prefix}: " + " ".join(messages)


# =========================================================
# ZIP EXPORT
# =========================================================
def export_selected_livestreams_to_zip(
    session: requests.Session,
    config: ExportConfig,
    selected_rows: list[dict[str, Any]],
    status_box,
    progress_bar,
) -> tuple[list[dict[str, Any]], str]:
    results: list[dict[str, Any]] = []

    if not selected_rows:
        raise ValueError("No rows selected for export.")

    zip_path = Path("/tmp") / (
        f"livestream_export_{config.workvivo_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
    )

    with TemporaryDirectory() as temp_dir:
        export_root = Path(temp_dir) / f"Exported_Livestreams_{config.workvivo_id}"
        export_root.mkdir(parents=True, exist_ok=True)

        total = len(selected_rows)

        for item_index, row in enumerate(selected_rows, start=1):
            livestream_id = row["livestream_id"]
            title = row["title"]
            recording_url = row["recording_url"]
            timestamp = row["started_at"] or row["created_at"]

            status_box.info(f"Exporting {item_index}/{total}: {title or livestream_id}")
            row = dict(row)

            if not recording_url:
                row["status"] = "matched but no recording URL found"
                results.append(row)
                progress_bar.progress(item_index / total)
                continue

            file_base = build_filename_base(
                livestream_id=livestream_id,
                title=title,
                timestamp=timestamp,
            )

            try:
                if is_m3u8_url(recording_url):

                    def segment_progress(index: int, segment_total: int):
                        segment_fraction = index / max(segment_total, 1)
                        overall = ((item_index - 1) + segment_fraction) / total
                        progress_bar.progress(min(overall, 1.0))

                    export_info = export_hls_assets(
                        session=session,
                        recording_url=recording_url,
                        file_base=file_base,
                        export_folder=export_root,
                        timeout=config.request_timeout,
                        chunk_size=config.chunk_size,
                        progress_callback=segment_progress,
                    )

                    row["saved_path"] = str(Path(export_info["saved_path"]).name)
                    row["output_type"] = export_info["output_type"]
                    row["segment_count"] = export_info["segment_count"]
                    row["playlist_path"] = str(Path(export_info["playlist_path"]).name)
                    row["media_playlist_path"] = str(Path(export_info["media_playlist_path"]).name)
                    row["resolved_playlist_url"] = export_info["playlist_url"]
                    row["status"] = f"hls merged to {row['output_type']} and m3u8 saved"

                else:
                    ext = Path(urlparse(recording_url).path).suffix or ".mp4"
                    destination = export_root / f"{file_base}{ext}"

                    download_binary(
                        session=session,
                        url=recording_url,
                        destination=destination,
                        timeout=config.request_timeout,
                        chunk_size=config.chunk_size,
                    )

                    row["saved_path"] = str(destination.name)
                    row["output_type"] = ext.lstrip(".")
                    row["status"] = f"file downloaded as {row['output_type']}"
                    progress_bar.progress(item_index / total)

            except Exception as exc:
                row["status"] = f"failed: {exc}"
                progress_bar.progress(item_index / total)

            results.append(row)

        results_df = pd.DataFrame(results, columns=MANIFEST_COLUMNS)
        manifest_path = export_root / f"livestream_export_manifest_{config.workvivo_id}.csv"
        results_df.to_csv(manifest_path, index=False, quoting=csv.QUOTE_MINIMAL)

        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for file_path in export_root.rglob("*"):
                if file_path.is_file():
                    arcname = file_path.relative_to(export_root.parent)
                    zf.write(file_path, arcname=str(arcname))

    return results, str(zip_path)


# =========================================================
# STATE
# =========================================================
def init_state():
    st.session_state.setdefault("authenticated", False)
    st.session_state.setdefault("fetched_rows", [])
    st.session_state.setdefault("export_results", [])
    st.session_state.setdefault("last_fetch_count", 0)
    st.session_state.setdefault("export_zip_path", "")
    st.session_state.setdefault("export_zip_name", "")
    st.session_state.setdefault("config_test_passed", False)
    st.session_state.setdefault("fetch_warnings", [])
    st.session_state.setdefault("selection_mode", "none")
    st.session_state.setdefault("selection_version", 0)


# =========================================================
# STYLING
# =========================================================
def apply_global_branding():
    st.markdown(
        """
        <style>
            .stApp {
                background: linear-gradient(
                    180deg,
                    #F8F5FF 0%,
                    #F0EAFF 28%,
                    #EAF2FF 75%,
                    #FCFDFF 100%
                );
            }

            section[data-testid="stSidebar"] {
                background: rgba(255,255,255,0.88);
                backdrop-filter: blur(6px);
                border-right: 1px solid rgba(90, 62, 166, 0.08);
            }

            .main-title {
                font-size: 2.2rem;
                color: #5A3EA6;
                font-weight: 800;
                margin-bottom: 0.25rem;
            }

            .main-subtitle {
                font-size: 1rem;
                color: #6B56B0;
                opacity: 0.9;
                margin-bottom: 1.2rem;
            }

            .download-anchor {
                padding: 1rem 1rem 0.4rem 1rem;
                border-radius: 14px;
                background: rgba(255,255,255,0.72);
                border: 1px solid rgba(60,79,168,0.08);
                margin-bottom: 0.8rem;
            }

            section[data-testid="stSidebar"] div[data-testid="stTextInput"] input,
            section[data-testid="stSidebar"] div[data-testid="stNumberInput"] input,
            section[data-testid="stSidebar"] div[data-testid="stDateInput"] input {
                background: #FAFBFF !important;
                border: 1px solid #E3E8F4 !important;
                border-radius: 10px !important;
                box-shadow: 0 1px 2px rgba(16, 24, 40, 0.04) !important;
            }

            section[data-testid="stSidebar"] div[data-testid="stTextInput"] input:focus,
            section[data-testid="stSidebar"] div[data-testid="stNumberInput"] input:focus,
            section[data-testid="stSidebar"] div[data-testid="stDateInput"] input:focus {
                border: 1px solid #B7C3E0 !important;
                box-shadow: 0 0 0 2px rgba(90, 62, 166, 0.08) !important;
            }

            section[data-testid="stSidebar"] div[data-testid="stTextInput"] input::placeholder {
                color: #98A2B3 !important;
                opacity: 1 !important;
            }

            section[data-testid="stSidebar"] .stButton > button {
                border-radius: 10px !important;
                border: 1px solid rgba(60,79,168,0.10) !important;
                box-shadow: 0 1px 2px rgba(16, 24, 40, 0.04) !important;
            }

            section[data-testid="stSidebar"] details {
                background: transparent !important;
                border: none !important;
            }

            section[data-testid="stSidebar"] details summary {
                border-radius: 10px !important;
                border: 1px solid rgba(60,79,168,0.10) !important;
                background: #FAFBFF !important;
                padding: 0.35rem 0.6rem !important;
            }

            section[data-testid="stSidebar"] details[open] summary {
                margin-bottom: 0.5rem !important;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_login_screen():
    admin_username = get_secret("APP_ADMIN_USERNAME", DEFAULT_ADMIN_USERNAME)
    admin_password = get_secret("APP_ADMIN_PASSWORD", DEFAULT_ADMIN_PASSWORD)

    st.markdown(
        """
        <style>
            .stApp {
                background: linear-gradient(
                    180deg,
                    #EFE8FF 0%,
                    #E4D9FF 30%,
                    #DBEFFF 80%,
                    #F9FCFF 100%
                );
            }

            .login-wrapper {
                max-width: 420px;
                margin: 1.5rem auto 2rem auto;
            }

            .login-title {
                font-size: 2rem;
                color: #5A3EA6;
                font-weight: 700;
                margin-bottom: 0.4rem;
                margin-top: 1rem;
                text-align: center;
            }

            .login-note {
                font-size: 1.05rem;
                color: #6B56B0;
                opacity: 0.8;
                margin-bottom: 2rem;
                text-align: center;
            }

            .underline-input input {
                background: transparent !important;
                border: none !important;
                border-bottom: 1px solid #8368D8 !important;
                border-radius: 0 !important;
                color: #4A2F8A !important;
                padding: 0.55rem 0 !important;
                font-size: 1rem !important;
                box-shadow: none !important;
            }

            .underline-input input::placeholder {
                color: #9A84DD !important;
                opacity: 0.6;
            }

            .blue-btn button {
                background-color: #3C4FA8 !important;
                color: white !important;
                border-radius: 8px !important;
                height: 2.7rem;
                font-weight: 600;
                letter-spacing: 0.5px;
                border: none !important;
                margin-top: 1.2rem;
            }

            .request-button {
                display: inline-block;
                margin-top: 1.4rem;
                font-size: 0.95rem;
                color: #3C4FA8 !important;
                text-decoration: underline;
                opacity: 0.85;
            }

            div[data-testid="stTextInput"] label p {
                color: #3f3f46 !important;
                font-weight: 500;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="login-wrapper">', unsafe_allow_html=True)

    st.markdown(
        """
        <div style="text-align:center; margin-bottom:10px;">
            <img src="https://d3lkrqe5vfp7un.cloudfront.net/images/Picture4.png" style="height:170px;">
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="login-title">User Login</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="login-note">Please sign in to access the Livestream Export Tool</div>',
        unsafe_allow_html=True,
    )

    form_left, form_mid, form_right = st.columns([1.2, 2.2, 1.2])

    with form_mid:
        st.markdown('<div class="underline-input">', unsafe_allow_html=True)
        username = st.text_input("Username", placeholder="Username", key="login_username")
        password = st.text_input("Password", placeholder="Password", type="password", key="login_password")
        st.markdown('</div>', unsafe_allow_html=True)

        st.checkbox("Remember me", disabled=True, key="remember_me")

        st.markdown('<div class="blue-btn">', unsafe_allow_html=True)
        login_button = st.button("LOGIN", width="stretch")
        st.markdown('</div>', unsafe_allow_html=True)

        if login_button:
            if username == admin_username and password == admin_password:
                st.session_state.authenticated = True
                st.success("Logged in!")
                st.rerun()
            else:
                st.error("❌ Invalid username or password.")

        st.markdown(
            """
            <div style="text-align:center;">
                <a class="request-button"
                   href="https://support.workvivo.com/hc/en-gb/requests/new"
                   target="_blank">
                    Request Access
                </a>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown('</div>', unsafe_allow_html=True)


# =========================================================
# UI
# =========================================================
def sidebar_config() -> tuple[ExportConfig, bool, Any]:
    st.sidebar.header("Connection")

    workvivo_id = st.sidebar.text_input(
        "Workvivo ID",
        value="",
        placeholder="",
    )

    auto_detect = st.sidebar.checkbox(
        "Auto-detect API URL from Workvivo ID",
        value=True,
    )

    detected_api_url = get_api_url_from_workvivo_id(workvivo_id) if workvivo_id else ""

    if auto_detect:
        api_base_url = st.sidebar.text_input(
            "API Base URL",
            value=detected_api_url if detected_api_url else "",
            disabled=True,
            help="Auto-detected from Workvivo ID prefix.",
        )
    else:
        api_base_url = st.sidebar.text_input(
            "API Base URL",
            value="",
            placeholder="https://api.workvivo.com/v1",
            help="Example: https://api.workvivo.com/v1",
        )

    api_token = st.sidebar.text_input(
        "API token",
        type="password",
        value=get_secret("WORKVIVO_API_TOKEN", ""),
        placeholder="",
        help="API key created in Workvivo.",
    )

    test_clicked = st.sidebar.button("Test connection", width="stretch")
    test_result = st.sidebar.empty()

    st.sidebar.header("Filter")

    use_date_from = st.sidebar.checkbox("Use Date from")
    date_from_value = st.sidebar.date_input("Date from", value=None, disabled=not use_date_from)

    use_date_to = st.sidebar.checkbox("Use Date to")
    date_to_value = st.sidebar.date_input("Date to", value=None, disabled=not use_date_to)

    st.sidebar.header("Scope")
    include_global_livestreams = st.sidebar.checkbox("Include global livestreams", value=True)
    include_space_livestreams = st.sidebar.checkbox("Include space livestreams", value=True)

    with st.sidebar.expander("Advanced", expanded=False):
        take = st.number_input("Page size", min_value=1, max_value=500, value=100, step=1)

        request_timeout = st.number_input(
            "Request timeout (seconds)",
            min_value=5,
            max_value=600,
            value=60,
            step=5,
        )

        sleep_between_requests = st.number_input(
            "Delay between API requests (seconds)",
            min_value=0.0,
            max_value=5.0,
            value=0.2,
            step=0.1,
        )

        spaces_endpoint = st.text_input(
            "Spaces endpoint",
            value="/spaces",
            help="Default is /spaces",
        )

        space_batch_size = st.number_input(
            "Space IDs per in_spaces batch",
            min_value=1,
            max_value=500,
            value=50,
            step=1,
            help="Builds pipe-separated in_spaces values in batches.",
        )

    date_from = None
    date_to = None

    if use_date_from and date_from_value:
        date_from = datetime.combine(date_from_value, dt_time.min)

    if use_date_to and date_to_value:
        date_to = datetime.combine(date_to_value, dt_time.min)

    config = ExportConfig(
        api_base_url=api_base_url.strip().rstrip("/"),
        api_token=api_token.strip(),
        workvivo_id=workvivo_id.strip(),
        date_from=date_from,
        date_to=date_to,
        take=int(take),
        request_timeout=int(request_timeout),
        sleep_between_requests=float(sleep_between_requests),
        force_manual_api_url=not auto_detect,
        include_global_livestreams=include_global_livestreams,
        include_space_livestreams=include_space_livestreams,
        spaces_endpoint=(spaces_endpoint.strip() or "/spaces"),
        space_batch_size=int(space_batch_size),
    )

    return config, test_clicked, test_result


def render_header(config: ExportConfig):
    st.markdown(
        '<div class="main-title">🎥 Workvivo Livestream Exporter</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        '<div class="main-subtitle">Fetch recorded livestreams, review them, and download a ZIP export bundle.</div>',
        unsafe_allow_html=True,
    )

    with st.expander("Current configuration", expanded=False):
        st.write(f"**Workvivo ID:** `{config.workvivo_id or 'None'}`")
        st.write(f"**API Base URL:** `{config.api_base_url or 'None'}`")
        st.write(f"**Date from:** `{config.date_from.date() if config.date_from else 'None'}`")
        st.write(f"**Date to:** `{config.date_to.date() if config.date_to else 'None'}`")
        st.write(f"**Include global livestreams:** `{config.include_global_livestreams}`")
        st.write(f"**Include space livestreams:** `{config.include_space_livestreams}`")
        st.write(f"**Spaces endpoint:** `{config.spaces_endpoint}`")
        st.write(f"**Space IDs per batch:** `{config.space_batch_size}`")


def render_summary(rows: list[dict[str, Any]], exported_rows: list[dict[str, Any]]):
    matched = len(rows)

    exported_ok = sum(
        1 for row in exported_rows
        if str(row.get("status", "")).startswith(("hls merged", "file downloaded"))
    )

    failed = sum(
        1 for row in exported_rows
        if str(row.get("status", "")).startswith("failed:")
    )

    c1, c2, c3 = st.columns(3)
    c1.metric("Matched recorded livestreams", matched)
    c2.metric("Successfully exported", exported_ok)
    c3.metric("Failed exports", failed)


def main_app():
    apply_global_branding()

    config, test_clicked, test_result = sidebar_config()
    render_header(config)

    status_box = st.empty()
    progress_bar = st.progress(0.0)

    if test_clicked:
        try:
            validate_config(config)
            session = build_session(config)
            ok, message = test_connection(session, config)

            if ok:
                test_result.success(message)
                st.session_state.config_test_passed = True
            else:
                test_result.error(message)
                st.session_state.config_test_passed = False

        except Exception as exc:
            test_result.error(f"Error: connection failed. {exc}")
            st.session_state.config_test_passed = False

    col_left, col_right = st.columns([1, 1])

    fetch_clicked = col_left.button("Fetch livestreams", width="stretch")
    export_clicked = col_right.button("Export livestreams", width="stretch")

    if fetch_clicked:
        try:
            validate_config(config)
            session = build_session(config)

            all_livestreams, warnings = collect_all_livestreams(
                session,
                config,
                status_box,
                progress_bar,
            )

            filtered_rows = [
                livestream_to_manifest_row(
                    livestream,
                    source_scope=livestream.get("_source_scope", "global"),
                )
                for livestream in all_livestreams
                if matches_filters(livestream, config)
            ]

            st.session_state["fetched_rows"] = filtered_rows
            st.session_state["last_fetch_count"] = len(all_livestreams)
            st.session_state["export_results"] = []
            st.session_state["export_zip_path"] = ""
            st.session_state["export_zip_name"] = ""
            st.session_state["fetch_warnings"] = warnings
            st.session_state["selection_mode"] = "none"
            st.session_state["selection_version"] += 1

            progress_bar.progress(1.0)

            status_box.success(
                f"Fetched {len(all_livestreams)} unique livestreams. "
                f"{len(filtered_rows)} matched the recorded/date filters."
            )

        except Exception as exc:
            status_box.error(str(exc))
            st.exception(exc)

    rows = st.session_state.get("fetched_rows", [])
    export_results = st.session_state.get("export_results", [])
    fetch_warnings = st.session_state.get("fetch_warnings", [])

    if fetch_warnings:
        with st.expander("Fetch warnings", expanded=False):
            for warning in fetch_warnings:
                st.warning(warning)

    if rows:
        render_summary(rows, export_results)

        st.subheader("Matched livestreams")

        st.write(
            f"Total unique livestreams fetched: **{st.session_state.get('last_fetch_count', 0)}**  \n"
            f"Matched recorded livestreams: **{len(rows)}**"
        )

        action_col1, action_col2, action_col3 = st.columns([1, 1, 6])

        with action_col1:
            if st.button("Select all", width="stretch"):
                st.session_state["selection_mode"] = "all"
                st.session_state["selection_version"] += 1

        with action_col2:
            if st.button("Deselect all", width="stretch"):
                st.session_state["selection_mode"] = "none"
                st.session_state["selection_version"] += 1

        selection_mode = st.session_state.get("selection_mode", "none")
        default_selected = selection_mode == "all"

        df = pd.DataFrame(rows).copy()
        df.insert(0, "selected", default_selected)

        editor_key = f"livestream_editor_{st.session_state.get('selection_version', 0)}"

        edited_df = st.data_editor(
            df,
            width="stretch",
            hide_index=True,
            disabled=[col for col in df.columns if col != "selected"],
            column_config={
                "selected": st.column_config.CheckboxColumn(
                    "Export",
                    help="Tick rows to include in the ZIP.",
                    default=False,
                ),
                "recording_url": st.column_config.TextColumn(width="medium"),
                "permalink": st.column_config.TextColumn(width="medium"),
                "description": st.column_config.TextColumn(width="large"),
                "source_scope": st.column_config.TextColumn("Source Scope", width="small"),
            },
            key=editor_key,
        )

        selected_rows = (
            edited_df[edited_df["selected"]]
            .drop(columns=["selected"])
            .to_dict(orient="records")
        )

        st.caption(f"Selected for export: {len(selected_rows)}")

        manifest_csv = dataframe_to_csv_bytes(pd.DataFrame(rows, columns=MANIFEST_COLUMNS))

        st.download_button(
            label="Download matched manifest CSV",
            data=manifest_csv,
            file_name=f"livestream_export_manifest_{config.workvivo_id or 'tenant'}.csv",
            mime="text/csv",
        )

        if export_clicked:
            try:
                validate_config(config)
                session = build_session(config)

                progress_bar.progress(0.0)

                results, zip_path = export_selected_livestreams_to_zip(
                    session=session,
                    config=config,
                    selected_rows=selected_rows,
                    status_box=status_box,
                    progress_bar=progress_bar,
                )

                st.session_state["export_results"] = results
                st.session_state["export_zip_path"] = zip_path
                st.session_state["export_zip_name"] = Path(zip_path).name

                status_box.success(
                    f"Success: {len(results)} row(s) processed. "
                    f"Scroll down to download your ZIP export."
                )

            except Exception as exc:
                status_box.error(str(exc))
                st.exception(exc)

    if st.session_state.get("export_results"):
        st.subheader("Export results")

        results_df = pd.DataFrame(st.session_state["export_results"], columns=MANIFEST_COLUMNS)

        st.dataframe(
            results_df,
            width="stretch",
            hide_index=True,
        )

        results_csv = dataframe_to_csv_bytes(results_df)

        st.download_button(
            label="Download export results CSV",
            data=results_csv,
            file_name=f"livestream_export_results_{config.workvivo_id or 'tenant'}.csv",
            mime="text/csv",
        )

    if st.session_state.get("export_zip_path"):
        st.markdown(
            """
            <div class="download-anchor">
                <h3 style="margin-bottom: 0.2rem; color: #5A3EA6;">Your export is ready</h3>
                <p style="margin-top: 0; color: #6B56B0;">Download the ZIP bundle below.</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

        zip_path = Path(st.session_state["export_zip_path"])

        if zip_path.exists():
            with open(zip_path, "rb") as zip_file:
                st.download_button(
                    label="Download selected to ZIP",
                    data=zip_file,
                    file_name=st.session_state["export_zip_name"],
                    mime="application/zip",
                    width="stretch",
                )
        else:
            st.error("ZIP file is no longer available. Please run the export again.")


# =========================================================
# ENTRY
# =========================================================
def main():
    init_state()

    if not st.session_state.authenticated:
        render_login_screen()
        return

    main_app()


if __name__ == "__main__":
    main()
