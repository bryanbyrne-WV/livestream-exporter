import csv
import io
import re
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
import streamlit as st


# =========================
# APP CONFIG
# =========================
st.set_page_config(
    page_title="Workvivo Livestream Exporter",
    page_icon="🎥",
    layout="wide",
)

API_BASE_URL = "https://api.workvivo.com/v1"
DEFAULT_TAKE = 100
DEFAULT_REQUEST_TIMEOUT = 60
DEFAULT_SLEEP_BETWEEN_REQUESTS = 0.2
DEFAULT_CHUNK_SIZE = 1024 * 256
DEFAULT_EXPORT_ROOT = Path.home() / "Downloads"

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
]


# =========================
# DATA MODELS
# =========================
@dataclass
class ExportConfig:
    api_token: str
    workvivo_id: str
    date_from: datetime | None
    date_to: datetime | None
    take: int = DEFAULT_TAKE
    request_timeout: int = DEFAULT_REQUEST_TIMEOUT
    sleep_between_requests: float = DEFAULT_SLEEP_BETWEEN_REQUESTS
    chunk_size: int = DEFAULT_CHUNK_SIZE
    export_folder: Path = DEFAULT_EXPORT_ROOT

    @property
    def export_path(self) -> Path:
        return self.export_folder / f"Exported_Livestreams_{self.workvivo_id}"

    @property
    def csv_path(self) -> Path:
        return self.export_path / f"livestream_export_manifest_{self.workvivo_id}.csv"


# =========================
# SESSION + HELPERS
# =========================
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
    if not config.workvivo_id or config.workvivo_id == "YOUR_WORKVIVO_ID":
        raise ValueError("Set WORKVIVO_ID to the real Workvivo tenant ID.")
    if not config.api_token or config.api_token in {"YOUR_API_TOKEN", "REPLACE_ME"}:
        raise ValueError("Set API_TOKEN to a valid API token.")


def ensure_export_folder(export_path: Path) -> None:
    export_path.mkdir(parents=True, exist_ok=True)


def iso_to_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def within_date_range(iso_value: str, date_from: datetime | None, date_to: datetime | None) -> bool:
    dt = iso_to_datetime(iso_value)
    if dt is None:
        return False

    dt_naive = dt.replace(tzinfo=None)

    if date_from and dt_naive < date_from:
        return False

    if date_to and dt_naive > date_to.replace(hour=23, minute=59, second=59):
        return False

    return True


def sanitize_filename(filename: str) -> str:
    filename = (filename or "").strip().replace("\n", " ").replace("\r", " ")
    filename = re.sub(r'[<>:"/\\|?*]', "_", filename)
    return filename[:180] if filename else "livestream"


def build_filename_base(livestream_id: str, title: str, timestamp: str) -> str:
    safe_title = sanitize_filename(title) or f"livestream_{livestream_id}"
    safe_timestamp = sanitize_filename((timestamp or "").replace(":", "-"))
    if safe_timestamp:
        return f"{livestream_id}_{safe_timestamp}_{safe_title}"
    return f"{livestream_id}_{safe_title}"


def get_next_page(payload: dict[str, Any]):
    return payload.get("meta", {}).get("pagination", {}).get("next_page")


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


def is_m3u8_url(url: str) -> bool:
    return ".m3u8" in (url or "").lower()


def fetch_text(session: requests.Session, url: str, timeout: int) -> str:
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    return response.text


def download_binary(session: requests.Session, url: str, destination: Path, timeout: int, chunk_size: int) -> None:
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


# =========================
# API
# =========================
def fetch_livestreams(session: requests.Session, config: ExportConfig, skip: int, take: int) -> dict[str, Any]:
    url = f"{API_BASE_URL.rstrip('/')}/livestreams"
    params = {"skip": skip, "take": take}
    response = session.get(url, params=params, timeout=config.request_timeout)

    if not response.ok:
        raise RuntimeError(
            f"Request failed with status {response.status_code}. URL: {response.url}. Body: {response.text}"
        )

    return response.json()


def collect_all_livestreams(session: requests.Session, config: ExportConfig, status_box, progress_bar) -> list[dict[str, Any]]:
    skip = 0
    collected: list[dict[str, Any]] = []
    page_number = 0

    while True:
        page_number += 1
        payload = fetch_livestreams(session, config, skip=skip, take=config.take)
        livestreams = payload.get("data", [])

        if not livestreams:
            break

        collected.extend(livestreams)
        status_box.info(f"Fetched page {page_number}: {len(livestreams)} livestreams (total {len(collected)})")

        next_page = get_next_page(payload)
        if next_page is None:
            break

        skip += config.take
        progress_bar.progress(min(0.2 + (page_number * 0.03), 0.35))
        time.sleep(config.sleep_between_requests)

    return collected


# =========================
# MANIFEST + EXPORT
# =========================
def livestream_to_manifest_row(livestream: dict[str, Any]) -> dict[str, Any]:
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
    }


def export_selected_livestreams(
    session: requests.Session,
    config: ExportConfig,
    selected_rows: list[dict[str, Any]],
    status_box,
    progress_bar,
) -> list[dict[str, Any]]:
    ensure_export_folder(config.export_path)
    results: list[dict[str, Any]] = []

    total = len(selected_rows)
    if total == 0:
        return results

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
                    export_folder=config.export_path,
                    timeout=config.request_timeout,
                    chunk_size=config.chunk_size,
                    progress_callback=segment_progress,
                )
                row["saved_path"] = export_info["saved_path"]
                row["output_type"] = export_info["output_type"]
                row["segment_count"] = export_info["segment_count"]
                row["playlist_path"] = export_info["playlist_path"]
                row["media_playlist_path"] = export_info["media_playlist_path"]
                row["resolved_playlist_url"] = export_info["playlist_url"]
                row["status"] = f"hls merged to {row['output_type']} and m3u8 saved"
            else:
                ext = Path(urlparse(recording_url).path).suffix or ".mp4"
                destination = config.export_path / f"{file_base}{ext}"
                download_binary(
                    session=session,
                    url=recording_url,
                    destination=destination,
                    timeout=config.request_timeout,
                    chunk_size=config.chunk_size,
                )
                row["saved_path"] = str(destination)
                row["output_type"] = ext.lstrip(".")
                row["status"] = f"file downloaded as {row['output_type']}"
                progress_bar.progress(item_index / total)

        except Exception as exc:
            row["status"] = f"failed: {exc}"
            progress_bar.progress(item_index / total)

        results.append(row)

    return results


def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    return buffer.getvalue().encode("utf-8")


# =========================
# UI
# =========================
def init_state():
    st.session_state.setdefault("fetched_rows", [])
    st.session_state.setdefault("export_results", [])
    st.session_state.setdefault("last_fetch_count", 0)


def sidebar_config() -> ExportConfig:
    st.sidebar.header("Connection")
    api_token = st.sidebar.text_input("API token", type="password", help="Stored only in this Streamlit session.")
    workvivo_id = st.sidebar.text_input("Workvivo tenant ID", value="1102")

    st.sidebar.header("Filter")
    default_from = datetime(2025, 4, 1)
    default_to = datetime(2026, 4, 30)

    date_from_date = st.sidebar.date_input("Date from", value=default_from.date())
    date_to_date = st.sidebar.date_input("Date to", value=default_to.date())

    st.sidebar.header("Advanced")
    take = st.sidebar.number_input("Page size", min_value=1, max_value=500, value=100, step=1)
    request_timeout = st.sidebar.number_input("Request timeout (seconds)", min_value=5, max_value=600, value=60, step=5)
    sleep_between_requests = st.sidebar.number_input(
        "Delay between API requests (seconds)", min_value=0.0, max_value=5.0, value=0.2, step=0.1
    )
    export_folder = st.sidebar.text_input("Export root folder", value=str(DEFAULT_EXPORT_ROOT))

    return ExportConfig(
        api_token=api_token,
        workvivo_id=workvivo_id.strip(),
        date_from=datetime.combine(date_from_date, datetime.min.time()) if date_from_date else None,
        date_to=datetime.combine(date_to_date, datetime.min.time()) if date_to_date else None,
        take=int(take),
        request_timeout=int(request_timeout),
        sleep_between_requests=float(sleep_between_requests),
        export_folder=Path(export_folder).expanduser(),
    )


def render_header(config: ExportConfig):
    st.title("🎥 Workvivo Livestream Exporter")
    st.caption("Fetch recorded livestreams, review them in a table, and export the selected recordings locally.")

    with st.expander("Export destination", expanded=False):
        st.write(f"Media files will be written to: `{config.export_path}`")
        st.write(f"Manifest path: `{config.csv_path}`")



def render_summary(rows: list[dict[str, Any]], exported_rows: list[dict[str, Any]]):
    matched = len(rows)
    exported_ok = sum(1 for row in exported_rows if str(row.get("status", "")).startswith(("hls merged", "file downloaded")))
    failed = sum(1 for row in exported_rows if str(row.get("status", "")).startswith("failed:"))

    c1, c2, c3 = st.columns(3)
    c1.metric("Matched recorded livestreams", matched)
    c2.metric("Successfully exported", exported_ok)
    c3.metric("Failed exports", failed)



def main():
    init_state()
    config = sidebar_config()
    render_header(config)

    col_left, col_right = st.columns([1, 1])
    fetch_clicked = col_left.button("Fetch livestreams", use_container_width=True)
    export_clicked = col_right.button("Export selected", use_container_width=True)

    status_box = st.empty()
    progress_bar = st.progress(0.0)

    if fetch_clicked:
        try:
            validate_config(config)
            session = build_session(config)
            all_livestreams = collect_all_livestreams(session, config, status_box, progress_bar)
            filtered_rows = [
                livestream_to_manifest_row(livestream)
                for livestream in all_livestreams
                if matches_filters(livestream, config)
            ]
            st.session_state["fetched_rows"] = filtered_rows
            st.session_state["last_fetch_count"] = len(all_livestreams)
            st.session_state["export_results"] = []
            progress_bar.progress(1.0)
            status_box.success(
                f"Fetched {len(all_livestreams)} livestreams. {len(filtered_rows)} matched the recorded/date filters."
            )
        except Exception as exc:
            status_box.error(str(exc))

    rows = st.session_state.get("fetched_rows", [])
    export_results = st.session_state.get("export_results", [])

    if rows:
        render_summary(rows, export_results)

        st.subheader("Matched livestreams")
        st.write(
            f"Total livestreams fetched: **{st.session_state.get('last_fetch_count', 0)}**  \\nMatched recorded livestreams: **{len(rows)}**"
        )

        df = pd.DataFrame(rows)
        df.insert(0, "selected", True)

        edited_df = st.data_editor(
            df,
            use_container_width=True,
            hide_index=True,
            disabled=[col for col in df.columns if col != "selected"],
            column_config={
                "selected": st.column_config.CheckboxColumn("Export", help="Tick rows to export."),
                "recording_url": st.column_config.TextColumn(width="medium"),
                "permalink": st.column_config.TextColumn(width="medium"),
                "description": st.column_config.TextColumn(width="large"),
            },
            key="livestream_editor",
        )

        selected_rows = edited_df[edited_df["selected"]].drop(columns=["selected"]).to_dict(orient="records")

        manifest_csv = dataframe_to_csv_bytes(pd.DataFrame(rows, columns=MANIFEST_COLUMNS))
        st.download_button(
            label="Download matched manifest CSV",
            data=manifest_csv,
            file_name=f"livestream_export_manifest_{config.workvivo_id}.csv",
            mime="text/csv",
        )

        if export_clicked:
            try:
                validate_config(config)
                session = build_session(config)
                if not selected_rows:
                    status_box.warning("No rows selected for export.")
                else:
                    progress_bar.progress(0.0)
                    results = export_selected_livestreams(session, config, selected_rows, status_box, progress_bar)
                    st.session_state["export_results"] = results

                    results_df = pd.DataFrame(results, columns=MANIFEST_COLUMNS)
                    ensure_export_folder(config.export_path)
                    results_df.to_csv(config.csv_path, index=False, quoting=csv.QUOTE_MINIMAL)
                    status_box.success(
                        f"Export complete. {len(results)} rows processed. Manifest written to {config.csv_path}"
                    )
            except Exception as exc:
                status_box.error(str(exc))

    if st.session_state.get("export_results"):
        st.subheader("Export results")
        results_df = pd.DataFrame(st.session_state["export_results"], columns=MANIFEST_COLUMNS)
        st.dataframe(results_df, use_container_width=True, hide_index=True)

        results_csv = dataframe_to_csv_bytes(results_df)
        st.download_button(
            label="Download export results CSV",
            data=results_csv,
            file_name=f"livestream_export_results_{config.workvivo_id}.csv",
            mime="text/csv",
        )

    st.markdown("---")
    st.markdown(
        "**Notes**  \n"
        "- API token is entered in the sidebar and used only for the current session.  \n"
        "- HLS recordings (`.m3u8`) are merged by downloading and concatenating segments.  \n"
        "- Output files and a manifest are written to your selected export folder on the machine running Streamlit."
    )


if __name__ == "__main__":
    main()
