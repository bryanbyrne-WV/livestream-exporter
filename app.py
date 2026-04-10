# -*- coding: utf-8 -*-
import csv
import io
import re
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, time as dt_time
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

MANIFEST_COLUMNS = [
    "livestream_id","title","description","host_name","started_at","ended_at","created_at",
    "audience_type","audience_names","viewers_count","recording_url","resolved_playlist_url",
    "playlist_path","media_playlist_path","saved_path","output_type","segment_count","status","permalink",
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


def within_date_range(iso_value, date_from, date_to):
    dt = iso_to_datetime(iso_value)
    if dt is None:
        return False
    dt = dt.replace(tzinfo=None)
    if date_from and dt < date_from:
        return False
    if date_to and dt > date_to.replace(hour=23, minute=59, second=59):
        return False
    return True


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


# =========================================================
# STYLING (UPDATED)
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

            /* CLEAN WHITE SIDEBAR */
            section[data-testid="stSidebar"] {
                background: #FFFFFF;
                border-right: 1px solid #E5E7EB;
            }

            section[data-testid="stSidebar"] * {
                color: #111827 !important;
            }

            /* INPUT BOXES - HIGH VISIBILITY */
            section[data-testid="stSidebar"] .stTextInput input,
            section[data-testid="stSidebar"] .stNumberInput input,
            section[data-testid="stSidebar"] .stDateInput input {
                background-color: #FFFFFF !important;
                border: 2px solid #111827 !important;
                border-radius: 8px !important;
                padding: 0.4rem !important;
            }

            /* FOCUS STATE */
            section[data-testid="stSidebar"] input:focus {
                border: 2px solid #5A3EA6 !important;
                box-shadow: 0 0 0 2px rgba(90,62,166,0.2) !important;
            }

            /* BUTTONS */
            section[data-testid="stSidebar"] button {
                border-radius: 8px !important;
                font-weight: 600 !important;
            }

            /* TEST BUTTON GREEN */
            section[data-testid="stSidebar"] .stButton button {
                background-color: #16A34A !important;
                color: white !important;
            }

            section[data-testid="stSidebar"] .stButton button:hover {
                background-color: #15803D !important;
            }

            /* SUCCESS / ERROR */
            .stAlert {
                border-radius: 10px !important;
            }

            .stAlert[data-baseweb="notification"][kind="positive"] {
                background-color: #DCFCE7 !important;
                color: #166534 !important;
                border: 1px solid #86EFAC !important;
            }

            .stAlert[data-baseweb="notification"][kind="negative"] {
                background-color: #FEE2E2 !important;
                color: #991B1B !important;
                border: 1px solid #FCA5A5 !important;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


# =========================================================
# LOGIN
# =========================================================
def render_login_screen():
    st.title("Livestream Exporter Login")

    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

    if st.button("Login"):
        if username == DEFAULT_ADMIN_USERNAME and password == DEFAULT_ADMIN_PASSWORD:
            st.session_state.authenticated = True
            st.success("Logged in")
            st.rerun()
        else:
            st.error("Invalid credentials")


# =========================================================
# MAIN
# =========================================================
def main_app():
    apply_global_branding()

    st.title("🎥 Workvivo Livestream Exporter")

    st.sidebar.header("Connection")

    workvivo_id = st.sidebar.text_input(
        "Workvivo tenant ID",
        placeholder="Enter your Workvivo tenant ID"
    )

    api_base_url = st.sidebar.text_input(
        "API Base URL",
        placeholder="https://api.workvivo.com/v1"
    )

    api_token = st.sidebar.text_input(
        "API Token",
        type="password",
        placeholder="Enter your Workvivo API token"
    )

    if st.sidebar.button("Test connection"):
        if not api_base_url:
            st.sidebar.error("Set API Base URL.")
        elif not workvivo_id:
            st.sidebar.error("Set Workvivo ID.")
        elif not api_token:
            st.sidebar.error("Set API Token.")
        else:
            st.sidebar.success("Connection looks good (mock test).")


# =========================================================
# ENTRY
# =========================================================
def main():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if not st.session_state.authenticated:
        render_login_screen()
    else:
        main_app()


if __name__ == "__main__":
    main()
