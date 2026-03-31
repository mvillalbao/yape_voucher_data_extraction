from __future__ import annotations

import hmac
import traceback

import streamlit as st

from main import ConfigError, run_update


st.set_page_config(page_title="Yape Voucher Updater", page_icon=":page_facing_up:")


def get_app_password() -> str:
    password = st.secrets.get("APP_PASSWORD", "")
    if not password:
        raise RuntimeError("Missing APP_PASSWORD in Streamlit secrets.")
    return password


def password_is_valid(submitted_password: str) -> bool:
    expected_password = get_app_password()
    return hmac.compare_digest(submitted_password, expected_password)


def require_login() -> None:
    if st.session_state.get("authenticated", False):
        return

    st.subheader("Admin Login")
    submitted_password = st.text_input("Password", type="password")

    if st.button("Unlock", use_container_width=True):
        if password_is_valid(submitted_password):
            st.session_state["authenticated"] = True
            st.rerun()
        else:
            st.error("Incorrect password.")

    st.stop()


st.title("Yape Voucher Updater")
st.write("Run the Google Sheets voucher update without using PowerShell.")
require_login()

if st.button("Update Google Sheets", type="primary", use_container_width=True):
    with st.spinner("Processing new submissions..."):
        try:
            summary = run_update()
        except ConfigError as exc:
            st.error(f"Configuration error: {exc}")
        except Exception as exc:
            st.error(f"Update failed: {exc}")
            st.code(traceback.format_exc(), language="text")
        else:
            st.success("Update completed.")
            st.write(f"Processed sheet: `{summary.processed_sheet_name}`")
            st.write(f"Model: `{summary.openai_model}`")
            st.write(f"Max workers: `{summary.max_workers}`")
            st.write(f"Raw rows found: `{summary.raw_rows_found}`")
            st.write(f"Pending submissions: `{summary.pending_submissions}`")
            st.write(f"Rows appended: `{summary.appended_rows}`")

if st.button("Log out", use_container_width=True):
    st.session_state["authenticated"] = False
    st.rerun()
