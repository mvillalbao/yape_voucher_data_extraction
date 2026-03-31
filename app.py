from __future__ import annotations

import traceback

import streamlit as st

from main import ConfigError, run_update


st.set_page_config(page_title="Yape Voucher Updater", page_icon=":page_facing_up:")

st.title("Yape Voucher Updater")
st.write("Run the Google Sheets voucher update without using PowerShell.")

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
