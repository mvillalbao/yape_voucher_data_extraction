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

    st.subheader("Ingreso de Administrador")
    submitted_password = st.text_input("Contraseña", type="password")

    if st.button("Ingresar", use_container_width=True):
        if password_is_valid(submitted_password):
            st.session_state["authenticated"] = True
            st.rerun()
        else:
            st.error("Contraseña incorrecta.")

    st.stop()


st.title("Yape Voucher Updater")
st.write("Ejecuta la actualización de Google Sheets sin usar PowerShell.")
require_login()

if st.button("Actualizar Google Sheets", type="primary", use_container_width=True):
    with st.spinner("Procesando nuevas observaciones..."):
        try:
            summary = run_update()
        except ConfigError as exc:
            st.error(f"Error de configuración: {exc}")
        except Exception as exc:
            st.error(f"La actualización falló: {exc}")
            st.code(traceback.format_exc(), language="text")
        else:
            st.success("Actualización completada.")
            st.write(f"Hoja procesada: `{summary.processed_sheet_name}`")
            st.write(f"Modelo: `{summary.openai_model}`")
            st.write(f"Máximo de workers: `{summary.max_workers}`")
            st.write(f"Filas crudas encontradas: `{summary.raw_rows_found}`")
            st.write(f"Observaciones pendientes: `{summary.pending_submissions}`")
            st.write(f"Filas agregadas: `{summary.appended_rows}`")

if st.button("Cerrar sesión", use_container_width=True):
    st.session_state["authenticated"] = False
    st.rerun()
