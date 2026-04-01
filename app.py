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


@st.dialog("Actualización de Google Sheets", width="large")
def show_update_dialog() -> None:
    if st.session_state.get("execute_update", False):
        try:
            with st.spinner("Procesando nuevas observaciones..."):
                summary = run_update()
        except ConfigError as exc:
            st.session_state["update_error"] = f"Error de configuración: {exc}"
            st.session_state["update_traceback"] = None
            st.session_state["update_summary"] = None
        except Exception as exc:
            st.session_state["update_error"] = f"La actualización falló: {exc}"
            st.session_state["update_traceback"] = traceback.format_exc()
            st.session_state["update_summary"] = None
        else:
            st.session_state["update_error"] = None
            st.session_state["update_traceback"] = None
            st.session_state["update_summary"] = summary
        finally:
            st.session_state["execute_update"] = False

    if st.session_state.get("update_error"):
        st.error(st.session_state["update_error"])
        if st.session_state.get("update_traceback"):
            st.code(st.session_state["update_traceback"], language="text")
    elif st.session_state.get("update_summary") is not None:
        summary = st.session_state["update_summary"]
        st.success("Actualización completada.")
        st.write(f"Tamaño total de la base antes de actualizar: `{summary.dataset_size_before_update}`")
        st.write(f"Total de comprobantes detectados para analizar: `{summary.total_submissions_to_analyze}`")
        st.write(f"Nuevas filas agregadas: `{summary.appended_rows}`")
        st.write(f"Comprobantes aceptados: `{summary.accepted_rows}`")
        st.write(f"Comprobantes que requieren revisión: `{summary.rows_requiring_review}`")
        st.write(f"Comprobantes con operación en blanco: `{summary.blank_operation_number_rows}`")
        st.write(f"Comprobantes duplicados por número de operación: `{summary.duplicate_operation_number_rows}`")
        st.write(f"Comprobantes duplicados por contenido: `{summary.duplicate_file_content_rows}`")
        st.write(f"Links inválidos: `{summary.invalid_link_rows}`")
        st.write(f"Errores de procesamiento: `{summary.processing_error_rows}`")

    if st.button("Cerrar", use_container_width=True):
        st.session_state["show_update_dialog"] = False
        st.session_state["execute_update"] = False
        st.rerun()


st.title("Yape Voucher Updater")
st.write("Ejecuta la actualización de Google Sheets sin usar PowerShell.")
require_login()

if st.button("Actualizar Google Sheets", type="primary", use_container_width=True):
    st.session_state["update_summary"] = None
    st.session_state["update_error"] = None
    st.session_state["update_traceback"] = None
    st.session_state["execute_update"] = True
    st.session_state["show_update_dialog"] = True
    st.rerun()

if st.session_state.get("show_update_dialog", False):
    show_update_dialog()

if st.button("Cerrar sesión", use_container_width=True):
    st.session_state["authenticated"] = False
    st.rerun()
