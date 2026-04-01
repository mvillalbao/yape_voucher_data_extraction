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

if st.button("Cerrar sesión", use_container_width=True):
    st.session_state["authenticated"] = False
    st.rerun()
