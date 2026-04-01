from __future__ import annotations

import hmac
import traceback

import pandas as pd
import streamlit as st

from main import ConfigError, fetch_processed_dataset, run_update


st.set_page_config(page_title="Yape Voucher Updater", page_icon=":page_facing_up:")

st.markdown(
    """
    <style>
    div[data-testid="stDialog"] div[role="dialog"] {
        max-height: 96vh;
        height: auto;
        overflow: visible;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


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

    st.subheader("Ingreso de administrador")
    submitted_password = st.text_input("Contrasena", type="password")

    if st.button("Ingresar", use_container_width=True):
        if password_is_valid(submitted_password):
            st.session_state["authenticated"] = True
            st.rerun()
        else:
            st.error("Contrasena incorrecta.")

    st.stop()


@st.dialog("Actualizacion de Google Sheets", width="large")
def show_update_dialog() -> None:
    if st.session_state.get("execute_update", False):
        try:
            with st.spinner("Procesando nuevas observaciones..."):
                summary = run_update()
        except ConfigError as exc:
            st.session_state["update_error"] = f"Error de configuracion: {exc}"
            st.session_state["update_traceback"] = None
            st.session_state["update_summary"] = None
        except Exception as exc:
            st.session_state["update_error"] = f"La actualizacion fallo: {exc}"
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
        st.success("Actualizacion completada.")
        st.write(f"Tamano total de la base antes de actualizar: `{summary.dataset_size_before_update}`")
        st.write(f"Total de comprobantes detectados para analizar: `{summary.total_submissions_to_analyze}`")
        st.write(f"Nuevas filas agregadas: `{summary.appended_rows}`")
        st.write(f"Comprobantes aceptados: `{summary.accepted_rows}`")
        st.write(f"Comprobantes que requieren revision: `{summary.rows_requiring_review}`")
        st.write(f"Comprobantes con operacion en blanco: `{summary.blank_operation_number_rows}`")
        st.write(f"Comprobantes duplicados por numero de operacion: `{summary.duplicate_operation_number_rows}`")
        st.write(f"Comprobantes duplicados por contenido: `{summary.duplicate_file_content_rows}`")
        st.write(f"Links invalidos: `{summary.invalid_link_rows}`")
        st.write(f"Errores de procesamiento: `{summary.processing_error_rows}`")

    if st.button("Cerrar", use_container_width=True):
        st.session_state["active_dialog"] = None
        st.session_state["execute_update"] = False
        st.rerun()


@st.dialog("Base procesada", width="large")
def show_dataset_dialog() -> None:
    try:
        with st.spinner("Cargando base procesada..."):
            dataset = fetch_processed_dataset()
    except ConfigError as exc:
        st.error(f"Error de configuracion: {exc}")
        st.stop()
    except Exception as exc:
        st.error(f"No se pudo cargar la base procesada: {exc}")
        st.code(traceback.format_exc(), language="text")
        st.stop()

    df = pd.DataFrame(dataset.rows)
    if df.empty:
        st.info("La base procesada todavia no tiene registros.")
    else:
        visible_columns = [
            "uploader_email",
            "voucher_drive_link",
            "extracted_operation_number",
            "extracted_amount",
            "extracted_currency",
            "extracted_date",
            "extracted_time",
            "extracted_phone_or_recipient",
            "status",
            "error_message",
        ]
        df = df[visible_columns].copy()
        df["extracted_amount"] = pd.to_numeric(df["extracted_amount"], errors="coerce")

        status_counts = df["status"].value_counts(dropna=False).to_dict()
        with st.expander("Resumen de la base", expanded=False):
            c1, c2, c3 = st.columns(3)
            c1.metric("Total de filas", dataset.total_rows)
            c2.metric("Aceptadas", int(status_counts.get("ok", 0)))
            c3.metric(
                "Requieren revision",
                int(
                    status_counts.get("blank_operation_number", 0)
                    + status_counts.get("duplicate_operation_number", 0)
                    + status_counts.get("invalid_drive_link", 0)
                    + status_counts.get("duplicate_invalid_link", 0)
                    + status_counts.get("processing_error", 0)
                ),
            )

        available_statuses = sorted([status for status in df["status"].dropna().unique().tolist() if status])
        selected_statuses = st.multiselect(
            "Filtrar por estado",
            options=available_statuses,
            default=available_statuses,
        )

        if selected_statuses:
            df = df[df["status"].isin(selected_statuses)]
        else:
            df = df.iloc[0:0]

        df = df.rename(
            columns={
                "uploader_email": "Correo",
                "voucher_drive_link": "Comprobante",
                "extracted_operation_number": "Numero de operacion",
                "extracted_amount": "Monto",
                "extracted_currency": "Moneda",
                "extracted_date": "Fecha",
                "extracted_time": "Hora",
                "extracted_phone_or_recipient": "Telefono o destinatario",
                "status": "Estado",
                "error_message": "Detalle",
            }
        )

        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            height=420,
            column_config={
                "Comprobante": st.column_config.LinkColumn("Comprobante"),
                "Monto": st.column_config.NumberColumn("Monto", format="%.2f"),
                "Detalle": st.column_config.TextColumn("Detalle", width="large"),
            },
        )

    if st.button("Cerrar", use_container_width=True, key="close_dataset_dialog_bottom"):
        st.session_state["active_dialog"] = None
        st.rerun()


st.title("Yape Voucher Updater")
st.write("Ejecuta la actualizacion de Google Sheets sin usar PowerShell.")
require_login()

if st.button("Actualizar Google Sheets", type="primary", use_container_width=True):
    st.session_state["active_dialog"] = "update"
    st.session_state["update_summary"] = None
    st.session_state["update_error"] = None
    st.session_state["update_traceback"] = None
    st.session_state["execute_update"] = True
    st.rerun()

if st.session_state.get("active_dialog") == "update":
    show_update_dialog()

if st.button("Ver base procesada", use_container_width=True):
    st.session_state["active_dialog"] = "dataset"
    st.rerun()

if st.session_state.get("active_dialog") == "dataset":
    show_dataset_dialog()

if st.button("Cerrar sesion", use_container_width=True):
    st.session_state["authenticated"] = False
    st.rerun()
