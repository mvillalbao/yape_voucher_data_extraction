from __future__ import annotations

import base64
import hmac
import re
import struct
import traceback

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from main import (
    ConfigError,
    fetch_drive_preview,
    fetch_processed_dataset,
    run_update,
    update_manual_review,
)


st.set_page_config(page_title="Yape Voucher Updater", page_icon=":page_facing_up:")

st.markdown(
    """
    <style>
    div[data-testid="stDialog"] div[role="dialog"] {
        max-height: none;
        height: auto;
        overflow: visible;
    }
    div[data-testid="stButton"] > button[kind="secondary"] {
        border-radius: 999px;
        display: flex;
        align-items: center;
        justify-content: center;
        padding: 0 !important;
        text-align: center;
    }
    div[data-testid="stButton"] > button[kind="secondary"] p {
        margin: 0;
        width: 100%;
        text-align: center;
    }
    @media (max-width: 768px) {
        div[data-testid="stVerticalBlock"]:has(#manual-review-mobile-nav) div[data-testid="stHorizontalBlock"] {
            flex-wrap: nowrap !important;
            gap: 0.5rem !important;
        }
        div[data-testid="stVerticalBlock"]:has(#manual-review-mobile-nav) div[data-testid="stHorizontalBlock"] > div[data-testid="column"] {
            min-width: 0 !important;
            flex: 1 1 0 !important;
        }
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


def is_mobile_session() -> bool:
    context = getattr(st, "context", None)
    headers = getattr(context, "headers", None)
    if headers is None:
        return False

    user_agent = ""
    try:
        user_agent = str(headers.get("user-agent", ""))
    except Exception:
        user_agent = ""

    if not user_agent:
        return False

    return bool(
        re.search(
            r"iphone|ipad|ipod|android|mobile|blackberry|opera mini|iemobile",
            user_agent,
            re.IGNORECASE,
        )
    )


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
            height=500,
            column_config={
                "Comprobante": st.column_config.LinkColumn("Comprobante"),
                "Monto": st.column_config.NumberColumn("Monto", format="%.2f"),
                "Detalle": st.column_config.TextColumn("Detalle", width="large"),
            },
        )

    if st.button("Cerrar", use_container_width=True, key="close_dataset_dialog_bottom"):
        st.session_state["active_dialog"] = None
        st.rerun()


@st.cache_data(show_spinner=False, ttl=3600)
def get_review_preview(file_id: str) -> tuple[str, bytes]:
    preview = fetch_drive_preview(file_id)
    return preview.mime_type, preview.content


def is_manually_reviewed(value: str) -> bool:
    return value.strip().lower() in {"yes", "true", "1", "si", "sí"}


def parse_manual_amount(raw_value: str) -> float | None:
    cleaned = raw_value.strip()
    if not cleaned:
        return None
    return float(cleaned)


def validate_manual_review_inputs(
    *,
    amount_text: str,
    currency: str,
    date_value: str,
    time_value: str,
) -> tuple[float | None, list[str]]:
    errors: list[str] = []

    try:
        amount = parse_manual_amount(amount_text)
    except ValueError:
        amount = None
        errors.append("El monto debe ser un numero valido o quedar vacio.")

    currency_value = currency.strip().upper()
    if currency_value and not re.fullmatch(r"[A-Z]{3}", currency_value):
        errors.append("La moneda debe estar en formato de 3 letras mayusculas, por ejemplo PEN o USD.")

    date_clean = date_value.strip()
    if date_clean and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_clean):
        errors.append("La fecha debe estar en formato YYYY-MM-DD.")

    time_clean = time_value.strip()
    if time_clean and not re.fullmatch(r"\d{2}:\d{2}", time_clean):
        errors.append("La hora debe estar en formato HH:MM.")

    return amount, errors


def get_image_dimensions(content: bytes, mime_type: str) -> tuple[int, int] | None:
    try:
        if mime_type == "image/png" and content[:8] == b"\x89PNG\r\n\x1a\n":
            width, height = struct.unpack(">II", content[16:24])
            return int(width), int(height)
        if mime_type in {"image/jpeg", "image/jpg"} and content[:2] == b"\xff\xd8":
            index = 2
            while index < len(content):
                if content[index] != 0xFF:
                    index += 1
                    continue
                marker = content[index + 1]
                if marker in {0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7, 0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF}:
                    height, width = struct.unpack(">HH", content[index + 5:index + 9])
                    return int(width), int(height)
                segment_length = struct.unpack(">H", content[index + 2:index + 4])[0]
                index += 2 + segment_length
        if mime_type == "image/gif" and content[:6] in {b"GIF87a", b"GIF89a"}:
            width, height = struct.unpack("<HH", content[6:10])
            return int(width), int(height)
    except Exception:
        return None
    return None


def render_hover_zoom_image(*, content: bytes, mime_type: str, key: str) -> None:
    encoded = base64.b64encode(content).decode("utf-8")
    image_url = f"data:{mime_type};base64,{encoded}"
    container_id = f"hover-zoom-{key}"
    dimensions = get_image_dimensions(content, mime_type)
    frame_width = 340
    if dimensions:
        original_width, original_height = dimensions
        rendered_height = int(frame_width * (original_height / max(original_width, 1)))
    else:
        rendered_height = 500
    component_height = max(240, min(rendered_height + 8, 680))
    html = f"""
    <div id="{container_id}" class="hover-zoom-shell">
      <div class="hover-zoom-frame">
        <img src="{image_url}" alt="Comprobante" class="hover-zoom-image" />
      </div>
    </div>
    <style>
      #{container_id}.hover-zoom-shell {{
        width: 100%;
        display: flex;
        justify-content: center;
      }}
      #{container_id} .hover-zoom-frame {{
        width: min(100%, 340px);
        overflow: hidden;
        border-radius: 18px;
        cursor: zoom-in;
        background: transparent;
      }}
      #{container_id} .hover-zoom-image {{
        display: block;
        width: 100%;
        height: auto;
        border-radius: 18px;
        transform-origin: center center;
        transform: scale(1);
        transition: transform 120ms ease-out;
        will-change: transform, transform-origin;
      }}
      @media (hover: none) {{
        #{container_id} .hover-zoom-frame {{
          cursor: default;
        }}
      }}
    </style>
    <script>
      (() => {{
        const root = document.getElementById("{container_id}");
        if (!root) return;
        const frame = root.querySelector(".hover-zoom-frame");
        const image = root.querySelector(".hover-zoom-image");
        if (!frame || !image) return;
        const setZoom = (event) => {{
          const rect = frame.getBoundingClientRect();
          const x = Math.max(0, Math.min(event.clientX - rect.left, rect.width));
          const y = Math.max(0, Math.min(event.clientY - rect.top, rect.height));
          const percentX = rect.width ? (x / rect.width) * 100 : 50;
          const percentY = rect.height ? (y / rect.height) * 100 : 50;
          image.style.transformOrigin = `${{percentX}}% ${{percentY}}%`;
          image.style.transform = "scale(4)";
        }};
        frame.addEventListener("mousemove", setZoom);
        frame.addEventListener("mouseenter", setZoom);
        frame.addEventListener("mouseleave", () => {{
          image.style.transformOrigin = "center center";
          image.style.transform = "scale(1)";
        }});
      }})();
    </script>
    """
    components.html(html, height=component_height, scrolling=False)


@st.dialog("Revision manual", width="large")
def show_manual_review_dialog() -> None:
    try:
        with st.spinner("Cargando observaciones pendientes de revision manual..."):
            dataset = fetch_processed_dataset()
    except ConfigError as exc:
        st.error(f"Error de configuracion: {exc}")
        st.stop()
    except Exception as exc:
        st.error(f"No se pudo cargar la base procesada: {exc}")
        st.code(traceback.format_exc(), language="text")
        st.stop()

    pending_rows = [row for row in dataset.rows if not is_manually_reviewed(str(row.get("manually_reviewed", "")))]
    if not pending_rows:
        st.success("No hay observaciones pendientes de revision manual.")
        if st.button("Cerrar", use_container_width=True, key="close_manual_review_dialog_empty"):
            st.session_state["active_dialog"] = None
            st.rerun()
        return

    mobile_session = is_mobile_session()

    current_index = int(st.session_state.get("manual_review_index", 0))
    if current_index >= len(pending_rows):
        current_index = max(len(pending_rows) - 1, 0)
        st.session_state["manual_review_index"] = current_index

    current_row = pending_rows[current_index]
    sheet_row_number = int(current_row["_sheet_row_number"])
    current_status = str(current_row.get("status", "")).strip()

    meta_left, meta_right = st.columns([2, 1])
    with meta_left:
        st.caption(f"Observacion {current_index + 1} de {len(pending_rows)} pendientes")
    with meta_right:
        st.caption(f"Estado actual: `{current_status or 'sin estado'}`")

    file_id = str(current_row.get("voucher_drive_file_id", "")).strip()
    if file_id:
        try:
            mime_type, content = get_review_preview(file_id)
        except Exception as exc:
            st.warning(f"No se pudo cargar la imagen del comprobante: {exc}")
        else:
            if mime_type.startswith("image/"):
                if mobile_session:
                    image_left, image_center, image_right = st.columns([0.5, 2.4, 0.5])
                    with image_center:
                        render_hover_zoom_image(
                            content=content,
                            mime_type=mime_type,
                            key=str(sheet_row_number),
                        )

                    with st.container():
                        st.markdown('<div id="manual-review-mobile-nav"></div>', unsafe_allow_html=True)
                        mobile_prev, mobile_next = st.columns([1, 1], gap="small")
                        with mobile_prev:
                            if st.button("‹", key=f"manual_review_prev_mobile_{sheet_row_number}", disabled=current_index == 0, use_container_width=True):
                                st.session_state["manual_review_index"] = max(current_index - 1, 0)
                                st.rerun()
                        with mobile_next:
                            if st.button("›", key=f"manual_review_next_mobile_{sheet_row_number}", disabled=current_index >= len(pending_rows) - 1, use_container_width=True):
                                st.session_state["manual_review_index"] = min(current_index + 1, len(pending_rows) - 1)
                                st.rerun()
                else:
                    outer_left, left_button_col, center, right_button_col, outer_right = st.columns(
                        [1.05, 0.3, 1.2, 0.3, 1.05],
                        vertical_alignment="center",
                    )
                    with left_button_col:
                        left_spacer, left_center, left_spacer_2 = st.columns([1, 1, 1])
                        with left_center:
                            if st.button("‹", key=f"manual_review_prev_{sheet_row_number}", disabled=current_index == 0):
                                st.session_state["manual_review_index"] = max(current_index - 1, 0)
                                st.rerun()
                    with center:
                        render_hover_zoom_image(
                            content=content,
                            mime_type=mime_type,
                            key=str(sheet_row_number),
                        )
                    with right_button_col:
                        right_spacer, right_center, right_spacer_2 = st.columns([1, 1, 1])
                        with right_center:
                            if st.button("›", key=f"manual_review_next_{sheet_row_number}", disabled=current_index >= len(pending_rows) - 1):
                                st.session_state["manual_review_index"] = min(current_index + 1, len(pending_rows) - 1)
                                st.rerun()
            else:
                st.info("Este archivo no es una imagen. Usa el link del comprobante para revisarlo manualmente.")
    else:
        st.info("Esta observacion no tiene un archivo de imagen disponible.")

    if current_row.get("voucher_drive_link"):
        left, center, right = st.columns([1.4, 1.2, 1.4])
        with center:
            st.link_button("Abrir comprobante", str(current_row["voucher_drive_link"]), use_container_width=True)

    with st.form(key=f"manual_review_form_{sheet_row_number}"):
        st.markdown("### Revision")

        row1_col1, row1_col2, row1_col3 = st.columns(3)
        with row1_col1:
            operation_number = st.text_input(
                "Numero de operacion",
                value=str(current_row.get("extracted_operation_number", "")),
            )
        with row1_col2:
            amount_text = st.text_input(
                "Monto",
                value=str(current_row.get("extracted_amount", "")),
                help="Dejalo vacio si no corresponde o no aplica.",
            )
        with row1_col3:
            currency = st.text_input(
                "Moneda",
                value=str(current_row.get("extracted_currency", "")),
                max_chars=3,
            )

        row2_col1, row2_col2, row2_col3 = st.columns(3)
        with row2_col1:
            date_value = st.text_input(
                "Fecha",
                value=str(current_row.get("extracted_date", "")),
                help="Formato YYYY-MM-DD",
            )
        with row2_col2:
            time_value = st.text_input(
                "Hora",
                value=str(current_row.get("extracted_time", "")),
                help="Formato HH:MM",
            )
        with row2_col3:
            phone_or_recipient = st.text_input(
                "Telefono o destinatario",
                value=str(current_row.get("extracted_phone_or_recipient", "")),
            )

        left, center, right = st.columns([3, 1, 3])
        with center:
            submitted = st.form_submit_button("OK", type="primary", use_container_width=True)

    if submitted:
        amount, errors = validate_manual_review_inputs(
            amount_text=amount_text,
            currency=currency,
            date_value=date_value,
            time_value=time_value,
        )
        if errors:
            for error in errors:
                st.error(error)
        else:
            try:
                update_manual_review(
                    sheet_row_number=sheet_row_number,
                    operation_number=operation_number,
                    amount=amount,
                    currency=currency,
                    date=date_value,
                    time_value=time_value,
                    phone_or_recipient=phone_or_recipient,
                    status=current_status,
                    error_message=str(current_row.get("error_message", "")),
                )
            except Exception as exc:
                st.error(f"No se pudo guardar la revision manual: {exc}")
                st.code(traceback.format_exc(), language="text")
            else:
                st.session_state["manual_review_message"] = "Observacion revisada y guardada."
                st.rerun()

    if st.session_state.get("manual_review_message"):
        st.success(st.session_state["manual_review_message"])
        st.session_state["manual_review_message"] = None

    if st.button("Cerrar", use_container_width=True, key="close_manual_review_dialog"):
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

if st.button("Revision manual", use_container_width=True):
    st.session_state["active_dialog"] = "manual_review"
    st.session_state["manual_review_index"] = 0
    st.rerun()

if st.session_state.get("active_dialog") == "manual_review":
    show_manual_review_dialog()

if st.button("Cerrar sesion", use_container_width=True):
    st.session_state["authenticated"] = False
    st.rerun()
