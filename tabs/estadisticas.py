"""
Fase 3 — tabs/estadisticas.py
Pestaña Estadísticas: datos de la cuenta ML, reputación y ventas.
"""
from __future__ import annotations
import calendar
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional

from nicegui import app, background_tasks, run, ui

from ml_api import (
    get_ml_access_token,
    ml_get_user_profile,
    ml_get_user_id,
    ml_get_orders,
    ml_get_shipments_today,
    ml_get_my_items,
    ml_get_unanswered_questions,
)
from db import get_cotizador_param


# ---------------------------------------------------------------------------
# Helpers de sesión
# ---------------------------------------------------------------------------

def _require_login() -> Optional[Dict[str, Any]]:
    user = app.storage.user.get("user")
    if not user:
        ui.notify("Debes iniciar sesión para continuar", color="negative")
    return user


# ---------------------------------------------------------------------------
# Helpers de formato (exclusivos de esta tab)
# ---------------------------------------------------------------------------

def fmt_m(val) -> str:
    try:
        return f"${int(round(float(val))):,}".replace(",", ".")
    except Exception:
        return "$0"


def fmt_n(val) -> str:
    try:
        return f"{int(round(float(val))):,}".replace(",", ".")
    except Exception:
        return "0"


def _safe_str(val) -> str:
    if isinstance(val, str):
        return val.strip()
    if isinstance(val, dict):
        return (val.get("picture_url") or val.get("secure_url") or
                val.get("url") or val.get("data") or "").strip()
    return ""


def _cuotas_key(it: dict) -> tuple:
    sku = (it.get("seller_sku") or "").strip()
    if sku:
        return ("sku", sku)
    cpid = (it.get("catalog_product_id") or "").strip()
    if cpid:
        return ("catalog", cpid)
    return ("id", str(it.get("id") or ""))


# ---------------------------------------------------------------------------
# Renderer principal (sólo llamado desde build_tab_estadisticas)
# ---------------------------------------------------------------------------

def _pintar_home_inline(
    container, profile: Optional[Dict], orders_data: Dict[str, Any], user_id: Optional[int] = None, items_data: Optional[Dict[str, Any]] = None, on_refresh: Optional[Callable[[], None]] = None, shipments_today: Optional[Dict[str, int]] = None, questions: Optional[List] = None
) -> None:
    """Pinta el contenido del Home con los datos ya cargados. on_refresh permite actualizar datos al vuelo."""
    raw_orders = orders_data.get("results") or orders_data.get("orders") or orders_data.get("elements") or []
    results = [o for o in raw_orders if isinstance(o, dict)]
    rep = (profile or {}).get("seller_reputation") or {}
    today_local = datetime.now().date()
    primer_dia_mes = today_local.replace(day=1)
    hoy_unidades, hoy_monto = 0, 0.0
    flex_hoy = 0
    me_hoy = 0
    ayer_unidades, ayer_monto = 0, 0.0
    antes_ayer_unidades, antes_ayer_monto = 0, 0.0
    semana_unidades, semana_monto = 0, 0.0
    d15_unidades, d15_monto = 0, 0.0
    d21_unidades, d21_monto = 0, 0.0
    mes_unidades, mes_monto = 0, 0.0
    d60_unidades, d60_monto = 0, 0.0
    d90_unidades, d90_monto = 0, 0.0
    ventas_mes_actual_unid, ventas_mes_actual_monto = 0, 0.0
    por_mes: Dict[str, Any] = {}
    top_productos: Dict[str, Dict[str, Any]] = {}  # item_id -> {title, units}
    ayer_local = today_local - timedelta(days=1)
    antes_ayer_local = today_local - timedelta(days=2)

    for ord_item in results:
        dt_str = ord_item.get("date_created") or ord_item.get("date_closed") or ord_item.get("date_last_updated") or ""
        if not dt_str or not isinstance(dt_str, str):
            continue
        try:
            dt = datetime.strptime(dt_str[:10], "%Y-%m-%d").date()
        except Exception:
            continue
        total_amount = ord_item.get("total_amount") or ord_item.get("paid_amount")
        if total_amount is None and ord_item.get("payments"):
            pay = ord_item["payments"][0] if isinstance(ord_item["payments"], list) else {}
            total_amount = pay.get("total_amount") or pay.get("total_paid_amount") or pay.get("transaction_amount")
        try:
            total_amount = float(total_amount or 0)
        except (TypeError, ValueError):
            total_amount = 0.0
        items = ord_item.get("order_items") or ord_item.get("items") or []
        units = sum(int(it.get("quantity") or it.get("qty") or 0) for it in items if isinstance(it, dict))
        if units == 0 and total_amount > 0:
            units = 1
        if dt == today_local:
            hoy_unidades += units
            hoy_monto += total_amount
            logistic = (ord_item.get("shipping") or {}).get("logistic_type") or ""
            if logistic == "self_service":
                flex_hoy += 1
            elif logistic in ("fulfillment", "xd_drop_off", "drop_off", "cross_docking"):
                me_hoy += 1
        if dt == ayer_local:
            ayer_unidades += units
            ayer_monto += total_amount
        if dt == antes_ayer_local:
            antes_ayer_unidades += units
            antes_ayer_monto += total_amount
        days_ago = (today_local - dt).days
        if days_ago <= 6:
            semana_unidades += units
            semana_monto += total_amount
        if days_ago <= 14:
            d15_unidades += units
            d15_monto += total_amount
        if days_ago <= 20:
            d21_unidades += units
            d21_monto += total_amount
        if days_ago <= 30:
            mes_unidades += units
            mes_monto += total_amount
        if days_ago <= 59:
            d60_unidades += units
            d60_monto += total_amount
        if days_ago <= 89:
            d90_unidades += units
            d90_monto += total_amount
        if primer_dia_mes <= dt <= today_local:
            ventas_mes_actual_unid += units
            ventas_mes_actual_monto += total_amount
            items = ord_item.get("order_items") or ord_item.get("items") or []
            for it in items:
                if not isinstance(it, dict):
                    continue
                obj = it.get("item") or it
                qty = int(it.get("quantity") or it.get("qty") or 0)
                if qty <= 0:
                    continue
                titulo = (obj.get("title") if isinstance(obj, dict) else None) or it.get("title") or "Sin nombre"
                iid = (str(obj.get("id") or it.get("item_id") or "") if isinstance(obj, dict) else str(it.get("item_id") or "")).strip()
                key_id = iid or titulo[:80]
                if key_id not in top_productos:
                    top_productos[key_id] = {"title": titulo, "units": 0}
                top_productos[key_id]["units"] += qty
        key = dt.strftime("%Y-%m")
        if key not in por_mes:
            por_mes[key] = {"units": 0, "total": 0.0}
        por_mes[key]["units"] += units
        por_mes[key]["total"] += total_amount

    # Si se obtuvo conteo directo de /shipments/search, tiene prioridad sobre el loop
    if shipments_today is not None:
        flex_hoy = shipments_today.get("flex", 0)
        me_hoy = shipments_today.get("me", 0)

    # Incluir siempre el mes actual aunque no tenga ventas (para que el gráfico muestre marzo, etc.)
    mes_actual_key = today_local.strftime("%Y-%m")
    if mes_actual_key not in por_mes:
        por_mes[mes_actual_key] = {"units": 0, "total": 0.0}
    meses_orden = sorted(por_mes.keys(), reverse=True)[:6]  # Solo 6 meses para caber en pantalla

    container.clear()
    with container:
        _CARD = "background:#fff;border:1px solid #e0e2e7;border-radius:10px;padding:14px 16px"
        _CARD_NP = "background:#fff;border:1px solid #e0e2e7;border-radius:10px"
        _LBL = "font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:.05em;font-weight:500;margin-bottom:2px"
        _BLUE = "#1d4ed8"
        _GREEN = "#16a34a"

        with ui.column().classes("w-full gap-3"):
            # ── HEADER + KPI ROW ──────────────────────────────────────────────────
            prof = profile or {}

            secure_thumb = _safe_str(prof.get("secure_thumbnail")) or _safe_str(prof.get("thumbnail"))
            logo = _safe_str(prof.get("logo"))
            img_url = logo or secure_thumb
            nickname = _safe_str(prof.get("nickname")) or _safe_str(prof.get("first_name")) or "Usuario ML"
            power = _safe_str(prof.get("power_seller_status"))
            dolar_kpi_str = (get_cotizador_param("dolar_oficial", user_id) or "1475") if user_id else "1475"
            try:
                dolar_kpi = float(str(dolar_kpi_str).replace(",", ".").strip())
                if dolar_kpi <= 0:
                    dolar_kpi = 1475.0
            except (TypeError, ValueError):
                dolar_kpi = 1475.0
            mes_usd_kpi = ventas_mes_actual_monto / dolar_kpi if dolar_kpi > 0 else 0
            ticket_prom_kpi = (ventas_mes_actual_monto / ventas_mes_actual_unid) if ventas_mes_actual_unid > 0 else 0

            meses_nombres = {1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 5: "Mayo", 6: "Junio",
                            7: "Julio", 8: "Agosto", 9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"}
            mes_actual_nom = meses_nombres.get(today_local.month, today_local.strftime("%B"))

            no_concretadas = max(0, hoy_unidades - flex_hoy - me_hoy)
            nc_color = "#dc2626" if no_concretadas > 0 else "#6b7280"

            with ui.row().classes("w-full gap-2 flex-wrap items-stretch"):
                # BLOQUE 1 — Tienda
                with ui.element("div").style("flex:1.1;min-width:280px;background:#fff;border:1px solid #e0e2e7;border-radius:10px;padding:10px 14px"):
                    with ui.element("div").style("display:flex;align-items:center;justify-content:space-between;border-bottom:2px solid #1d4ed8;padding-bottom:5px;margin-bottom:8px"):
                        ui.label("TIENDA").style("font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:.04em;font-weight:500")
                        if on_refresh:
                            ui.button("↻ Actualizar", on_click=lambda: on_refresh()).props("flat dense").style(f"font-size:10px;color:{_BLUE};padding:0;min-height:0")
                    with ui.element("div").style("display:flex;align-items:center;gap:10px"):
                        if img_url:
                            ui.image(img_url).style("width:40px;height:40px;object-fit:cover;border-radius:8px;flex-shrink:0;border:1px solid #e0e2e7")
                        else:
                            initials = "".join(w[0].upper() for w in nickname.split()[:2]) if nickname else "ML"
                            with ui.element("div").style(f"width:40px;height:40px;border-radius:50%;background:{_BLUE};display:flex;align-items:center;justify-content:center;flex-shrink:0"):
                                ui.label(initials).style("color:white;font-size:15px;font-weight:700;line-height:1")
                        with ui.element("div").style("flex:1;min-width:0"):
                            ui.label(nickname).style(f"font-size:14px;font-weight:700;color:{_BLUE};overflow:hidden;text-overflow:ellipsis;white-space:nowrap")
                            if power:
                                with ui.element("span").style(f"background:#eff6ff;color:{_BLUE};font-size:9px;font-weight:600;padding:2px 7px;border-radius:12px;display:inline-block;margin-top:3px"):
                                    ui.label(f"MercadoLíder {power.capitalize()}")

                # BLOQUE 2 — Operaciones de hoy
                with ui.element("div").style("flex:2;min-width:280px;background:#fff;border:1px solid #e0e2e7;border-radius:10px;padding:10px 14px"):
                    with ui.element("div").style("border-bottom:2px solid #1d4ed8;padding-bottom:5px;margin-bottom:8px"):
                        ui.label("OPERACIONES DE HOY").style("font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:.04em;font-weight:500")
                    with ui.element("div").style("display:flex;align-items:flex-start;flex-wrap:wrap"):
                        with ui.element("div").style("flex:1;padding-right:14px;border-right:0.5px solid #e5e7eb"):
                            ui.label("VENTAS HOY").style("font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:.04em")
                            ui.label(str(hoy_unidades)).style(f"font-size:22px;font-weight:600;color:{_BLUE};line-height:1.2")
                            ui.label(fmt_m(hoy_monto)).style("font-size:11px;color:#6b7280")
                        with ui.element("div").style("flex:1;padding:0 14px;border-right:0.5px solid #e5e7eb"):
                            ui.label("MOTO FLEX HOY").style("font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:.04em")
                            ui.label(fmt_n(flex_hoy)).style("font-size:22px;font-weight:600;color:#6b7280;line-height:1.2")
                            ui.label("órdenes").style("font-size:11px;color:#6b7280")
                        with ui.element("div").style("flex:1;padding:0 14px;border-right:0.5px solid #e5e7eb"):
                            ui.label("CORREO HOY").style("font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:.04em")
                            ui.label(fmt_n(me_hoy)).style("font-size:22px;font-weight:600;color:#6b7280;line-height:1.2")
                            ui.label("órdenes").style("font-size:11px;color:#6b7280")
                        with ui.element("div").style("flex:1;padding-left:14px"):
                            ui.label("NO CONCRETADAS").style("font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:.04em")
                            ui.label(fmt_n(no_concretadas)).style("font-size:22px;font-weight:600;color:#6b7280;line-height:1.2")
                            ui.label("cancel./pend.").style("font-size:11px;color:#6b7280")

                # BLOQUE 3 — Facturación mes
                with ui.element("div").style("flex:1.3;min-width:280px;background:#fff;border:1px solid #e0e2e7;border-radius:10px;padding:10px 14px"):
                    with ui.element("div").style("border-bottom:2px solid #16a34a;padding-bottom:5px;margin-bottom:8px"):
                        ui.label(f"FACTURACIÓN — {mes_actual_nom.upper()}").style("font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:.04em;font-weight:500")
                    with ui.element("div").style("display:flex;align-items:flex-start;flex-wrap:wrap"):
                        with ui.element("div").style("flex:1;padding-right:14px;border-right:0.5px solid #e5e7eb"):
                            ui.label("FACTURADO").style("font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:.04em")
                            ui.label(fmt_m(ventas_mes_actual_monto)).style(f"font-size:17px;font-weight:600;color:{_GREEN};line-height:1.2")
                            ui.label(f"u$ {fmt_n(mes_usd_kpi)}").style("font-size:11px;color:#6b7280")
                        with ui.element("div").style("flex:1;padding-left:14px"):
                            ui.label("TICKET PROM").style("font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:.04em")
                            ui.label(fmt_m(ticket_prom_kpi)).style(f"font-size:17px;font-weight:600;color:{_BLUE};line-height:1.2")
                            ui.label(f"{fmt_n(ventas_mes_actual_unid)} unidades").style("font-size:11px;color:#6b7280")

            # ── FILA 1: Reputación | Ventas períodos | Facturación | Históricas ───
            metrics = rep.get("metrics", {}) or rep.get("transactions", {}) or {}
            sales_meta = metrics.get("sales", {}) or {}
            completed = sales_meta.get("completed") or 0
            claims = metrics.get("claims", {}) or metrics.get("disputes", {}) or {}
            canc = metrics.get("cancellations", {}) or {}
            delayed = metrics.get("delayed_handling_time", {}) or {}
            mediat = metrics.get("mediations", {}) or metrics.get("disputes", {}) or {}

            def _get_rate(m: Dict[str, Any], total_completed: float = 0) -> Any:
                exc = m.get("excluded") or {}
                if isinstance(exc.get("real_rate"), (int, float)):
                    return exc["real_rate"]
                if isinstance(exc.get("real_value"), (int, float)) and total_completed > 0:
                    return exc["real_value"] / total_completed
                if isinstance(m.get("rate"), (int, float)):
                    return m["rate"]
                if isinstance(m.get("value"), (int, float)) and total_completed > 0:
                    return m["value"] / total_completed
                return None

            try:
                tot = float(completed) if completed else 0
            except (TypeError, ValueError):
                tot = 0
            rate_claims = _get_rate(claims, tot)
            rate_canc = _get_rate(canc, tot)
            rate_delayed = _get_rate(delayed, tot)
            rate_mediat = _get_rate(mediat, tot) if mediat else None
            level_id = rep.get("level_id") or "—"
            level_label = {"1_red": "Rojo", "2_orange": "Naranja", "3_yellow": "Amarillo", "4_light_green": "Verde claro", "5_green": "Verde"}.get(str(level_id), str(level_id))
            level_colors = {"1_red": "#ef4444", "2_orange": "#f97316", "3_yellow": "#eab308", "4_light_green": "#84cc16", "5_green": "#22c55e"}
            level_color = level_colors.get(str(level_id), "#6b7280")
            MAX_CLAIMS, MAX_MEDIAT, MAX_CANC, MAX_DELAYED = 0.01, 0.005, 0.005, 0.08

            def _to_float_rate(v: Any) -> Optional[float]:
                if v is None:
                    return None
                try:
                    x = float(v)
                    return x if 0 < x <= 1 else x / 100.0
                except (TypeError, ValueError):
                    return None

            def _semaforo(rate_raw: Any, max_val: float, label: str) -> None:
                rate_f = _to_float_rate(rate_raw)
                if rate_f is None:
                    rate_pct_str = "—"
                    color = "#9ca3af"
                    bar_pct = 0.0
                elif rate_f == 0:
                    rate_pct_str = "0,00%"
                    color = "#3B6D11"
                    bar_pct = 0.0
                else:
                    rate_pct_str = f"{rate_f * 100:.2f}%".replace(".", ",")
                    ratio = rate_f / max_val if max_val > 0 else 1.0
                    if ratio < 0.5:
                        color = "#16a34a"
                    elif ratio < 0.9:
                        color = "#f59e0b"
                    else:
                        color = "#ef4444"
                    bar_pct = min(ratio * 100, 100)
                with ui.element("div").style("margin-bottom:5px"):
                    with ui.element("div").style("display:flex;align-items:center;gap:6px"):
                        with ui.element("div").style(f"width:8px;height:8px;border-radius:50%;background:{color};flex-shrink:0"):
                            pass
                        ui.label(label).style("font-size:11px;flex:1;color:#374151")
                        ui.label(rate_pct_str).style(f"font-size:11px;font-weight:600;color:{color}")
                    with ui.element("div").style("height:3px;border-radius:2px;background:#f3f4f6;margin-top:3px"):
                        with ui.element("div").style(f"height:3px;border-radius:2px;background:{color};width:{bar_pct:.1f}%"):
                            pass

            def _pct_fmt(val: Any) -> str:
                if val is None:
                    return "—"
                try:
                    v = float(val)
                    return f"{(v * 100 if 0 <= v <= 1 else v):.2f}%"
                except (TypeError, ValueError):
                    return "—"

            with ui.row().classes("w-full gap-2 flex-wrap items-stretch overflow-hidden max-w-full"):
                # Card Reputación
                with ui.element("div").style(f"flex:1;min-width:220px;{_CARD_NP};overflow:hidden;flex-shrink:0"):
                    with ui.element("div").style("padding:12px 14px"):
                        ui.label("REPUTACIÓN").style(f"{_LBL};margin-bottom:8px")
                        with ui.row().classes("gap-2 items-center mb-3"):
                            with ui.element("div").style(f"width:10px;height:10px;border-radius:50%;background:{level_color};flex-shrink:0"):
                                pass
                            ui.label(f"Nivel: {level_label}").style(f"color:{level_color};font-weight:600;font-size:13px")
                        _semaforo(rate_claims, MAX_CLAIMS, f"Reclamos (máx {MAX_CLAIMS*100:.0f}%)")
                        _semaforo(rate_mediat, MAX_MEDIAT, f"Mediaciones (máx {MAX_MEDIAT*100:.1f}%)")
                        _semaforo(rate_canc, MAX_CANC, f"Cancelaciones (máx {MAX_CANC*100:.1f}%)")
                        _semaforo(rate_delayed, MAX_DELAYED, f"Demora envíos (máx {MAX_DELAYED*100:.0f}%)")
                        if questions is not None:
                            n_q = len(questions)
                            q_color = "#3B6D11" if n_q == 0 else "#A32D2D"
                            with ui.element("div").style("margin-bottom:5px"):
                                with ui.element("div").style("display:flex;align-items:center;gap:6px"):
                                    with ui.element("div").style(
                                        f"width:8px;height:8px;border-radius:50%;"
                                        f"background:{q_color};flex-shrink:0"):
                                        pass
                                    ui.label("Preguntas sin responder").style("font-size:11px;flex:1;color:#374151")
                                    ui.label(str(n_q)).style(f"font-size:11px;font-weight:600;color:{q_color}")

                # Card Ventas períodos
                with ui.element("div").style(f"flex:1;min-width:300px;{_CARD_NP};overflow:hidden;flex-shrink:0"):
                    with ui.element("div").style("padding:12px 14px"):
                        ui.label("VENTAS POR PERÍODO").style(f"{_LBL};margin-bottom:6px")
                        def _mini(lbl, unid, monto, bg, bdr, col):
                            with ui.element("div").style(f"flex:1;min-width:0;padding:5px 7px;border-radius:4px;background:{bg};border:1px solid {bdr}"):
                                ui.label(lbl).style(f"font-size:10px;color:{col};font-weight:500")
                                ui.label(fmt_n(unid)).style(f"font-size:13px;font-weight:700;color:{col}")
                                ui.label(fmt_m(monto)).style("font-size:9px;color:#6b7280;white-space:nowrap")
                        with ui.column().classes("gap-1 w-full"):
                            with ui.row().classes("gap-1 w-full flex-nowrap"):
                                _mini("Hoy", hoy_unidades, hoy_monto, "#eff6ff", "#bfdbfe", _BLUE)
                                _mini("Ayer", ayer_unidades, ayer_monto, "#f9fafb", "#e5e7eb", "#374151")
                                _mini("Antes de ayer", antes_ayer_unidades, antes_ayer_monto, "#f9fafb", "#e5e7eb", "#374151")
                            with ui.row().classes("gap-1 w-full flex-nowrap"):
                                _mini("7 días", semana_unidades, semana_monto, "#f9fafb", "#e5e7eb", "#374151")
                                _mini("15 días", d15_unidades, d15_monto, "#f9fafb", "#e5e7eb", "#374151")
                                _mini("21 días", d21_unidades, d21_monto, "#f9fafb", "#e5e7eb", "#374151")
                            with ui.row().classes("gap-1 w-full flex-nowrap"):
                                _mini("30 días", mes_unidades, mes_monto, "#f0fdf4", "#d1fae5", _GREEN)
                                _mini("60 días", d60_unidades, d60_monto, "#f9fafb", "#e5e7eb", "#374151")
                                _mini("90 días", d90_unidades, d90_monto, "#f9fafb", "#e5e7eb", "#374151")

                # Card Facturación Mensual (echart)
                if meses_orden:
                    orden_rev = list(reversed(meses_orden))
                    meses_abr = {"01": "ene", "02": "feb", "03": "mar", "04": "abr", "05": "may", "06": "jun",
                                 "07": "jul", "08": "ago", "09": "sep", "10": "oct", "11": "nov", "12": "dic"}
                    chart_labels = [f"{meses_abr.get(k[5:7], k[5:7])}-{k[2:4]}" for k in orden_rev]
                    chart_data = []
                    for i, k in enumerate(orden_rev):
                        val_m = por_mes[k]["total"]
                        is_actual = i == len(orden_rev) - 1
                        is_reciente = i >= len(orden_rev) - 3
                        if is_actual:
                            bar_color = _GREEN
                        elif is_reciente:
                            bar_color = "#3b82f6"
                        else:
                            bar_color = "#bfdbfe"
                        monto_str = fmt_m(val_m)
                        if i > 0:
                            val_prev = por_mes[orden_rev[i - 1]]["total"]
                            if val_prev > 0:
                                pct_var = (val_m - val_prev) / val_prev * 100
                                pct_str = f"{pct_var:+.1f}%"
                                rich_key = "pctpos" if pct_var >= 0 else "pctneg"
                                lbl_fmt = f"{{{rich_key}|{pct_str}}}\n{{monto|{monto_str}}}"
                            else:
                                lbl_fmt = f"{{monto|{monto_str}}}"
                        else:
                            lbl_fmt = f"{{monto|{monto_str}}}"
                        chart_data.append({
                            "value": round(val_m, 0),
                            "itemStyle": {"color": bar_color},
                            "label": {"formatter": lbl_fmt},
                        })
                    _dias_t_est = (today_local - primer_dia_mes).days + 1
                    _dias_m_est = calendar.monthrange(today_local.year, today_local.month)[1]
                    if _dias_t_est < _dias_m_est and ventas_mes_actual_monto > 0:
                        _venta_est = (ventas_mes_actual_monto / _dias_t_est) * _dias_m_est
                        _val_mes_ant = por_mes[orden_rev[-2]]["total"] if len(orden_rev) >= 2 else 0
                        _monto_est_str = fmt_m(_venta_est)
                        if _val_mes_ant > 0:
                            _pct_est = (_venta_est - _val_mes_ant) / _val_mes_ant * 100
                            _rich_est = "pctpos" if _pct_est >= 0 else "pctneg"
                            _lbl_est = f"{{{_rich_est}|{_pct_est:+.1f}%}}\n{{monto|{_monto_est_str}}}"
                        else:
                            _lbl_est = f"{{monto|{_monto_est_str}}}"
                        _mes_abr_est = meses_abr.get(today_local.strftime("%m"), today_local.strftime("%m"))
                        chart_labels.append(f"{_mes_abr_est}-{today_local.strftime('%y')} Est.")
                        chart_data.append({
                            "value": round(_venta_est, 0),
                            "itemStyle": {"color": "#86efac"},
                            "label": {"formatter": _lbl_est},
                        })
                    chart_options = {
                        "backgroundColor": "transparent",
                        "grid": {"left": 5, "right": 5, "top": 42, "bottom": 35, "containLabel": False},
                        "xAxis": {"type": "category", "data": chart_labels, "axisLabel": {"fontSize": 10, "interval": 0}},
                        "yAxis": {"show": False},
                        "series": [{
                            "type": "bar",
                            "data": chart_data,
                            "barWidth": "55%",
                            "label": {
                                "show": True,
                                "position": "top",
                                "textAlign": "center",
                                "rich": {
                                    "pctpos": {"color": "#16a34a", "fontSize": 9, "fontWeight": "bold", "align": "center"},
                                    "pctneg": {"color": "#dc2626", "fontSize": 9, "fontWeight": "bold", "align": "center"},
                                    "monto": {"color": "#111827", "fontSize": 9, "align": "center"},
                                },
                            },
                        }],
                    }
                    with ui.element("div").style(f"flex:1;min-width:280px;{_CARD_NP};overflow:hidden;min-height:185px;flex-shrink:0"):
                        with ui.element("div").style("padding:10px 14px 4px"):
                            ui.label("FACTURACIÓN MENSUAL").style(_LBL)
                        ui.echart(chart_options).classes("w-full").style("height:200px")
                else:
                    with ui.element("div").style(f"flex:1;min-width:120px;{_CARD};flex-shrink:0"):
                        ui.label("FACTURACIÓN MENSUAL").style(_LBL)
                        ui.label("Sin datos").style("font-size:12px;color:#9ca3af;margin-top:6px")

                # Card Ventas Históricas
                with ui.element("div").style(f"flex:1;min-width:240px;{_CARD_NP};overflow:hidden;flex-shrink:0"):
                    with ui.element("div").style("padding:12px 14px"):
                        ui.label("VENTAS HISTÓRICAS").style(f"{_LBL};margin-bottom:8px")
                        if not meses_orden:
                            trans = rep.get("transactions", {}) or {}
                            tot_trans = trans.get("total") or trans.get("completed") or 0
                            ui.label(f"Sin datos (perfil: {tot_trans} trans.)" if tot_trans else "No hay órdenes").style("font-size:12px;color:#9ca3af")
                        else:
                            dolar_str = get_cotizador_param("dolar_oficial", user_id) or "1475"
                            dolar_oficial = float(str(dolar_str).replace(",", ".").strip()) if dolar_str else 1475.0
                            if dolar_oficial <= 0:
                                dolar_oficial = 1475.0
                            with ui.element("table").style("width:100%;border-collapse:collapse;font-size:11px"):
                                with ui.element("thead"):
                                    with ui.element("tr").style("background:#f9fafb"):
                                        for ci, col_h in enumerate(["Mes", "Unid", "$ ARS", "u$ USD"]):
                                            align = "left" if ci == 0 else "right"
                                            with ui.element("th").style(f"padding:4px 8px;text-align:{align};font-weight:600;font-size:10px;text-transform:uppercase;color:#6b7280;border-bottom:1px solid #e0e2e7"):
                                                ui.label(col_h)
                                with ui.element("tbody"):
                                    for ri, key in enumerate(meses_orden):
                                        v = por_mes[key]
                                        total_usd = (v["total"] / dolar_oficial) if dolar_oficial else 0.0
                                        is_mes_actual = key == mes_actual_key
                                        row_bg = "#eff6ff" if is_mes_actual else ("#ffffff" if ri % 2 == 0 else "#fafafa")
                                        row_color = _BLUE if is_mes_actual else "#374151"
                                        with ui.element("tr").style(f"background:{row_bg};border-bottom:1px solid #f3f4f6"):
                                            with ui.element("td").style("padding:4px 8px;text-align:left"):
                                                if is_mes_actual:
                                                    ui.label(key).style(f"font-size:11px;color:{_BLUE};font-weight:600")
                                                else:
                                                    ui.label(key).style("font-size:11px;color:#374151")
                                            with ui.element("td").style(f"padding:4px 8px;text-align:right;font-weight:{'700' if is_mes_actual else '400'};color:{row_color}"):
                                                ui.label(fmt_n(v["units"]))
                                            with ui.element("td").style(f"padding:4px 8px;text-align:right;font-weight:{'700' if is_mes_actual else '400'};color:{row_color}"):
                                                ui.label(fmt_m(v["total"]))
                                            with ui.element("td").style(f"padding:4px 8px;text-align:right;font-weight:{'700' if is_mes_actual else '400'};color:{row_color if is_mes_actual else '#6b7280'}"):
                                                ui.label(f"u$ {fmt_n(total_usd)}")

            # ── FILA 2: Top Ventas | Stock | Graf Semanal | Ventas Mes ────────────
            claims_val = (claims.get("value") or claims.get("excluded", {}).get("real_value") or 0)
            mediat_val = (mediat.get("value") or mediat.get("excluded", {}).get("real_value") or 0) if mediat else 0
            canc_val = (canc.get("value") or canc.get("excluded", {}).get("real_value") or 0)
            postventa_total = claims_val + mediat_val + canc_val

            ventas_por_dia: Dict[str, int] = {}
            facturacion_por_dia: Dict[str, float] = {}
            dias_semana_es = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]
            for d in range(14):
                fd = today_local - timedelta(days=d)
                ventas_por_dia[fd.strftime("%Y-%m-%d")] = 0
                facturacion_por_dia[fd.strftime("%Y-%m-%d")] = 0.0
            for ord_item in results:
                dt_str = ord_item.get("date_created") or ord_item.get("date_closed") or ""
                if not dt_str:
                    continue
                try:
                    dt = datetime.strptime(dt_str[:10], "%Y-%m-%d").date()
                except Exception:
                    continue
                if (today_local - dt).days > 13:
                    continue
                items_ord = ord_item.get("order_items") or ord_item.get("items") or []
                units_ord = sum(int(it.get("quantity") or it.get("qty") or 0) for it in items_ord if isinstance(it, dict))
                if units_ord == 0:
                    total_amount_ord = ord_item.get("total_amount") or ord_item.get("paid_amount") or 0
                    if total_amount_ord and float(total_amount_ord or 0) > 0:
                        units_ord = 1
                key_ord = dt.strftime("%Y-%m-%d")
                if key_ord in ventas_por_dia:
                    ventas_por_dia[key_ord] += units_ord
                if key_ord in facturacion_por_dia:
                    facturacion_por_dia[key_ord] += float(ord_item.get("total_amount") or ord_item.get("paid_amount") or 0)

            with ui.row().classes("w-full gap-2 flex-wrap items-stretch mt-1"):
                # Card Top Ventas
                top_list = sorted(top_productos.values(), key=lambda x: x["units"], reverse=True)[:14]
                total_unid_mes = ventas_mes_actual_unid if ventas_mes_actual_unid > 0 else 1

                with ui.element("div").style(f"flex:1;min-width:260px;{_CARD_NP};overflow:hidden;flex-shrink:0"):
                    with ui.element("div").style("padding:12px 14px"):
                        ui.label(f"TOP VENTAS — {mes_actual_nom.upper()}").style(f"{_LBL};margin-bottom:8px")
                        if not top_list:
                            ui.label("Sin ventas este mes").style("font-size:12px;color:#9ca3af")
                        else:
                            for i, p in enumerate(top_list):
                                pct = (100.0 * p["units"] / total_unid_mes) if total_unid_mes else 0
                                tit = (p["title"] or "—")[:45]
                                if len(p.get("title") or "") > 45:
                                    tit += "…"
                                with ui.row().classes("w-full items-center gap-2 mb-1"):
                                    with ui.element("div").style(f"width:16px;height:16px;border-radius:50%;background:{_BLUE};display:flex;align-items:center;justify-content:center;flex-shrink:0"):
                                        ui.label(str(i + 1)).style("color:white;font-size:8px;font-weight:700")
                                    ui.label(tit).style("font-size:11px;color:#111827;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;flex:1;min-width:0")
                                    with ui.element("div").style("display:flex;align-items:center;gap:2px;flex-shrink:0"):
                                        ui.label(f"{p['units']}u").style(f"font-size:11px;color:{_BLUE};font-weight:500;white-space:nowrap")
                                        ui.label(f"· {pct:.1f}%").style("font-size:11px;color:#6b7280;white-space:nowrap")

                # Card Stock + Últimas ventas
                items_list = (items_data or {}).get("results") or []
                # Deduplicar por SKU — misma lógica que Productos
                _groups: Dict[tuple, list] = {}
                for _it in items_list:
                    if isinstance(_it, dict):
                        _groups.setdefault(_cuotas_key(_it), []).append(_it)
                items_list_dedup: list = []
                for _grupo in _groups.values():
                    if len(_grupo) == 1:
                        items_list_dedup.append(_grupo[0])
                    else:
                        _principal = max(
                            _grupo,
                            key=lambda x: (
                                1 if not x.get("catalog_listing") and
                                     str(x.get("listing_type_id") or "").lower() == "gold_special" else 0,
                                int(x.get("available_quantity") or 0),
                            ),
                        )
                        _fusionado = dict(_principal)
                        _fusionado["sold_quantity"] = sum(int(x.get("sold_quantity") or 0) for x in _grupo)
                        items_list_dedup.append(_fusionado)
                propias = [it for it in items_list_dedup if it.get("catalog_listing") is not True]
                publicaciones_propias_con_stock = sum(1 for it in propias if (it.get("available_quantity") or 0) > 0)
                unidades_propias_en_stock = sum(int(it.get("available_quantity") or 0) for it in propias)
                marcas_propias = [str(it.get("marca") or "").strip() for it in propias]
                marcas_distintas = len({m for m in marcas_propias if m and m != "—"})
                def _orden_fecha(o):
                    ds = o.get("date_closed") or o.get("date_created") or o.get("date_last_updated") or ""
                    return ds[:10] if ds else ""
                ultimas_5_ventas = sorted(results, key=_orden_fecha, reverse=True)[:10]

                with ui.element("div").style(f"flex:1;min-width:260px;{_CARD_NP};overflow:hidden;flex-shrink:0"):
                    with ui.element("div").style("padding:12px 14px"):
                        ui.label("STOCK PROPIAS").style(f"{_LBL};margin-bottom:6px")
                        with ui.row().classes("gap-2 w-full flex-nowrap mb-3"):
                            with ui.element("div").style(f"flex:1;text-align:center;padding:6px 4px;background:#f9fafb;border:1px solid #e5e7eb;border-radius:4px"):
                                ui.label("Publicaciones propias").style("font-size:8px;color:#6b7280")
                                ui.label(str(publicaciones_propias_con_stock)).style(f"font-size:16px;font-weight:700;color:{_BLUE}")
                            with ui.element("div").style(f"flex:1;text-align:center;padding:6px 4px;background:#f9fafb;border:1px solid #e5e7eb;border-radius:4px"):
                                ui.label("Unidades propias").style("font-size:8px;color:#6b7280")
                                ui.label(fmt_n(unidades_propias_en_stock)).style(f"font-size:16px;font-weight:700;color:{_BLUE}")
                            with ui.element("div").style(f"flex:1;text-align:center;padding:6px 4px;background:#f9fafb;border:1px solid #e5e7eb;border-radius:4px"):
                                ui.label("Marcas").style("font-size:9px;color:#6b7280")
                                ui.label(str(marcas_distintas)).style(f"font-size:16px;font-weight:700;color:{_BLUE}")
                        ui.label("ÚLTIMAS VENTAS").style(f"{_LBL};margin-bottom:4px")
                        if ultimas_5_ventas:
                            _tz_arg = timezone(timedelta(hours=-3))
                            for v in ultimas_5_ventas:
                                ds_raw = v.get("date_closed") or v.get("date_created") or v.get("date_last_updated") or ""
                                try:
                                    _s = re.sub(r'\.\d+', '', ds_raw)
                                    if "T" in _s:
                                        if re.search(r'[+-]\d{2}:\d{2}$', _s):
                                            dt_v = datetime.fromisoformat(_s).astimezone(_tz_arg)
                                        else:
                                            dt_v = datetime.strptime(_s[:19], "%Y-%m-%dT%H:%M:%S")
                                    elif " " in _s:
                                        dt_v = datetime.strptime(_s[:16], "%Y-%m-%d %H:%M")
                                    elif len(_s) >= 10:
                                        dt_v = datetime.strptime(_s[:10], "%Y-%m-%d")
                                    else:
                                        dt_v = None
                                    hora_fmt = f"{dt_v.hour:02d}:{dt_v.minute:02d}" if dt_v else ""
                                except Exception:
                                    hora_fmt = ""
                                items_v = v.get("order_items") or v.get("items") or []
                                uds = sum(int(it.get("quantity") or it.get("qty") or 0) for it in items_v if isinstance(it, dict))
                                if uds == 0:
                                    total = v.get("total_amount") or v.get("paid_amount") or 0
                                    uds = 1 if float(total or 0) > 0 else 0
                                primer_item = items_v[0] if items_v else {}
                                obj = primer_item.get("item") or primer_item
                                tit = (obj.get("title") if isinstance(obj, dict) else primer_item.get("title")) or "—"
                                tit = (tit[:55] + "…") if len(str(tit)) > 55 else str(tit)
                                with ui.row().classes("w-full items-center justify-between gap-2 py-0.5 flex-nowrap"):
                                    ui.label(tit).style("font-size:11px;color:#111827;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;min-width:0;flex:1")
                                    ui.label(f"{uds}u").style(f"font-size:11px;font-weight:700;color:{_BLUE};white-space:nowrap;flex-shrink:0")
                                    ui.label(hora_fmt).style("font-size:11px;color:#6b7280;white-space:nowrap;flex-shrink:0")

                # Card Gráfico Semanal — 14 días
                dias_orden = sorted(ventas_por_dia.keys())[-14:]
                uds_esta_semana = sum(ventas_por_dia.get((today_local - timedelta(days=d)).strftime("%Y-%m-%d"), 0) for d in range(7))
                uds_semana_pasada = sum(ventas_por_dia.get((today_local - timedelta(days=d)).strftime("%Y-%m-%d"), 0) for d in range(7, 14))
                var_pct = ((uds_esta_semana - uds_semana_pasada) / uds_semana_pasada * 100) if uds_semana_pasada > 0 else (100.0 if uds_esta_semana > 0 else 0.0)
                def _fmt_compacto(val: float) -> str:
                    if val >= 1_000_000:
                        return f"${val/1_000_000:.1f}M"
                    elif val >= 1_000:
                        return f"${val/1_000:.0f}K"
                    return f"${int(val)}"

                if dias_orden:
                    chart_labels_sem = []
                    chart_data_sem = []
                    for i, key in enumerate(dias_orden):
                        fd = datetime.strptime(key, "%Y-%m-%d").date()
                        dia_sem = dias_semana_es[fd.weekday()]
                        chart_labels_sem.append(f"{dia_sem} {fd.day}")
                        uds_s = ventas_por_dia.get(key, 0)
                        fact_s = facturacion_por_dia.get(key, 0.0)
                        days_back = (today_local - fd).days
                        if days_back == 0:
                            bar_color_s = _GREEN
                        elif days_back <= 6:
                            bar_color_s = "#3b82f6"
                        else:
                            bar_color_s = "#e5e7eb"
                        fact_str = _fmt_compacto(fact_s)
                        lbl_fmt = f"{{fact|{fact_str}}}\n{{uds|{uds_s}}}"
                        chart_data_sem.append({"value": uds_s, "itemStyle": {"color": bar_color_s}, "label": {"formatter": lbl_fmt}})
                    chart_options_sem = {
                        "backgroundColor": "transparent",
                        "grid": {"left": 35, "right": 15, "top": 60, "bottom": 25},
                        "xAxis": {"type": "category", "data": chart_labels_sem, "axisLabel": {"fontSize": 9, "interval": 0, "rotate": 30}},
                        "yAxis": {"type": "value", "axisLabel": {"fontSize": 9}},
                        "series": [{"type": "bar", "data": chart_data_sem, "barWidth": "60%", "label": {
                            "show": True,
                            "position": "top",
                            "rich": {
                                "fact": {"color": "#6b7280", "fontSize": 8, "align": "center"},
                                "uds":  {"color": "#111827", "fontSize": 9, "fontWeight": "bold", "align": "center"},
                            },
                        }}],
                    }
                    with ui.element("div").style(f"flex:1;min-width:280px;{_CARD_NP};overflow:hidden;min-height:185px;flex-shrink:0"):
                        with ui.element("div").style("padding:10px 14px 4px"):
                            ui.label("UNIDADES VENDIDAS — 14 DÍAS").style(_LBL)
                        ui.echart(chart_options_sem).classes("w-full").style("height:220px")
                        with ui.element("div").style("padding:4px 14px 10px"):
                            prom_7 = uds_esta_semana / 7
                            hoy_u = ventas_por_dia.get(today_local.strftime("%Y-%m-%d"), 0)
                            hoy_vs_prom = ((hoy_u - prom_7) / prom_7 * 100) if prom_7 > 0 else (100.0 if hoy_u > 0 else 0.0)
                            variacion_color = _GREEN if var_pct >= 0 else "#dc2626"
                            hoy_vs_color = _GREEN if hoy_vs_prom >= 0 else "#dc2626"
                            _CELL = "background:#f9fafb;border:0.5px solid #e5e7eb;border-radius:0 4px 4px 0;padding:5px 8px;display:flex;justify-content:space-between;align-items:center"
                            with ui.element("div").style("display:grid;grid-template-columns:1fr 1fr;gap:4px"):
                                with ui.element("div").style(f"{_CELL};border-left:3px solid #1d4ed8"):
                                    ui.label("Esta semana").style("font-size:10px;color:#6b7280")
                                    ui.label(f"{fmt_n(uds_esta_semana)} u").style("font-size:12px;font-weight:500;color:#1d4ed8")
                                with ui.element("div").style(f"{_CELL};border-left:3px solid #6b7280"):
                                    ui.label("Sem. anterior").style("font-size:10px;color:#6b7280")
                                    ui.label(f"{fmt_n(uds_semana_pasada)} u").style("font-size:12px;font-weight:500;color:#6b7280")
                                with ui.element("div").style(f"{_CELL};border-left:3px solid {variacion_color}"):
                                    ui.label("Variación").style("font-size:10px;color:#6b7280")
                                    ui.label(f"{var_pct:+.1f}%").style(f"font-size:12px;font-weight:500;color:{variacion_color}")
                                with ui.element("div").style(f"{_CELL};border-left:3px solid {hoy_vs_color}"):
                                    ui.label("Hoy vs prom 7d").style("font-size:10px;color:#6b7280")
                                    ui.label(f"{hoy_vs_prom:+.0f}%").style(f"font-size:12px;font-weight:500;color:{hoy_vs_color}")
                else:
                    with ui.element("div").style(f"flex:1;min-width:120px;{_CARD};flex-shrink:0"):
                        ui.label("UNIDADES VENDIDAS — 14 DÍAS").style(_LBL)
                        ui.label("Sin datos").style("font-size:12px;color:#9ca3af;margin-top:6px")

                # Card Ventas del mes / Estimaciones
                dias_transcurridos = (today_local - primer_dia_mes).days + 1
                dias_del_mes = calendar.monthrange(today_local.year, today_local.month)[1]
                venta_diaria = ventas_mes_actual_monto / dias_transcurridos if dias_transcurridos > 0 else 0
                venta_estimada_mes = venta_diaria * dias_del_mes if dias_transcurridos > 0 else 0
                dolar_str2 = (get_cotizador_param("dolar_oficial", user_id) or "1475") if user_id else "1475"
                dolar_oficial2 = float(str(dolar_str2).replace(",", ".").strip()) if dolar_str2 else 1475.0
                if dolar_oficial2 <= 0:
                    dolar_oficial2 = 1475.0
                venta_estimada_mes_usd = (venta_estimada_mes / dolar_oficial2) if dolar_oficial2 > 0 else 0
                venta_diaria_u = ventas_mes_actual_unid / dias_transcurridos if dias_transcurridos > 0 else 0
                ticket_prom2 = (ventas_mes_actual_monto / ventas_mes_actual_unid) if ventas_mes_actual_unid > 0 else 0
                venta_x_unidad = ventas_mes_actual_monto / ventas_mes_actual_unid if ventas_mes_actual_unid > 0 else 0
                proyeccion_anual = (ventas_mes_actual_monto / dias_transcurridos * 365) if dias_transcurridos > 0 else 0

                with ui.element("div").style("flex:1;min-width:240px;flex-shrink:0;background:#fff;border:1px solid #e0e2e7;border-radius:10px;padding:12px"):
                    ui.label(f"VENTAS — {mes_actual_nom.upper()}").style(f"{_LBL};margin-bottom:8px")
                    # Bloque 1 — Resultados a la fecha
                    with ui.element("div").style("border-left:3px solid #1d4ed8;background:#f8faff;border-radius:0 6px 6px 0;padding:8px 10px;margin-bottom:6px"):
                        ui.label(f"RESULTADOS AL DÍA {dias_transcurridos}").style("font-size:10px;color:#0c447c;text-transform:uppercase;letter-spacing:.04em;font-weight:600;margin-bottom:4px")
                        ui.label(fmt_m(ventas_mes_actual_monto)).style("font-size:18px;font-weight:500;color:#1d4ed8;margin:3px 0;display:block")
                        with ui.element("div").style("display:flex;justify-content:space-between;padding:2px 0;font-size:11px"):
                            ui.label("Días transcurridos").style("color:#6b7280")
                            ui.label(f"{dias_transcurridos}/{dias_del_mes}").style("font-weight:500;color:#374151")
                        with ui.element("div").style("display:flex;justify-content:space-between;padding:2px 0;font-size:11px"):
                            ui.label("Unidades vendidas").style("color:#6b7280")
                            ui.label(fmt_n(ventas_mes_actual_unid)).style("font-weight:500;color:#374151")
                        with ui.element("div").style("display:flex;justify-content:space-between;padding:2px 0;font-size:11px"):
                            ui.label("Prom. diario").style("color:#6b7280")
                            ui.label(fmt_m(venta_diaria)).style("font-weight:500;color:#374151")
                        with ui.element("div").style("display:flex;justify-content:space-between;padding:2px 0;font-size:11px"):
                            ui.label("Ticket promedio").style("color:#6b7280")
                            ui.label(fmt_m(ticket_prom2)).style("font-weight:500;color:#374151")
                    # Bloque 2 — Estimación fin de mes
                    venta_estimada_unid = int(venta_diaria_u * dias_del_mes) if dias_transcurridos > 0 else 0
                    with ui.element("div").style("border-left:3px solid #16a34a;background:#f0fdf4;border-radius:0 6px 6px 0;padding:8px 10px"):
                        ui.label("ESTIMACIÓN FIN DE MES").style("font-size:10px;color:#15803d;text-transform:uppercase;letter-spacing:.04em;font-weight:600;margin-bottom:4px")
                        ui.label(fmt_m(venta_estimada_mes)).style("font-size:18px;font-weight:500;color:#16a34a;margin:3px 0;display:block")
                        with ui.element("div").style("display:flex;justify-content:space-between;padding:2px 0;font-size:11px"):
                            ui.label("En dólares").style("color:#6b7280")
                            ui.label(f"u$ {fmt_n(venta_estimada_mes_usd)}").style("font-weight:500;color:#374151")
                        with ui.element("div").style("display:flex;justify-content:space-between;padding:2px 0;font-size:11px"):
                            ui.label("Unidades estimadas").style("color:#6b7280")
                            ui.label(fmt_n(venta_estimada_unid)).style("font-weight:500;color:#374151")


# ---------------------------------------------------------------------------
# Tab principal
# ---------------------------------------------------------------------------

def build_tab_estadisticas(estadisticas_container) -> None:
    """Pestaña Estadísticas: datos de la cuenta ML, reputación y ventas. Carga síncrona con botón Actualizar."""
    user = _require_login()
    if not user:
        return

    access_token = get_ml_access_token(user["id"])
    if not access_token:
        with estadisticas_container:
            with ui.column().classes("w-full max-w-2xl gap-4"):
                ui.label("Bienvenido a BDC systems").classes("text-2xl font-semibold")
                ui.label(
                    "Conectá tu cuenta de MercadoLibre en Configuración para ver aquí tu perfil, reputación y ventas."
                ).classes("text-gray-600")
        return

    def cargar_y_pintar() -> None:
        estadisticas_container.clear()
        with estadisticas_container:
            with ui.card().classes("w-full p-8 items-center gap-4"):
                ui.spinner(size="xl")
                ui.label("Cargando datos...").classes("text-xl text-gray-700")
        background_tasks.create(_cargar_estadisticas_async(), name="cargar_estadisticas")

    async def _cargar_estadisticas_async() -> None:
        try:
            profile = await run.io_bound(ml_get_user_profile, access_token)
            seller_id = (profile or {}).get("id") or await run.io_bound(ml_get_user_id, access_token)
            orders_data: Dict[str, Any] = {}
            items_data: Dict[str, Any] = {"results": []}
            shipments_today: Dict[str, int] = {"flex": 0, "me": 0}
            if seller_id:
                limit_str = get_cotizador_param("estadisticas_limit_ordenes", user["id"]) or "1000"
                try:
                    limit_ordenes = int(limit_str)
                    if limit_ordenes not in (300, 500, 1000, 2000, 3000, 4000, 5000):
                        limit_ordenes = 1000
                except (ValueError, TypeError):
                    limit_ordenes = 1000
                orders_data = await run.io_bound(ml_get_orders, access_token, str(seller_id), limit_ordenes, 0)
                _tz_arg = timezone(timedelta(hours=-3))
                _today_str = datetime.now(_tz_arg).strftime("%Y-%m-%d")
                shipping_ids_hoy: List[str] = []
                for _ord in (orders_data.get("results") or []):
                    _dt_str = (_ord.get("date_created") or _ord.get("date_closed") or "")[:10]
                    if _dt_str == _today_str:
                        _ship_id = (_ord.get("shipping") or {}).get("id")
                        if _ship_id:
                            shipping_ids_hoy.append(str(_ship_id))
                try:
                    shipments_today = await run.io_bound(ml_get_shipments_today, access_token, shipping_ids_hoy)
                except Exception:
                    pass
            try:
                items_data = await run.io_bound(ml_get_my_items, access_token, False)
            except Exception:
                pass
            questions: List[Dict[str, Any]] = []
            if seller_id:
                try:
                    questions = await run.io_bound(ml_get_unanswered_questions, access_token, str(seller_id))
                except Exception:
                    pass
        except Exception as e:
            estadisticas_container.clear()
            with estadisticas_container:
                ui.label(f"❌ Error al cargar datos: {e}").classes("text-negative")
            return
        estadisticas_container.clear()
        with estadisticas_container:
            _pintar_home_inline(estadisticas_container, profile, orders_data, user_id=user["id"], items_data=items_data, on_refresh=cargar_y_pintar, shipments_today=shipments_today, questions=questions)

    cargar_y_pintar()
