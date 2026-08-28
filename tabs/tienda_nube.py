"""
tabs/tienda_nube.py — Vinculación: cruce de publicaciones de MercadoLibre contra
productos de Tienda Nube por seller_sku. Y Diferencias: corrección de precio/stock
en TN contra el objetivo, con MercadoLibre como única fuente de verdad -- nunca se
escribe de TN hacia ML. Todo lo demás (Vinculación, categorías) sigue solo lectura
hacia TN salvo por la escritura puntual ya hecha de categorías.
"""
from __future__ import annotations

import json
import re
from collections import defaultdict
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional

import requests as _requests

from nicegui import app, background_tasks, context, run, ui

from db import (
    get_tiendanube_credentials,
    get_tiendanube_productos,
    replace_tiendanube_productos,
    get_tiendanube_sync_status,
    set_tiendanube_sync_status,
    upsert_tiendanube_producto,
    get_tn_categoria_mapeada,
    set_tn_categoria_mapeo,
    get_app_config,
    get_cotizador_param,
    COTIZADOR_DEFAULTS,
    get_producto_tn_descuento,
    set_producto_tn_descuento,
    log_tn_escritura,
    get_ultima_escritura_stock,
    GROQ_MODEL,
    DEEPSEEK_MODEL,
    DEEPSEEK_BASE_URL,
)
from ml_api import get_ml_access_token, ml_get_my_items, ml_get_item, ml_get_item_description
from tiendanube_api import (
    tiendanube_list_products_with_variants,
    tiendanube_get,
    tiendanube_create_product,
    tiendanube_find_by_sku,
    tiendanube_update_variant,
)

_PLATAFORMA_LABELS = {
    "en_ambos": "En ambos",
    "solo_ml": "Solo en Mercado Libre",
    "solo_tn": "Solo en Tienda Nube",
}


def _fmt_precio_ars(val: Any) -> str:
    """Formato argentino de presentación: $ + punto como separador de miles.
    Sin decimales si son cero (el sort numérico usa el valor crudo, no esto)."""
    if val is None:
        return "—"
    try:
        n = float(str(val).replace(",", "."))
    except (TypeError, ValueError):
        return "—"
    entero = int(n)
    dec = round(abs(n - entero) * 100)
    parte_entera = f"{entero:,}".replace(",", ".")
    if dec == 0:
        return f"${parte_entera}"
    return f"${parte_entera},{dec:02d}"


def precio_contado_ml(ml_items_grupo: List[dict]) -> Optional[float]:
    """Precio de contado de un SKU: el mínimo entre las publicaciones ML del grupo
    (mismo criterio que usa el popup de creación en TN -- son tiers de cuotas del
    mismo producto, contado siempre es el más bajo)."""
    precios = [it.get("price") for it in ml_items_grupo if it.get("price") is not None]
    return min(precios) if precios else None


def _redondear_tn(precio: float, regla: str) -> float:
    if regla == "10":
        return round(precio / 10) * 10
    if regla == "100":
        return round(precio / 100) * 100
    if regla == "99":
        return round((precio + 1) / 100) * 100 - 1
    if regla == "999":
        return round((precio + 1) / 1000) * 1000 - 1
    if regla == "899":
        return round((precio + 101) / 1000) * 1000 - 101
    return precio  # "ninguno"


def calcular_precio_tn(sku: str, ml_items: List[dict], user_id: int) -> Optional[float]:
    """Precio objetivo de Tienda Nube para un SKU: precio de contado ML (mínimo del
    grupo, ver precio_contado_ml) con el descuento configurado -- override por
    producto si está cargado, si no el global -- y la regla de redondeo configurada."""
    sku_norm = (sku or "").strip().lower()
    grupo = [it for it in ml_items if (it.get("seller_sku") or "").strip().lower() == sku_norm]
    base = precio_contado_ml(grupo)
    if base is None:
        return None

    override = get_producto_tn_descuento(sku, user_id)
    if override is not None:
        descuento_pct = override
    else:
        # tn_descuento_pct (global, cotizador_datos) y productos.tn_descuento_pct
        # (override) guardan la MISMA unidad: porcentaje directo (10 = 10%), sin
        # conversión -- ver RANGE_KEYS en tabs/datos.py.
        raw = get_cotizador_param("tn_descuento_pct", user_id) or COTIZADOR_DEFAULTS["tn_descuento_pct"]
        descuento_pct = float(raw)

    regla = get_cotizador_param("tn_regla_redondeo", user_id) or COTIZADOR_DEFAULTS["tn_regla_redondeo"]
    return _redondear_tn(base * (1 - descuento_pct / 100), regla)


def escribir_tn_verificado(
    uid: int, tn_creds: Dict[str, Any], sku: str, tn_product_id: str, tn_variant_id: str,
    campo: str, valor_anterior: Any, valor_nuevo: Any, origen: str,
) -> tuple:
    """ÚNICA función de escritura hacia Tiendanube (precio o stock) -- todo lo demás
    en Diferencias pasa por acá, nunca arma su propio PUT.

    1. Guard de sucursales: si la variante tiene más de una location en
       inventory_levels, aborta sin escribir "stock" a ciegas (ver Fase 0 punto D).
    2. Escribe con tiendanube_update_variant (PUT a nivel variante -- confirmado en
       Fase 0 que ese endpoint SÍ devuelve cuerpo fresco, a diferencia del PUT de
       categorías a nivel producto).
    3. Verifica con un GET independiente -- nunca con el cuerpo del PUT. Es la
       misma regla que ya nos salvó con categorías: un 200 no es prueba de nada.
    4. Registra en tn_escrituras el resultado REAL (nunca el esperado): 'ok' solo
       si el GET confirma, 'error' en cualquier otro caso, siempre con detalle.

    Devuelve (ok: bool, mensaje: str)."""
    store_id, token, style = tn_creds["store_id"], tn_creds["access_token"], tn_creds["auth_header_style"]

    def _log(resultado: str, detalle: Optional[str]) -> None:
        log_tn_escritura(uid, sku, tn_product_id, tn_variant_id, campo, valor_anterior, valor_nuevo, origen, resultado, detalle)

    if campo not in ("precio", "stock"):
        msg = f"campo desconocido: {campo!r}"
        _log("error", msg)
        return False, msg

    # 1. Guard de sucursales -- SOLO aplica a stock (precio no depende de location)
    if campo == "stock":
        r_pre = tiendanube_get(store_id, token, style, f"products/{tn_product_id}")
        if not r_pre.ok:
            msg = f"No se pudo leer el producto antes de escribir: HTTP {r_pre.status_code}"
            _log("error", msg)
            return False, msg
        variante_pre = next(
            (v for v in (r_pre.json().get("variants") or []) if str(v.get("id")) == str(tn_variant_id)), None
        )
        if variante_pre is None:
            msg = "La variante no aparece en el producto -- no se escribe"
            _log("error", msg)
            return False, msg
        n_locations = len(variante_pre.get("inventory_levels") or [])
        if n_locations > 1:
            msg = f"Abortado: la variante tiene {n_locations} locations -- 'stock' simple no alcanza, hace falta inventory_levels"
            _log("error", msg)
            return False, msg

    # 2. Escritura -- SOLO el campo pedido
    if campo == "precio":
        try:
            payload = {"price": f"{Decimal(str(valor_nuevo)):.2f}"}
        except InvalidOperation:
            msg = f"precio nuevo inválido: {valor_nuevo!r}"
            _log("error", msg)
            return False, msg
    else:
        try:
            payload = {"stock": int(valor_nuevo)}
        except (TypeError, ValueError):
            msg = f"stock nuevo inválido: {valor_nuevo!r}"
            _log("error", msg)
            return False, msg

    data, error = tiendanube_update_variant(store_id, token, style, tn_product_id, tn_variant_id, payload)
    if error:
        _log("error", error)
        return False, error

    # 3. Verificación SIEMPRE con GET independiente, nunca con `data` (el body del PUT)
    r_verif = tiendanube_get(store_id, token, style, f"products/{tn_product_id}")
    if not r_verif.ok:
        msg = f"Escritura enviada pero no se pudo verificar: HTTP {r_verif.status_code}"
        _log("error", msg)
        return False, msg
    variante_verif = next(
        (v for v in (r_verif.json().get("variants") or []) if str(v.get("id")) == str(tn_variant_id)), None
    )
    if variante_verif is None:
        msg = "Escritura enviada pero la variante no apareció en la verificación"
        _log("error", msg)
        return False, msg

    if campo == "precio":
        try:
            ok = Decimal(str(variante_verif.get("price"))) == Decimal(str(valor_nuevo))
        except InvalidOperation:
            ok = False
        valor_confirmado = variante_verif.get("price")
    else:
        try:
            ok = int(variante_verif.get("stock")) == int(valor_nuevo)
        except (TypeError, ValueError):
            ok = False
        valor_confirmado = variante_verif.get("stock")

    if not ok:
        msg = f"GET independiente NO confirma: esperaba {valor_nuevo!r}, quedó {valor_confirmado!r}"
        _log("error", msg)
        return False, msg

    # Refleja el valor confirmado en el cache local (tiendanube_productos) para
    # que Diferencias no vuelva a mostrar el valor viejo hasta el próximo
    # "Actualizar" -- upsert puntual, no dispara un resync completo.
    d_verif = r_verif.json()
    nombre_field = d_verif.get("name")
    nombre_verif = nombre_field.get("es") if isinstance(nombre_field, dict) else (nombre_field or "")
    upsert_tiendanube_producto(
        uid, tn_variant_id, tn_product_id, variante_verif.get("sku"), nombre_verif,
        variante_verif.get("price"), variante_verif.get("stock"),
    )

    _log("ok", None)
    return True, "OK"


def _parse_precio_input(texto: Optional[str]) -> Optional[float]:
    """Entiende "214800", "214.800", "$214.800" y "214800.00" -- siempre da el
    mismo número entero de pesos (sin decimales nunca). El punto es ambiguo
    (separador de miles vs. decimal): se distingue por la cantidad de dígitos
    después del ÚLTIMO punto -- 3 dígitos es agrupación de miles (convención
    argentina), 2 dígitos es la parte decimal (se descarta, sin decimales)."""
    if texto is None:
        return None
    limpio = texto.strip().replace("$", "").replace(" ", "")
    if not limpio:
        return None
    partes = limpio.split(".")
    if len(partes) == 1:
        cuerpo = partes[0]
    else:
        ultima = partes[-1]
        if len(ultima) == 2 and ultima.isdigit():
            cuerpo = "".join(partes[:-1])  # parte decimal: se descarta (sin decimales)
        else:
            cuerpo = "".join(partes)  # separador de miles en todos los puntos
    try:
        return float(round(float(cuerpo)))
    except (ValueError, TypeError):
        return None


def _formatear_ultima_sync(iso_str: str) -> str:
    """last_sync_at se guarda en UTC (datetime.utcnow().isoformat(), ver
    db.set_tiendanube_sync_status). Relativo si fue hoy, fecha y hora completa si
    fue otro día -- así se distingue "hace 3 minutos" de "hace una semana"."""
    from datetime import datetime as _dt
    dt = _dt.fromisoformat(iso_str)
    ahora = _dt.utcnow()
    segundos = (ahora - dt).total_seconds()
    if segundos < 60:
        return "hace instantes"
    minutos = int(segundos // 60)
    if minutos < 60:
        return f"hace {minutos} minuto{'s' if minutos != 1 else ''}"
    if dt.date() == ahora.date():
        horas = int(minutos // 60)
        return f"hace {horas} hora{'s' if horas != 1 else ''}"
    return dt.strftime("%d/%m/%Y %H:%M")


def _atributo_valor(attrs: List[dict], *ids: str) -> Optional[str]:
    """Devuelve el value_name del primer atributo cuyo id (case-insensitive)
    coincide con alguno de los pedidos, en el orden dado -- por ejemplo, para MPN:
    ALPHANUMERIC_MODEL primero, MODEL como respaldo si no está."""
    por_id = {(a.get("id") or "").upper(): a.get("value_name") for a in (attrs or [])}
    for id_ in ids:
        val = por_id.get(id_.upper())
        if val:
            return str(val).strip()
    return None


def _atributos_a_texto(attrs: List[dict]) -> str:
    """Todos los atributos del ítem, tal cual los devuelve ML (name humano +
    value_name, o value_struct si no hay value_name -- ej. medidas numéricas con
    unidad). Sin filtrar ni interpretar: es la única fuente de datos técnicos
    reales que se le pasa a la IA para generar la descripción -- ver
    _generar_descripcion_html_prompt."""
    lineas = []
    for a in attrs or []:
        nombre = str(a.get("name") or a.get("id") or "").strip()
        valor = a.get("value_name")
        if not valor:
            struct = a.get("value_struct") or {}
            numero = struct.get("number")
            if numero is not None:
                unidad = struct.get("unit")
                valor = f"{numero} {unidad}".strip() if unidad else str(numero)
        valor = str(valor).strip() if valor else ""
        if nombre and valor:
            lineas.append(f"- {nombre}: {valor}")
    return "\n".join(lineas)


def _peso_gramos_desde_atributos(attrs: List[dict]) -> Optional[float]:
    """Busca el atributo WEIGHT de ML (name='Peso') y devuelve el valor en GRAMOS.
    ML lo da como texto con unidad libre (ej. '456 g', '8.7 g') -- nunca se asume
    la unidad, se la reconoce del propio texto. Devuelve None si no está cargado
    o si la unidad no se pudo reconocer (mejor no crear el campo que inventar)."""
    for a in attrs or []:
        if (a.get("id") or "").upper() != "WEIGHT":
            continue
        texto = str(a.get("value_name") or "").strip().lower()
        m = re.match(r"^([\d.,]+)\s*(kilogramos?|kg|gramos?|gr|g)\b", texto)
        if not m:
            return None
        valor = float(m.group(1).replace(",", "."))
        unidad = m.group(2)
        if unidad.startswith("kilo") or unidad == "kg":
            return valor * 1000.0
        return valor
    return None


def _kg_desde_gramos(gramos: float) -> str:
    """Convierte a la unidad que Tiendanube espera para el peso: KILOGRAMOS.

    ⚠️ INFERENCIA, NO CONFIRMADO TODAVÍA CONTRA EL RECURSO PRODUCT: la cita textual
    "Order's total weight, in kilograms" está en la documentación oficial del
    recurso ORDER (no del recurso Product/variant, que no aclara la unidad). Es
    razonable asumir que ambos recursos comparten la misma unidad en toda la API,
    pero queda como inferencia hasta que se cree el primer producto real y se
    verifique el peso mostrado en el panel de Tiendanube contra el peso real del
    paquete. Si algún día se detecta que Product usa otra unidad, corregir SOLO
    esta función -- todo el resto del flujo ya trabaja en gramos hasta este punto."""
    return f"{gramos / 1000.0:.3f}"


def _url_maxima_resolucion(url: str) -> str:
    """Reemplaza el sufijo de tamaño de una URL de imagen de ML por -F.

    Verificado EMPÍRICAMENTE 2026-08-27 (descargando las fotos reales y midiendo
    los píxeles con PIL, en 3 fotos de 3 ítems distintos): el sufijo -O (el que
    trae url/secure_url tal cual) da el tamaño CHICO -- coincide exacto con el
    campo "size" del JSON (ej. 500x500, 445x500, 364x500). El sufijo -F da la
    máxima resolución real -- coincide exacto, píxel a píxel, con el campo
    "max_size" del JSON en los 3 casos probados (ej. 1200x1200, 758x852, 855x1175).
    La suposición anterior ("-O ya es la máxima calidad") era incorrecta."""
    return re.sub(r"-[A-Za-z](\.\w+)$", r"-F\1", url)


def _texto_a_html(texto: str) -> str:
    """Envoltorio HTML mínimo para la descripción -- Tiendanube espera HTML por
    idioma (doc oficial: {"es": "<p>...</p>"}), la descripción de ML viene en texto
    plano. Solo se envuelven párrafos/saltos de línea, sin agregar más formato.

    Si el texto YA es HTML (generado con IA vía _generar_descripcion_html_una_vez,
    o pegado a mano) se deja tal cual -- envolverlo de nuevo anidaría <p> dentro
    de <p> y convertiría los saltos de línea internos del HTML en <br> sueltos."""
    texto = (texto or "").strip()
    if not texto:
        return ""
    if re.match(r"^<\w+[^>]*>", texto):
        return texto
    parrafos = [p.strip() for p in texto.split("\n\n") if p.strip()] or [texto]
    return "".join(f"<p>{p.replace(chr(10), '<br>')}</p>" for p in parrafos)


def _clean_json(raw: str) -> str:
    """Saca el envoltorio de backticks que a veces agregan los modelos (mismo
    patrón que ya usa tabs/guias.py para respuestas de Groq/Gemini)."""
    raw = (raw or "").strip()
    if raw.startswith("```"):
        lineas = raw.split("\n")
        raw = "\n".join(lineas[1:])
        if raw.endswith("```"):
            raw = raw[:-3]
    return raw.strip()


def _recortar_prolijo(texto: str, limite: int) -> str:
    """Recorta al límite de caracteres SIN cortar una palabra a la mitad. Si ya
    entra, lo devuelve tal cual."""
    texto = (texto or "").strip()
    if len(texto) <= limite:
        return texto
    recorte = texto[:limite]
    if " " in recorte:
        recorte = recorte.rsplit(" ", 1)[0]
    return recorte.rstrip(" ,.-")


def _generar_seo_completo_prompt(nombre: str, marca: Optional[str], categoria_nombre: str, descripcion: str) -> str:
    """Un solo prompt para título SEO + descripción SEO + tags -- antes eran dos
    llamadas separadas al proveedor de IA (una para título/descripción, otra para
    tags); ahora cada botón hace UNA sola llamada y trae los tres campos juntos.

    A PROPÓSITO no pide conteo exacto de caracteres: confirmado en vivo 2026-08-27
    (respuesta cruda con reasoning/reasoning_content lleno de "Amazon(6) espacio=7
    Echo(4)=11..." y content vacío, finish_reason="length") que gpt-oss-120b y
    deepseek-v4-flash se ponen a contar letra por letra en su razonamiento interno
    cuando se les exige un límite exacto, y se quedan sin tokens de salida antes de
    escribir la respuesta. El modelo ahora escribe con brevedad aproximada; el
    largo exacto lo valida y recorta _recortar_prolijo/_generar_seo_completo_una_vez
    -- esa es la división correcta del trabajo."""
    desc_recortada = (descripcion or "").strip()[:500]
    return (
        "Sos un experto en SEO para e-commerce argentino. Escribí en español rioplatense.\n"
        "Basándote en este producto, generá SOLAMENTE un JSON válido con tres campos:\n"
        '- "seo_title": un título corto, de alrededor de 60 caracteres, con marca y modelo, orientado a búsqueda.\n'
        '- "seo_description": una descripción breve, de dos oraciones cortas, orientada a búsqueda.\n'
        '- "tags": una lista de palabras clave de SEO, unas 10 (entre 8 y 12), como array de strings.\n'
        "No hace falta que cuentes caracteres -- sé breve y directo, nosotros ajustamos el largo después.\n\n"
        f"Nombre del producto: {nombre}\n"
        f"Marca: {marca or 'sin marca'}\n"
        f"Categoría: {categoria_nombre or 'sin categoría'}\n"
        f"Descripción: {desc_recortada}\n"
        'Respondé SOLO con el JSON, sin backticks ni texto adicional, con este formato exacto: '
        '{"seo_title": "...", "seo_description": "...", "tags": ["...", "..."]}'
    )


class _RespuestaSinEspacio(Exception):
    """El modelo agotó los tokens de salida antes de escribir el content -- confirmado
    en vivo 2026-08-27 con gpt-oss-120b (Groq) y deepseek-v4-flash (DeepSeek): gastan
    todo max_tokens en el campo interno de razonamiento y devuelven content vacío con
    finish_reason="length". Se distingue de una respuesta vacía sin motivo aparente
    para poder avisarlo en pantalla con precisión."""


class _LimiteDeVelocidad(Exception):
    """HTTP 429 -- confirmado en vivo 2026-08-27 (Groq, al probar 5 pedidos seguidos
    sin pausa). Un rate limit confundido con un fallo del modelo hace que el usuario
    cambie de proveedor cuando en realidad solo hacía falta esperar -- se distingue
    para avisarlo con precisión en pantalla."""


def _groq_generate(api_key: str, prompt: str, max_tokens: int = 300) -> str:
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": GROQ_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": max_tokens,
        "temperature": 0.5,
    }
    resp = _requests.post(url, headers=headers, json=payload, timeout=15)
    if resp.status_code == 429:
        raise _LimiteDeVelocidad()
    resp.raise_for_status()
    choice = resp.json()["choices"][0]
    content = choice["message"]["content"]
    if not content and choice.get("finish_reason") == "length":
        raise _RespuestaSinEspacio()
    return content


def _gemini_generate(api_key: str, prompt: str, max_tokens: int = 300) -> str:
    # max_tokens no se usa -- Gemini no mostró el modo de falla "sin espacio" (5/5 en
    # la verificación en vivo); se deja el parámetro solo para que las tres funciones
    # compartan la misma firma y _generar_seo_completo_una_vez pueda llamarlas igual.
    from google import genai
    from google.genai import errors as _genai_errors
    client = genai.Client(api_key=api_key)
    try:
        response = client.models.generate_content(model="gemini-2.5-flash", contents=prompt)
    except _genai_errors.APIError as e:
        if getattr(e, "code", None) == 429:
            raise _LimiteDeVelocidad() from e
        raise
    return response.text


def _deepseek_generate(api_key: str, prompt: str, max_tokens: int = 300) -> str:
    """Mismo helper que ya usa tabs/preguntas.py -- ninguna integración nueva.
    max_tokens con default 300 preserva el comportamiento de tabs/preguntas.py
    (que tiene su propia copia de esta función, no la importa de acá) -- esta
    firma es local a tienda_nube.py."""
    url = f"{DEEPSEEK_BASE_URL}/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": DEEPSEEK_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": max_tokens,
        "temperature": 0.7,
    }
    resp = _requests.post(url, headers=headers, json=payload, timeout=15)
    if resp.status_code == 429:
        raise _LimiteDeVelocidad()
    resp.raise_for_status()
    choice = resp.json()["choices"][0]
    content = choice["message"]["content"]
    if not content and choice.get("finish_reason") == "length":
        raise _RespuestaSinEspacio()
    return content


def _generar_seo_completo_una_vez(generador, api_key: str, nombre: str, marca: Optional[str], categoria_nombre: str, descripcion: str) -> tuple:
    """Cada botón (Gemini/Groq/DeepSeek) dispara UN proveedor puntual con UNA sola
    llamada que trae título SEO, descripción SEO y tags juntos -- el usuario elige
    qué proveedor probar. Se corre vía run.io_bound.

    Devuelve (resultado, motivo). motivo es None si salió bien. Si resultado es None,
    NUNCA se completa ningún campo con basura -- se avisa en pantalla, y motivo indica
    el porqué:
    - "sin_espacio": el modelo agotó los tokens de salida antes de escribir (ver
      _RespuestaSinEspacio) -- confirmado en vivo con Groq y DeepSeek, 2026-08-27.
    - "rate_limit": HTTP 429 (ver _LimiteDeVelocidad) -- confirmado en vivo con Groq.
    - "vacio": no hay datos usables por otro motivo (vacío, no parseable, incompleto).
    - "error": excepción de red/API.

    NO reintenta si algún campo se pasa de largo (70 / 160 / 8-12 tags) -- confirmado
    que con estos modelos un segundo intento insistiendo en el límite exacto empeora
    las cosas (más razonamiento sobre el conteo, mismo problema). Si se pasa, recorta
    prolijo directo (nunca a mitad de palabra, tags a 12) sin volver a llamar a la IA."""
    if not api_key:
        return None, "error"

    prompt = _generar_seo_completo_prompt(nombre, marca, categoria_nombre, descripcion)
    try:
        raw = generador(api_key, prompt, max_tokens=2000)
    except _RespuestaSinEspacio:
        return None, "sin_espacio"
    except _LimiteDeVelocidad:
        return None, "rate_limit"
    except Exception:
        return None, "error"

    if not raw or not raw.strip():
        return None, "vacio"
    try:
        data = json.loads(_clean_json(raw))
    except Exception:
        return None, "vacio"
    if not isinstance(data, dict):
        return None, "vacio"

    seo_title = str(data.get("seo_title") or "").strip()
    seo_description = str(data.get("seo_description") or "").strip()
    tags_raw = data.get("tags")
    if isinstance(tags_raw, list):
        tags = [str(t).strip() for t in tags_raw if str(t).strip()]
    elif isinstance(tags_raw, str):
        tags = [t.strip() for t in tags_raw.split(",") if t.strip()]
    else:
        tags = []
    if not (seo_title and seo_description and tags):
        return None, "vacio"

    return {
        "seo_title": _recortar_prolijo(seo_title, 70),
        "seo_description": _recortar_prolijo(seo_description, 160),
        "tags": tags[:12],
    }, None


def _generar_descripcion_html_prompt(
    nombre: str, marca: Optional[str], modelo: Optional[str], atributos_texto: str, descripcion_ml: str,
) -> str:
    """Prompt para la descripción completa del producto (no confundir con el SEO
    de _generar_seo_completo_prompt, que es un resumen de 2 oraciones para buscadores).
    Esta va directo al campo HTML de Tiendanube.

    La regla de no inventar specs es la parte crítica: los atributos de ML son la
    ÚNICA fuente de verdad técnica que tenemos (conectividad, batería, potencia,
    medidas). Una especificación inventada queda publicada como afirmación del
    vendedor -- riesgo real de reclamo, no un detalle de estilo."""
    desc_ml_recortada = (descripcion_ml or "").strip()[:1500]
    return (
        "Sos un redactor de e-commerce argentino. Escribí en español rioplatense.\n"
        "Generá la descripción de este producto para publicarla en la tienda online, en HTML simple, "
        "con dos bloques:\n"
        "1. Un párrafo <p> comercial breve, orientado a la venta.\n"
        "2. Una lista <ul> con <li> de especificaciones técnicas, sacadas de los atributos.\n\n"
        "⚠️ REGLA NO NEGOCIABLE: Usá EXCLUSIVAMENTE los datos técnicos provistos abajo. "
        "NO inventes especificaciones, medidas, duraciones de batería, potencias ni "
        "compatibilidades que no estén en los datos. Si un dato no está, no lo menciones. "
        "Preferí una descripción más corta antes que agregar un dato que no tenés.\n\n"
        f"Nombre del producto: {nombre}\n"
        f"Marca: {marca or 'sin marca'}\n"
        f"Modelo: {modelo or 'sin modelo'}\n"
        f"Atributos técnicos de Mercado Libre (única fuente de verdad técnica):\n"
        f"{atributos_texto or '(sin atributos cargados)'}\n\n"
        f"Descripción existente en Mercado Libre (material de base, puede faltar o estar incompleta):\n"
        f"{desc_ml_recortada or '(sin descripción previa)'}\n\n"
        "No hace falta que cuentes caracteres ni respetes un largo exacto -- sé breve "
        "(el párrafo comercial, de 2 a 4 oraciones; la lista, de tres a cinco viñetas) y priorizá "
        "que todo lo que digas sea verificable en los atributos de arriba.\n"
        "Respondé SOLO con el HTML (un <p> seguido de un <ul>), sin backticks, sin explicación, sin markdown."
    )


def _generar_descripcion_html_una_vez(
    generador, api_key: str, nombre: str, marca: Optional[str], modelo: Optional[str],
    atributos_texto: str, descripcion_ml: str,
) -> tuple:
    """Mismo patrón y mismo manejo de errores que _generar_seo_completo_una_vez
    (ver ese docstring para el porqué de no pedir conteo exacto de caracteres:
    gpt-oss-120b y deepseek-v4-flash se ponen a contar letra por letra en su
    razonamiento interno y se quedan sin tokens de salida).

    max_tokens=4000, el doble que el de SEO (2000): una descripción con párrafo +
    lista de especificaciones en HTML es bastante más larga que un título de 70
    caracteres + descripción de 160 + 10 tags.

    Devuelve (html, motivo) -- mismo contrato que _generar_seo_completo_una_vez:
    html es None si falló (motivo: "sin_espacio"/"rate_limit"/"vacio"/"error"),
    o el HTML crudo del modelo (sin backticks) si salió bien."""
    if not api_key:
        return None, "error"

    prompt = _generar_descripcion_html_prompt(nombre, marca, modelo, atributos_texto, descripcion_ml)
    try:
        raw = generador(api_key, prompt, max_tokens=4000)
    except _RespuestaSinEspacio:
        return None, "sin_espacio"
    except _LimiteDeVelocidad:
        return None, "rate_limit"
    except Exception:
        return None, "error"

    if not raw or not raw.strip():
        return None, "vacio"
    html = _clean_json(raw).strip()  # _clean_json solo saca el cerco de backticks, sirve para cualquier lenguaje
    if not html:
        return None, "vacio"
    return html, None


def _require_login() -> Optional[Dict[str, Any]]:
    user = app.storage.user.get("user")
    if not user:
        ui.notify("Debes iniciar sesión", color="negative")
    return user


def _cruzar(ml_items: List[dict], tn_rows: List[dict]) -> tuple:
    """Cruce por seller_sku (case-insensitive). Dos ejes independientes:

    - plataforma (en_ambos/solo_ml/solo_tn): en qué plataformas existe el SKU.
    - duplicado_ml / duplicado_tn: anomalía DENTRO de una plataforma -- más de un
      catalog_product_id (ML) o product_id (TN) DISTINTO comparte el mismo SKU.

    Ítems de ML sin seller_sku cargado no entran al cruce -- no hay clave con la cual
    cruzarlos -- y se cuentan aparte para que no desaparezcan en silencio.

    IMPORTANTE: "duplicado" NO se basa en cuántas publicaciones tiene un SKU. La
    estrategia de cuotas (contado/3/6/9/12) hace que la mayoría de los SKU tengan
    entre 5 y 10 publicaciones activas sin que eso sea una anomalía -- verificado
    contra datos reales de la cuenta: 123 de 140 SKUs reales tienen 10 publicaciones
    (5 tiers de cuotas × copia catálogo + copia sin catálogo, mismo catalog_product_id
    las 10), y otros 7 tienen 5 publicaciones sin catalog_product_id (mismo patrón,
    sin vínculo a catálogo). Ninguno de esos 130 casos es un duplicado real."""
    ml_by_sku: Dict[str, List[dict]] = defaultdict(list)
    tn_by_sku: Dict[str, List[dict]] = defaultdict(list)
    sin_sku_ml = 0
    for it in ml_items:
        sku = (it.get("seller_sku") or "").strip().lower()
        if sku:
            ml_by_sku[sku].append(it)
        else:
            sin_sku_ml += 1
    for r in tn_rows:
        sku = (r.get("sku") or "").strip().lower()
        if sku:
            tn_by_sku[sku].append(r)

    filas = []
    for sku in sorted(set(ml_by_sku) | set(tn_by_sku)):
        ml_m = ml_by_sku.get(sku, [])
        tn_m = tn_by_sku.get(sku, [])
        if ml_m and tn_m:
            plataforma = "en_ambos"
        elif ml_m:
            plataforma = "solo_ml"
        else:
            plataforma = "solo_tn"

        cpids_ml = {(it.get("catalog_product_id") or "").strip() for it in ml_m}
        cpids_ml.discard("")
        duplicado_ml = len(cpids_ml) > 1

        pids_tn = {str(r.get("product_id") or "").strip() for r in tn_m}
        pids_tn.discard("")
        duplicado_tn = len(pids_tn) > 1

        filas.append({
            "sku": sku,
            "plataforma": plataforma,
            "duplicado_ml": duplicado_ml,
            "duplicado_tn": duplicado_tn,
            "ml_publicaciones": len(ml_m),
            "tn_variantes": len(tn_m),
            # ML agrupa el stock por user_product: todas las publicaciones del SKU
            # comparten el mismo available_quantity, por eso alcanza con el primero.
            "ml_stock": ml_m[0].get("available_quantity") if ml_m else None,
            "ml_nombre": ml_m[0].get("title", "") if ml_m else "",
            "ml_precio": ml_m[0].get("price") if ml_m else None,
            "ml_status": ml_m[0].get("status", "") if ml_m else "",
            "tn_nombre": tn_m[0].get("nombre", "") if tn_m else "",
            "tn_precio": tn_m[0].get("precio") if tn_m else None,  # string crudo de TN, sin convertir
            "tn_stock": tn_m[0].get("stock") if tn_m else None,
        })
    return filas, sin_sku_ml


def build_tab_vinculacion(container) -> None:
    container.clear()
    user = _require_login()
    if not user:
        return
    uid = user["id"]

    access_token = get_ml_access_token(uid)
    tn_creds = get_tiendanube_credentials(uid)
    if not access_token:
        with container:
            ui.label("⚠️ No tenés MercadoLibre vinculado. Andá a Configuración.").classes("text-warning")
        return
    if not tn_creds or not tn_creds.get("store_id") or not tn_creds.get("access_token") or not tn_creds.get("auth_header_style"):
        with container:
            ui.label("⚠️ No tenés Tienda Nube vinculada (o falta 'Probar conexión' en Configuración).").classes("text-warning")
        return

    with container:
        ui.label("Tienda Nube — Vinculación").classes("text-xl font-bold")

        status_container = ui.column().classes("w-full")

        with ui.row().classes("w-full items-center gap-3 flex-wrap"):
            filtro_opciones = {"todos": "Todos", **_PLATAFORMA_LABELS, "duplicados": "Con SKU duplicado (ML o TN)"}
            filtro_sel = ui.select(filtro_opciones, value="solo_ml", label="Estado").props("dense outlined").classes("w-64")
            busqueda_input = ui.input(placeholder="Buscar por SKU o nombre...").props(
                "dense outlined clearable debounce=300"
            ).classes("w-64")
            incluir_pausadas_chk = ui.checkbox("Incluir pausadas (ML)", value=False)
            ui.space()
            actualizar_btn = ui.button("Actualizar").props("unelevated dense no-caps icon=refresh").classes("text-xs")
            ultima_sync_lbl = ui.label("").classes("text-xs text-gray-600")

        contadores_container = ui.row().classes("w-full gap-2 flex-wrap")
        header_div_vinc = ui.element("div").style("width:100%;overflow:hidden")
        table_container = ui.element("div").style("width:100%;height:calc(100vh - 454px);overflow-y:scroll;overflow-x:auto")
        _hid_v = header_div_vinc.id
        _cid_v = table_container.id
        _sync_vinc_client = context.client

        async def _setup_sync_vinc() -> None:
            with _sync_vinc_client:
                await ui.run_javascript(
                    f"(function(){{"
                    f"var body=document.getElementById('c{_cid_v}');"
                    f"var hdr=document.getElementById('c{_hid_v}');"
                    f"if(!body||!hdr)return;"
                    f"body.addEventListener('scroll',function(){{hdr.scrollLeft=body.scrollLeft;}});"
                    f"function _sg(){{hdr.style.paddingRight=(body.offsetWidth-body.clientWidth)+'px';}}"
                    f"_sg();new ResizeObserver(_sg).observe(body);"
                    f"}})();"
                )
        background_tasks.create(_setup_sync_vinc())

        columns = [
            {"name": "sku", "label": "SKU", "field": "sku", "align": "left"},
            {"name": "plataforma", "label": "Plataforma", "field": "plataforma", "align": "center"},
            {"name": "duplicado", "label": "Duplicado", "field": "duplicado", "align": "center"},
            {"name": "ml_stock", "label": "ML — Stock", "field": "ml_stock", "align": "right"},
            {"name": "ml_status", "label": "ML — Estado", "field": "ml_status", "align": "center"},
            {"name": "ml_nombre", "label": "ML — Nombre", "field": "ml_nombre", "align": "left"},
            {"name": "ml_precio", "label": "ML — Precio", "field": "ml_precio", "align": "right"},
            {"name": "tn_nombre", "label": "TN — Nombre", "field": "tn_nombre", "align": "center"},
            {"name": "tn_precio", "label": "TN — Precio", "field": "tn_precio", "align": "right"},
            {"name": "tn_stock", "label": "TN — Stock", "field": "tn_stock", "align": "right"},
            {"name": "acciones", "label": "Acciones", "field": "acciones", "align": "center", "sortable": False},
        ]
        _col_w_vinc = {
            "sku": "110px", "plataforma": "130px", "duplicado": "100px",
            "ml_stock": "70px", "ml_status": "110px", "ml_nombre": "260px",
            "ml_precio": "90px", "tn_nombre": "260px", "tn_precio": "90px", "tn_stock": "70px",
            "acciones": "170px",
        }

        def _build_colgroup_vinc() -> None:
            with ui.element("colgroup"):
                for col in columns:
                    ui.element("col").style(f"width:{_col_w_vinc.get(col['name'], '90px')}")

        sort_col_ref: Dict[str, Any] = {"val": "sku"}
        sort_asc_ref: Dict[str, bool] = {"val": True}

        def _sort_key_vinc(row: dict, col_name: str) -> Any:
            if col_name == "plataforma":
                return _PLATAFORMA_LABELS.get(row.get("plataforma"), "")
            if col_name == "duplicado":
                return 1 if (row.get("duplicado_ml") or row.get("duplicado_tn")) else 0
            if col_name in ("ml_stock", "ml_publicaciones", "tn_variantes"):
                return int(row.get(col_name) or 0)
            if col_name in ("ml_precio", "tn_precio", "tn_stock"):
                v = row.get(col_name)
                try:
                    return float(str(v).replace(",", ".")) if v is not None else -1.0
                except (ValueError, TypeError):
                    return -1.0
            return str(row.get(col_name) or "").lower()

        def _on_sort_click_vinc(col_name: str) -> None:
            if sort_col_ref.get("val") == col_name:
                sort_asc_ref["val"] = not sort_asc_ref.get("val", True)
            else:
                sort_col_ref["val"] = col_name
                sort_asc_ref["val"] = True
            _render_tabla()

        def _render_status() -> None:
            status_container.clear()
            st = get_tiendanube_sync_status(uid)
            if not st or not st.get("last_sync_at"):
                ultima_sync_lbl.set_text("Nunca se sincronizó — apretá Actualizar")
                ultima_sync_lbl.classes(replace="text-xs text-warning")
                return
            relativo = _formatear_ultima_sync(st["last_sync_at"])
            if st.get("ok"):
                ultima_sync_lbl.set_text(
                    f"Última sincronización: {relativo} ({st.get('items_leidos', 0)} variantes)"
                )
                ultima_sync_lbl.classes(replace="text-xs text-gray-600")
            else:
                ultima_sync_lbl.set_text(f"Última sincronización: {relativo} — FALLÓ")
                ultima_sync_lbl.classes(replace="text-xs text-negative")
                with status_container:
                    with ui.row().classes("w-full items-center gap-2 p-2 rounded").style("background:#fef2f2;border:1px solid #fecaca"):
                        ui.icon("error", color="negative", size="sm")
                        ui.label(
                            f"Sincronización incompleta/fallida: {st.get('error') or 'sin detalle'}"
                        ).classes("text-sm text-negative")

        def _render_tabla() -> None:
            ml_data = ml_get_my_items(access_token, include_paused=incluir_pausadas_chk.value)
            ml_items_actuales = ml_data.get("results", [])
            tn_rows = get_tiendanube_productos(uid)
            filas, sin_sku_ml = _cruzar(ml_items_actuales, tn_rows)

            filtro = filtro_sel.value
            if filtro == "todos":
                visibles = filas
            elif filtro == "duplicados":
                visibles = [f for f in filas if f["duplicado_ml"] or f["duplicado_tn"]]
            else:
                visibles = [f for f in filas if f["plataforma"] == filtro]

            busqueda = (busqueda_input.value or "").strip().lower()
            if busqueda:
                visibles = [
                    f for f in visibles
                    if busqueda in (f["sku"] or "").lower()
                    or busqueda in (f["ml_nombre"] or "").lower()
                    or busqueda in (f["tn_nombre"] or "").lower()
                ]

            visibles = sorted(
                visibles,
                key=lambda r: _sort_key_vinc(r, sort_col_ref.get("val", "sku")),
                reverse=not sort_asc_ref.get("val", True),
            )

            contadores_container.clear()
            with contadores_container:
                for key, label in _PLATAFORMA_LABELS.items():
                    n = sum(1 for f in filas if f["plataforma"] == key)
                    ui.badge(f"{label}: {n}", color="primary").props("outline")
                n_dup = sum(1 for f in filas if f["duplicado_ml"] or f["duplicado_tn"])
                ui.badge(f"Con SKU duplicado: {n_dup}", color="negative" if n_dup else "positive").props("outline")
                if sin_sku_ml:
                    ui.badge(f"ML sin SKU cargado (excluidos del cruce): {sin_sku_ml}", color="warning").props("outline")
                ui.badge(f"Productos en Tienda Nube (variantes): {len(tn_rows)}", color="secondary").props("outline")

            rows = []
            for f in visibles:
                dup_partes = []
                if f["duplicado_ml"]:
                    dup_partes.append("ML")
                if f["duplicado_tn"]:
                    dup_partes.append("TN")
                rows.append({
                    **f,
                    "plataforma": _PLATAFORMA_LABELS[f["plataforma"]],
                    "duplicado": ("⚠ " + "+".join(dup_partes)) if dup_partes else "—",
                    # el sort ya corrió sobre el valor crudo (visibles, arriba) -- esto
                    # solo formatea la presentación, no altera el dato ni el orden
                    "tn_precio": _fmt_precio_ars(f["tn_precio"]),
                    "tn_stock": f["tn_stock"] if f["tn_stock"] is not None else "—",
                    "ml_precio": _fmt_precio_ars(f["ml_precio"]),
                    "ml_stock": f["ml_stock"] if f["ml_stock"] is not None else "—",
                    "puede_crear_tn": f["plataforma"] == "solo_ml",
                })

            def _abrir_popup_crear(sku: str, ml_items: List[dict]) -> None:
                """Popup de creación asistida, UN producto por vez -- no hay botón
                de sincronización masiva. Reconstruye el grupo del SKU desde los
                ítems de ML ya cargados (misma agrupación que _cruzar, sin tocarla)."""
                ml_m = [it for it in ml_items if (it.get("seller_sku") or "").strip().lower() == sku]
                if not ml_m:
                    ui.notify("No se encontraron publicaciones de ML para este SKU.", color="negative")
                    return
                propia = next(
                    (x for x in ml_m if not x.get("catalog_listing")
                     and str(x.get("listing_type_id") or "").lower() == "gold_special"),
                    None,
                )
                catalogo = next((x for x in ml_m if x.get("catalog_listing")), None)
                fuente = propia or catalogo or ml_m[0]
                precio_min = precio_contado_ml(ml_m)
                stock_pool = ml_m[0].get("available_quantity")
                category_id = ml_m[0].get("category_id")

                with ui.dialog().props("persistent") as dlg, ui.card().classes("w-[720px] max-w-full"):
                    cuerpo = ui.column().classes("w-full gap-2")
                    with cuerpo:
                        ui.label("Crear en Tienda Nube").classes("text-lg font-bold")
                        ui.label(f"SKU: {sku}").classes("text-sm text-gray-600 font-mono")
                        with ui.row().classes("w-full items-center gap-2 py-4"):
                            ui.spinner(size="sm")
                            ui.label("Cargando datos de MercadoLibre y Tienda Nube...")
                dlg.open()

                async def _cargar() -> None:
                    full = await run.io_bound(ml_get_item, access_token, fuente.get("id"))
                    # La descripción NO siempre vive en "fuente" (esa se elige para
                    # atributos/marca/fotos). Diagnóstico 2026-08-28: las publicaciones
                    # de catálogo tienen descripción heredada casi siempre (40/40 en
                    # muestra); las propias duplicadas por tier de cuotas casi nunca
                    # (el recurso ni existe -- 404). Se recorre catálogo primero, después
                    # propias, y se corta en la primera con texto real no vacío.
                    candidatos = [x for x in ml_m if x.get("catalog_listing")] + \
                        [x for x in ml_m if not x.get("catalog_listing")]
                    descripcion = ""
                    for cand in candidatos:
                        txt = await run.io_bound(ml_get_item_description, access_token, cand.get("id"))
                        if txt:
                            descripcion = txt
                            break
                    cats_resp = await run.io_bound(
                        tiendanube_get, tn_creds["store_id"], tn_creds["access_token"],
                        tn_creds["auth_header_style"], "categories",
                    )
                    categorias: List[tuple] = []
                    if cats_resp.ok:
                        for c in cats_resp.json():
                            nombre_c = c.get("name")
                            if isinstance(nombre_c, dict):
                                nombre_c = nombre_c.get("es") or next(iter(nombre_c.values()), str(c.get("id")))
                            categorias.append((str(c.get("id")), nombre_c))
                    cat_options = dict(categorias)

                    attrs = (full or {}).get("attributes") or []
                    atributos_texto = _atributos_a_texto(attrs)
                    peso_g = _peso_gramos_desde_atributos(attrs)
                    gtin_precarga = _atributo_valor(attrs, "GTIN")
                    # Preferencia: MPN nativo de ML primero (es literalmente el mismo dato);
                    # si no está, ALPHANUMERIC_MODEL (código real del fabricante, ej.
                    # "A710BL"); si tampoco, MODEL (a veces es el nombre comercial, ej.
                    # "Watch SE (GPS) 3th Gen" -- eso NO es un MPN, por eso queda editable).
                    mpn_precarga = _atributo_valor(attrs, "MPN", "ALPHANUMERIC_MODEL", "MODEL")
                    marca_precarga = fuente.get("marca")
                    if marca_precarga == "—":
                        marca_precarga = None
                    pictures = (full or {}).get("pictures") or []
                    categoria_sugerida = get_tn_categoria_mapeada(uid, str(category_id)) if category_id else None
                    if categoria_sugerida not in cat_options:
                        categoria_sugerida = None

                    cuerpo.clear()
                    with cuerpo:
                        ui.label("Crear en Tienda Nube").classes("text-lg font-bold")
                        ui.label(f"SKU: {sku}").classes("text-sm text-gray-600 font-mono")

                        if not cats_resp.ok:
                            ui.label(
                                f"⚠️ No se pudieron leer las categorías de Tienda Nube (HTTP {cats_resp.status_code})."
                            ).classes("text-sm text-negative")

                        error_area = ui.column().classes("w-full")

                        ui.label("Nombre").classes("text-sm font-semibold mt-2")
                        nombre_input = ui.input(value="").props("outlined dense").classes("w-full")
                        with ui.row().classes("w-full gap-2"):
                            with ui.column().classes("flex-1 border rounded p-2"):
                                ui.label("Propia").classes("text-xs text-gray-500")
                                ui.label(propia.get("title") if propia else "— no hay publicación propia —").classes("text-sm")
                                if propia:
                                    ui.button(
                                        "Usar este", on_click=lambda: nombre_input.set_value(propia.get("title", ""))
                                    ).props("unelevated no-caps").classes("w-full mt-1")
                            with ui.column().classes("flex-1 border rounded p-2"):
                                ui.label("Catálogo").classes("text-xs text-gray-500")
                                ui.label(catalogo.get("title") if catalogo else "— no hay publicación de catálogo —").classes("text-sm")
                                if catalogo:
                                    ui.button(
                                        "Usar este", on_click=lambda: nombre_input.set_value(catalogo.get("title", ""))
                                    ).props("unelevated no-caps").classes("w-full mt-1")

                        with ui.row().classes("w-full items-center justify-between mt-2"):
                            ui.label("Descripción").classes("text-sm font-semibold")
                            with ui.row().classes("gap-2"):
                                desc_gemini_btn = ui.button("Gemini", icon="auto_awesome").props("outline dense no-caps size=sm")
                                desc_groq_btn = ui.button("Groq", icon="auto_awesome").props("outline dense no-caps size=sm")
                                desc_deepseek_btn = ui.button("DeepSeek", icon="auto_awesome").props("outline dense no-caps size=sm")
                        ui.label(
                            "Genera párrafo comercial + lista de especificaciones en HTML, usando SOLO "
                            "los atributos técnicos de ML -- no inventa datos. Reemplaza el texto actual, "
                            "revisalo antes de crear."
                        ).classes("text-xs text-gray-500")
                        if not descripcion:
                            ui.label(
                                "⚠️ Ninguna publicación de este SKU tiene descripción cargada en "
                                "Mercado Libre. Completala manualmente o generá una con IA."
                            ).classes("text-sm text-warning")
                        descripcion_input = ui.textarea(value=descripcion).props("outlined dense").classes("w-full").style("min-height:120px")

                        ui.label("Identificación").classes("text-sm font-semibold mt-2")
                        with ui.row().classes("w-full gap-3 items-start"):
                            with ui.column().classes("flex-1"):
                                ui.label("Marca").classes("text-xs text-gray-500")
                                marca_input = ui.input(value=marca_precarga or "").props("outlined dense").classes("w-full")
                            with ui.column().classes("flex-1"):
                                ui.label("Código de barras (GTIN)").classes("text-xs text-gray-500")
                                barcode_input = ui.input(value=gtin_precarga or "").props("outlined dense").classes("w-full")
                            with ui.column().classes("flex-1"):
                                ui.label("MPN (modelo del fabricante)").classes("text-xs text-gray-500")
                                mpn_input = ui.input(value=mpn_precarga or "").props("outlined dense").classes("w-full")
                        ui.label(
                            "MPN: si lo precargado es un nombre comercial y no un código de parte, "
                            "es más seguro dejarlo vacío que mandarlo -- un identificador inconsistente "
                            "puede hacer que Google rechace el producto."
                        ).classes("text-xs text-gray-500")

                        def _descripcion_ia_generador(nombre_config: str, api_key_config: str, generador, boton) -> Any:
                            async def _run() -> None:
                                api_key = get_app_config(api_key_config)
                                if not api_key:
                                    ui.notify(
                                        f"Configurá tu API key de {nombre_config} en Configuración.", color="negative",
                                    )
                                    return
                                boton.props("loading")
                                try:
                                    nombre_base = (nombre_input.value or "").strip() or fuente.get("title", "")
                                    marca_val = (marca_input.value or "").strip() or None
                                    modelo_val = (mpn_input.value or "").strip() or None
                                    resultado, motivo = await run.io_bound(
                                        _generar_descripcion_html_una_vez, generador, api_key, nombre_base,
                                        marca_val, modelo_val, atributos_texto, descripcion,
                                    )
                                    if resultado is None:
                                        if motivo == "sin_espacio":
                                            ui.notify(
                                                "El modelo se quedó sin espacio para responder -- probá de nuevo "
                                                "o con otro proveedor.", color="negative",
                                            )
                                        elif motivo == "rate_limit":
                                            ui.notify(
                                                "Demasiados pedidos -- esperá unos segundos y probá de nuevo.",
                                                color="negative",
                                            )
                                        elif motivo == "vacio":
                                            ui.notify(
                                                "La IA no devolvió un resultado usable -- probá de nuevo o con otro "
                                                "proveedor.", color="negative",
                                            )
                                        else:
                                            ui.notify(f"Error al generar con {nombre_config}.", color="negative")
                                        return
                                    descripcion_input.set_value(resultado)
                                finally:
                                    boton.props(remove="loading")
                            return _run
                        desc_gemini_btn.on_click(_descripcion_ia_generador("Gemini", "gemini_api_key", _gemini_generate, desc_gemini_btn))
                        desc_groq_btn.on_click(_descripcion_ia_generador("Groq", "groq_api_key", _groq_generate, desc_groq_btn))
                        desc_deepseek_btn.on_click(_descripcion_ia_generador("DeepSeek", "deepseek_api_key", _deepseek_generate, desc_deepseek_btn))

                        with ui.row().classes("w-full items-center justify-between mt-2"):
                            ui.label("SEO").classes("text-sm font-semibold")
                            with ui.row().classes("gap-2"):
                                seo_gemini_btn = ui.button("Gemini", icon="auto_awesome").props("outline dense no-caps size=sm")
                                seo_groq_btn = ui.button("Groq", icon="auto_awesome").props("outline dense no-caps size=sm")
                                seo_deepseek_btn = ui.button("DeepSeek", icon="auto_awesome").props("outline dense no-caps size=sm")
                        ui.label(
                            "Un solo botón completa título, descripción y tags juntos."
                        ).classes("text-xs text-gray-500")

                        ui.label("Título SEO").classes("text-xs text-gray-500")
                        seo_title_input = ui.input(value="").props("outlined dense").classes("w-full")
                        seo_title_counter = ui.label("0 / 70").classes("text-xs text-gray-500")

                        def _actualizar_seo_title_counter() -> None:
                            n = len(seo_title_input.value or "")
                            seo_title_counter.set_text(f"{n} / 70")
                            seo_title_counter.classes(replace="text-xs " + ("text-negative" if n > 70 else "text-gray-500"))
                        seo_title_input.on_value_change(lambda *_: _actualizar_seo_title_counter())
                        _actualizar_seo_title_counter()

                        ui.label("Descripción SEO").classes("text-xs text-gray-500 mt-1")
                        seo_description_input = ui.textarea(value="").props("outlined dense").classes("w-full")
                        seo_description_counter = ui.label("0 / 160").classes("text-xs text-gray-500")

                        def _actualizar_seo_desc_counter() -> None:
                            n = len(seo_description_input.value or "")
                            seo_description_counter.set_text(f"{n} / 160")
                            seo_description_counter.classes(replace="text-xs " + ("text-negative" if n > 160 else "text-gray-500"))
                        seo_description_input.on_value_change(lambda *_: _actualizar_seo_desc_counter())
                        _actualizar_seo_desc_counter()

                        ui.label("Tags / palabras clave SEO").classes("text-xs text-gray-500 mt-1")
                        tags_input = ui.input(value="").props("outlined dense").classes("w-full")

                        def _seo_completo_generador(nombre_config: str, api_key_config: str, generador, boton) -> Any:
                            async def _run() -> None:
                                api_key = get_app_config(api_key_config)
                                if not api_key:
                                    ui.notify(
                                        f"Configurá tu API key de {nombre_config} en Configuración.", color="negative",
                                    )
                                    return
                                boton.props("loading")
                                try:
                                    nombre_base = (nombre_input.value or "").strip() or fuente.get("title", "")
                                    marca_val = (marca_input.value or "").strip() or None
                                    resultado, motivo = await run.io_bound(
                                        _generar_seo_completo_una_vez, generador, api_key, nombre_base, marca_val,
                                        cat_options.get(categoria_sel.value, ""), descripcion_input.value or "",
                                    )
                                    if resultado is None:
                                        if motivo == "sin_espacio":
                                            ui.notify(
                                                "El modelo se quedó sin espacio para responder -- probá de nuevo "
                                                "o con otro proveedor.", color="negative",
                                            )
                                        elif motivo == "rate_limit":
                                            ui.notify(
                                                "Demasiados pedidos -- esperá unos segundos y probá de nuevo.",
                                                color="negative",
                                            )
                                        elif motivo == "vacio":
                                            ui.notify(
                                                "La IA no devolvió un resultado usable -- probá de nuevo o con otro "
                                                "proveedor.", color="negative",
                                            )
                                        else:
                                            ui.notify(f"Error al generar con {nombre_config}.", color="negative")
                                        return
                                    seo_title_input.set_value(resultado["seo_title"])
                                    seo_description_input.set_value(resultado["seo_description"])
                                    tags_input.set_value(", ".join(resultado["tags"]))
                                    _actualizar_seo_title_counter()
                                    _actualizar_seo_desc_counter()
                                finally:
                                    boton.props(remove="loading")
                            return _run
                        seo_gemini_btn.on_click(_seo_completo_generador("Gemini", "gemini_api_key", _gemini_generate, seo_gemini_btn))
                        seo_groq_btn.on_click(_seo_completo_generador("Groq", "groq_api_key", _groq_generate, seo_groq_btn))
                        seo_deepseek_btn.on_click(_seo_completo_generador("DeepSeek", "deepseek_api_key", _deepseek_generate, seo_deepseek_btn))

                        ui.label("Categoría (Tiendanube)").classes("text-sm font-semibold mt-2")
                        categoria_sel = ui.select(cat_options, value=categoria_sugerida).props("outlined dense").classes("w-full")
                        if categoria_sugerida:
                            ui.label("Preseleccionada por el mapeo aprendido de esta categoría de ML.").classes("text-xs text-gray-500")

                        with ui.row().classes("w-full gap-2"):
                            with ui.column().classes("flex-1"):
                                ui.label("Precio (mínimo del grupo -- contado)").classes("text-sm font-semibold")
                                precio_val_inicial = _parse_precio_input(str(precio_min)) if precio_min is not None else None
                                precio_input = ui.input(
                                    value=_fmt_precio_ars(precio_val_inicial) if precio_val_inicial is not None else ""
                                ).props("outlined dense").classes("w-full")

                                def _precio_a_edicion() -> None:
                                    v = _parse_precio_input(precio_input.value)
                                    precio_input.set_value(str(int(v)) if v is not None else "")

                                def _precio_a_formato() -> None:
                                    v = _parse_precio_input(precio_input.value)
                                    precio_input.set_value(_fmt_precio_ars(v) if v is not None else "")
                                precio_input.on("focus", lambda: _precio_a_edicion())
                                precio_input.on("blur", lambda: _precio_a_formato())
                            with ui.column().classes("flex-1"):
                                ui.label("Stock (pool de ML)").classes("text-sm font-semibold")
                                stock_input = ui.number(value=stock_pool, format="%.0f").props("outlined dense").classes("w-full")

                        with ui.row().classes("w-full gap-3 items-start"):
                            with ui.column().classes("flex-1"):
                                ui.label("Peso (gramos) -- obligatorio").classes("text-sm font-semibold")
                                peso_input = ui.number(
                                    value=round(peso_g) if peso_g is not None else None, format="%.0f"
                                ).props("outlined dense").classes("w-full")
                                peso_kg_lbl = ui.label("").classes("text-xs text-gray-500")
                                if peso_g is None:
                                    ui.label(
                                        "⚠️ ML no tiene el peso cargado para esta publicación. "
                                        "Sin peso no se puede crear -- cargalo a mano."
                                    ).classes("text-xs text-negative")

                                def _actualizar_peso_kg() -> None:
                                    v = peso_input.value
                                    peso_kg_lbl.set_text(
                                        f"= {float(v) / 1000.0:.3f} kg (Tiendanube espera kilogramos -- "
                                        f"verificá el peso en el panel de TN al ver el primer producto creado)"
                                        if v is not None else ""
                                    )
                                peso_input.on_value_change(lambda *_: _actualizar_peso_kg())
                                _actualizar_peso_kg()

                        with ui.row().classes("w-full gap-3 items-start"):
                            with ui.column().classes("flex-1"):
                                ui.label("Ancho (cm)").classes("text-sm font-semibold")
                                ancho_input = ui.number(value=None, format="%.1f").props("outlined dense").classes("w-full")
                            with ui.column().classes("flex-1"):
                                ui.label("Alto (cm)").classes("text-sm font-semibold")
                                alto_input = ui.number(value=None, format="%.1f").props("outlined dense").classes("w-full")
                            with ui.column().classes("flex-1"):
                                ui.label("Profundidad (cm)").classes("text-sm font-semibold")
                                profundidad_input = ui.number(value=None, format="%.1f").props("outlined dense").classes("w-full")
                        ui.label(
                            "Opcional -- ML no trae estos datos hoy para ningún SKU (0/140 verificado). "
                            "Unidad centímetros: confirmado en la ayuda oficial de Tiendanube "
                            "(la documentación de la API no la aclara)."
                        ).classes("text-xs text-gray-500")

                        ui.label(
                            "Imágenes (se envían en máxima resolución -- sufijo -F). "
                            "La primera es la portada del producto en la tienda -- usá las flechas para reordenar."
                        ).classes("text-sm font-semibold mt-2")
                        imagenes_state: List[Dict[str, Any]] = [
                            {"url": _url_maxima_resolucion(p.get("secure_url") or p.get("url") or ""), "incluir": True}
                            for p in pictures if (p.get("secure_url") or p.get("url"))
                        ]
                        imagenes_container = ui.column().classes("w-full")

                        def _mover_imagen(idx: int, delta: int) -> None:
                            nuevo = idx + delta
                            if 0 <= nuevo < len(imagenes_state):
                                imagenes_state[idx], imagenes_state[nuevo] = imagenes_state[nuevo], imagenes_state[idx]
                                _render_imagenes()

                        def _render_imagenes() -> None:
                            imagenes_container.clear()
                            with imagenes_container:
                                if not imagenes_state:
                                    ui.label("Esta publicación no tiene fotos.").classes("text-xs text-gray-400")
                                    return
                                with ui.row().classes("w-full flex-wrap gap-2"):
                                    for idx, item in enumerate(imagenes_state):
                                        with ui.column().classes("items-center gap-1 border rounded p-1"):
                                            ui.image(item["url"]).classes("rounded").style("width:80px;height:80px;object-fit:cover")
                                            ui.label(f"#{idx + 1}" + (" — portada" if idx == 0 else "")).classes("text-xs text-gray-500")
                                            with ui.row().classes("gap-0 items-center"):
                                                up_btn = ui.button(icon="arrow_upward").props("flat dense size=sm")
                                                up_btn.on_click(lambda i=idx: _mover_imagen(i, -1))
                                                if idx == 0:
                                                    up_btn.set_enabled(False)
                                                down_btn = ui.button(icon="arrow_downward").props("flat dense size=sm")
                                                down_btn.on_click(lambda i=idx: _mover_imagen(i, 1))
                                                if idx == len(imagenes_state) - 1:
                                                    down_btn.set_enabled(False)
                                            chk = ui.checkbox("Incluir", value=item["incluir"])
                                            chk.on_value_change(
                                                lambda e, i=idx: imagenes_state[i].update(incluir=getattr(e, "value", True))
                                            )
                        _render_imagenes()

                        ui.label(f"SKU: {sku}").classes("text-xs text-gray-500 font-mono mt-2")

                        with ui.row().classes("w-full justify-end gap-2 mt-3"):
                            ui.button("Cancelar", on_click=lambda: dlg.close()).props("flat no-caps")
                            crear_btn = ui.button("Crear en Tienda Nube").props("unelevated no-caps")

                    async def _submit() -> None:
                        error_area.clear()
                        nombre_val = (nombre_input.value or "").strip()
                        categoria_val = categoria_sel.value
                        precio_val = _parse_precio_input(precio_input.value)
                        stock_val = stock_input.value
                        peso_val = peso_input.value

                        faltantes = []
                        if not nombre_val:
                            faltantes.append("Nombre")
                        if not categoria_val:
                            faltantes.append("Categoría")
                        if precio_val is None or float(precio_val) <= 0:
                            faltantes.append("Precio")
                        if peso_val is None or float(peso_val) <= 0:
                            faltantes.append("Peso (obligatorio -- Tiendanube lo necesita para cotizar envíos)")
                        if len(seo_title_input.value or "") > 70:
                            faltantes.append("Título SEO se pasa de 70 caracteres")
                        if len(seo_description_input.value or "") > 160:
                            faltantes.append("Descripción SEO se pasa de 160 caracteres")
                        if faltantes:
                            with error_area:
                                ui.label("Faltan campos: " + ", ".join(faltantes)).classes("text-sm text-negative")
                            return

                        crear_btn.props("loading")
                        try:
                            existente = await run.io_bound(
                                tiendanube_find_by_sku, tn_creds["store_id"], tn_creds["access_token"],
                                tn_creds["auth_header_style"], sku,
                            )
                            if existente:
                                with error_area:
                                    ui.label(
                                        f"Ya existe en Tienda Nube (product_id={existente['product_id']}, "
                                        f"variant_id={existente['variant_id']}) -- no se creó ningún duplicado."
                                    ).classes("text-sm text-negative")

                                    def _vincular_existente(ex=existente) -> None:
                                        upsert_tiendanube_producto(
                                            uid, ex["variant_id"], ex["product_id"], ex["sku"], nombre_val, None, None,
                                        )
                                        ui.notify("Vinculado al producto existente.", color="positive")
                                        dlg.close()
                                        _render_tabla()

                                    ui.button("Vincular al existente", on_click=_vincular_existente).props("unelevated no-caps dense")
                                return

                            # position explícito -- Tiendanube documenta "position" como el campo
                            # que determina el orden de las imágenes, no el orden del array por sí solo.
                            imagenes_payload = [
                                {"src": item["url"], "position": i + 1}
                                for i, item in enumerate(imagenes_state) if item["incluir"]
                            ]
                            payload = {
                                "name": {"es": nombre_val},
                                "description": {"es": _texto_a_html(descripcion_input.value or "")},
                                "categories": [int(categoria_val)],
                                "published": True,
                                "images": imagenes_payload,
                                "variants": [{
                                    "price": f"{float(precio_val):.2f}",
                                    "stock": int(stock_val) if stock_val is not None else 0,
                                    "stock_management": True,
                                    "weight": _kg_desde_gramos(float(peso_val)),
                                    "sku": sku,
                                    "width": f"{float(ancho_input.value):.2f}" if ancho_input.value is not None else None,
                                    "height": f"{float(alto_input.value):.2f}" if alto_input.value is not None else None,
                                    "depth": f"{float(profundidad_input.value):.2f}" if profundidad_input.value is not None else None,
                                    "barcode": (barcode_input.value or "").strip() or None,
                                    "mpn": (mpn_input.value or "").strip() or None,
                                }],
                            }
                            if (marca_input.value or "").strip():
                                payload["brand"] = marca_input.value.strip()
                            if (tags_input.value or "").strip():
                                payload["tags"] = tags_input.value.strip()
                            if (seo_title_input.value or "").strip():
                                payload["seo_title"] = seo_title_input.value.strip()
                            if (seo_description_input.value or "").strip():
                                payload["seo_description"] = seo_description_input.value.strip()
                            creado, error = await run.io_bound(
                                tiendanube_create_product, tn_creds["store_id"], tn_creds["access_token"],
                                tn_creds["auth_header_style"], payload,
                            )
                            if error:
                                with error_area:
                                    ui.label(f"Error al crear el producto: {error}").classes("text-sm text-negative")
                                return

                            variantes_creadas = (creado or {}).get("variants") or []
                            if not variantes_creadas:
                                with error_area:
                                    ui.label(
                                        "Tiendanube respondió OK pero sin variantes en el cuerpo -- no se guardó "
                                        f"ningún vínculo porque no hay variant_id. Revisá el producto "
                                        f"{(creado or {}).get('id')} manualmente en el panel."
                                    ).classes("text-sm text-negative")
                                return
                            variante = variantes_creadas[0]

                            upsert_tiendanube_producto(
                                uid, str(variante.get("id")), str(creado.get("id")),
                                variante.get("sku") or sku, nombre_val,
                                variante.get("price"), variante.get("stock"),
                            )
                            if category_id:
                                set_tn_categoria_mapeo(uid, str(category_id), str(categoria_val))

                            ui.notify(f"Producto creado en Tiendanube (id={creado.get('id')}).", color="positive")
                            dlg.close()
                            _render_tabla()
                        finally:
                            crear_btn.props(remove="loading")

                    crear_btn.on_click(_submit)

                background_tasks.create(_cargar())

            header_div_vinc.clear()
            table_container.clear()
            if not rows:
                with table_container:
                    ui.label("Sin resultados para este filtro.").classes("text-sm text-gray-400")
                return

            with header_div_vinc:
                with ui.element("table").style("table-layout:fixed;width:100%;border-collapse:separate;border-spacing:0"):
                    _build_colgroup_vinc()
                    with ui.element("thead"):
                        with ui.element("tr").classes("bg-primary text-white font-semibold"):
                            for col in columns:
                                with ui.element("th").classes("px-2 py-1 border text-center").style("line-height:1.1"):
                                    if col.get("sortable", True):
                                        ui.button(
                                            col["label"], on_click=lambda c=col["name"]: _on_sort_click_vinc(c)
                                        ).props("flat dense no-caps").classes(
                                            "text-white hover:bg-white/20 cursor-pointer font-semibold"
                                        ).style(
                                            "white-space:nowrap;overflow:hidden;text-overflow:ellipsis;"
                                            "max-width:100%;min-height:0;padding:2px 6px;line-height:1.1"
                                        )
                                    else:
                                        ui.label(col["label"]).classes("font-semibold").style("line-height:1.1")

            with table_container:
                with ui.element("table").style("table-layout:fixed;width:100%;border-collapse:separate;border-spacing:0"):
                    _build_colgroup_vinc()
                    with ui.element("tbody"):
                        for row in rows:
                            with ui.element("tr").classes("border-t border-gray-200 hover:bg-gray-50"):
                                for col in columns:
                                    align = "text-right" if col["align"] == "right" else "text-center" if col["align"] == "center" else "text-left"
                                    with ui.element("td").classes(f"px-2 py-1 border-b border-gray-100 {align} text-xs").style("white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:0"):
                                        if col["name"] == "acciones":
                                            if row.get("puede_crear_tn"):
                                                ui.button(
                                                    "Crear en Tienda Nube",
                                                    on_click=lambda sku=row["sku"]: _abrir_popup_crear(sku, ml_items_actuales),
                                                ).props("unelevated dense no-caps size=sm").classes("text-xs")
                                            else:
                                                ui.label("—")
                                        else:
                                            val = row.get(col["field"])
                                            ui.label(str(val) if val is not None else "—")

                _recalc_padding_vinc_client = context.client

                async def _recalc_padding_vinc() -> None:
                    with _recalc_padding_vinc_client:
                        await ui.run_javascript(
                            f"(function(){{"
                            f"var body=document.getElementById('c{_cid_v}');"
                            f"var hdr=document.getElementById('c{_hid_v}');"
                            f"if(body&&hdr){{hdr.style.paddingRight=(body.offsetWidth-body.clientWidth)+'px';}}"
                            f"}})();"
                        )
                background_tasks.create(_recalc_padding_vinc())

        async def _actualizar() -> None:
            actualizar_btn.props("loading")
            ui.notify("Leyendo Tienda Nube...", color="info")
            try:
                filas_tn, error = await run.io_bound(
                    tiendanube_list_products_with_variants,
                    tn_creds["store_id"], tn_creds["access_token"], tn_creds["auth_header_style"],
                )
                replace_tiendanube_productos(uid, filas_tn)
                set_tiendanube_sync_status(uid, ok=(error is None), error=error, items_leidos=len(filas_tn))
                if error:
                    ui.notify(f"Sincronización incompleta -- ver detalle en pantalla", color="negative")
                else:
                    ui.notify(f"OK: {len(filas_tn)} variantes leídas de Tienda Nube", color="positive")
            finally:
                actualizar_btn.props(remove="loading")
            _render_status()
            _render_tabla()

        actualizar_btn.on_click(_actualizar)
        filtro_sel.on_value_change(lambda: _render_tabla())
        busqueda_input.on_value_change(lambda: _render_tabla())
        incluir_pausadas_chk.on_value_change(lambda: _render_tabla())

        _render_status()
        _render_tabla()


# =============================================================================
# Diferencias -- SOLO LECTURA: SKUs en ambas plataformas, precio objetivo TN vs
# actual y stock ML vs TN. La sincronización (crear/actualizar en TN) es de
# Vinculación, no de acá.
# =============================================================================


def _construir_filas_diferencias(ml_items: List[dict], tn_rows: List[dict], uid: int) -> List[Dict[str, Any]]:
    """Universo: SOLO SKUs presentes en ML y en TN a la vez -- los de una sola
    plataforma son ruido acá, eso lo resuelve Vinculación."""
    ml_by_sku: Dict[str, List[dict]] = defaultdict(list)
    tn_by_sku: Dict[str, List[dict]] = defaultdict(list)
    for it in ml_items:
        sku = (it.get("seller_sku") or "").strip().lower()
        if sku:
            ml_by_sku[sku].append(it)
    for r in tn_rows:
        sku = (r.get("sku") or "").strip().lower()
        if sku:
            tn_by_sku[sku].append(r)

    descuento_global = float(get_cotizador_param("tn_descuento_pct", uid) or COTIZADOR_DEFAULTS["tn_descuento_pct"])

    filas = []
    for sku in sorted(set(ml_by_sku) & set(tn_by_sku)):
        ml_m = ml_by_sku[sku]
        tn_m = tn_by_sku[sku]

        ml_contado = precio_contado_ml(ml_m)
        objetivo = calcular_precio_tn(sku, ml_items, uid)
        tn_precio_raw = tn_m[0].get("precio")
        promo_raw = tn_m[0].get("promotional_price")
        ml_stock = ml_m[0].get("available_quantity")
        tn_stock = tn_m[0].get("stock")

        # promotional_price viene null cuando NO hay promo (confirmado en vivo
        # 2026-08-28 -- nunca "0"/"0.00"/"" para "sin promo"), pero se chequean
        # esos valores igual por si TN cambia el contrato sin avisar.
        tiene_promo = promo_raw not in (None, "", "0.00", 0)

        # Comparación SIEMPRE con Decimal, nunca float ni string -- el objetivo
        # sale de un cálculo con decimales y TN devuelve el precio como string.
        precio_diff: Optional[int] = None
        precio_diff_pct: Optional[float] = None
        precio_estado = "SIN_DATOS"
        promo_alerta = False
        if tiene_promo:
            # Mercado Libre es la fuente de verdad, pero promotional_price lo
            # maneja Diego a mano desde el panel de TN -- un campo, un dueño.
            # Nunca se compara/corrige automáticamente contra el objetivo.
            precio_estado = "PROMO_ACTIVA"
            if objetivo is not None:
                try:
                    promo_alerta = Decimal(str(promo_raw)) > Decimal(str(round(objetivo)))
                except (InvalidOperation, TypeError):
                    pass
        elif objetivo is not None and tn_precio_raw not in (None, ""):
            try:
                tn_dec = Decimal(str(tn_precio_raw))
                obj_dec = Decimal(str(round(objetivo)))
                diff_dec = tn_dec - obj_dec
                precio_diff = int(diff_dec)
                precio_diff_pct = float(diff_dec / obj_dec * 100) if obj_dec != 0 else 0.0
                precio_estado = "ALINEADO" if diff_dec == 0 else "DIFERENTE"
            except (InvalidOperation, TypeError):
                pass
        precio_corregible = (
            precio_estado == "DIFERENTE" and precio_diff_pct is not None and abs(precio_diff_pct) <= 30
        )

        # Stock: ML es la fuente de verdad, cualquier diferencia es real. Los
        # estados TN=0/ML>0 se separan por si ESE 0 lo escribimos nosotros
        # (tn_escrituras lo dice) o lo puso una venta real en TN.
        stock_diff: Optional[int] = None
        stock_estado = "SIN_DATOS"
        if ml_stock is not None and tn_stock is not None:
            try:
                ml_i, tn_i = int(ml_stock), int(tn_stock)
                stock_diff = tn_i - ml_i
                if stock_diff == 0:
                    stock_estado = "OK"
                elif ml_i == 0 and tn_i > 0:
                    stock_estado = "AGOTADO_ML"
                elif tn_i == 0 and ml_i > 0:
                    ultima = get_ultima_escritura_stock(uid, sku)
                    stock_estado = "REPONER" if (ultima and ultima.get("valor_nuevo") == "0") else "REVISAR_VENTA_TN"
                else:
                    stock_estado = "REVISAR"
            except (ValueError, TypeError):
                pass

        filas.append({
            "sku": sku,
            "nombre": ml_m[0].get("title", ""),
            "tn_product_id": tn_m[0].get("product_id"),
            "tn_variant_id": tn_m[0].get("variant_id"),
            "ml_contado": ml_contado,
            "tn_objetivo": objetivo,
            "tn_precio": tn_precio_raw,
            "tn_promotional_price": promo_raw if tiene_promo else None,
            "precio_diff": precio_diff,
            "precio_diff_pct": precio_diff_pct,
            "precio_estado": precio_estado,
            "precio_alineado": precio_estado == "ALINEADO",
            "precio_corregible": precio_corregible,
            "promo_alerta": promo_alerta,
            "ml_stock": ml_stock,
            "tn_stock": tn_stock,
            "stock_diff": stock_diff,
            "stock_estado": stock_estado,
            "stock_alineado": stock_estado == "OK",
            "descuento_override": get_producto_tn_descuento(sku, uid),
            "descuento_global": descuento_global,
        })
    return filas


def _fmt_pesos_dif(v: Any) -> str:
    return _fmt_precio_ars(v) if v not in (None, "") else "—"


def _fmt_delta_pesos_dif(v: float) -> str:
    """Para variaciones (puede ser negativo -- una baja de precio): _fmt_precio_ars
    ya antepone el signo "-" a los negativos, acá solo se agrega "+" a los positivos."""
    return ("+" if v >= 0 else "") + _fmt_precio_ars(v)


def build_tab_diferencias(container) -> None:
    container.clear()
    user = _require_login()
    if not user:
        return
    uid = user["id"]

    access_token = get_ml_access_token(uid)
    tn_creds = get_tiendanube_credentials(uid)
    if not access_token:
        with container:
            ui.label("⚠️ No tenés MercadoLibre vinculado. Andá a Configuración.").classes("text-warning")
        return
    if not tn_creds or not tn_creds.get("store_id") or not tn_creds.get("access_token") or not tn_creds.get("auth_header_style"):
        with container:
            ui.label("⚠️ No tenés Tienda Nube vinculada (o falta 'Probar conexión' en Configuración).").classes("text-warning")
        return

    # Estado compartido entre el render de la tabla y los handlers de acciones
    # (lote y fila viven fuera de _render_tabla_dif, necesitan ver las filas
    # completas -- no solo las visibles tras filtro/búsqueda -- y si la
    # escritura está habilitada este render).
    seleccionados_ref: Dict[str, Any] = {"ids": set()}
    filas_ref: Dict[str, Any] = {"data": [], "escritura_habilitada": False}

    with container:
        ui.label("Tienda Nube — Diferencias").classes("text-xl font-bold")
        ui.label(
            "MercadoLibre es la fuente de verdad. Nunca se escribe de Tienda Nube hacia MercadoLibre."
        ).classes("text-xs text-gray-500")

        status_container = ui.column().classes("w-full")
        guardrail_container = ui.column().classes("w-full")

        with ui.row().classes("w-full items-center gap-3 flex-wrap"):
            filtro_opciones_dif = {
                "con_diferencias": "Solo con diferencias",
                "alineados": "Alineados",
                "dif_precio": "Con diferencia de precio",
                "dif_stock": "Con diferencia de stock",
                "todos": "Todos",
            }
            filtro_sel_dif = ui.select(filtro_opciones_dif, value="con_diferencias", label="Estado").props("dense outlined").classes("w-64")
            busqueda_input_dif = ui.input(placeholder="Buscar por SKU o nombre...").props(
                "dense outlined clearable debounce=300"
            ).classes("w-64")
            ui.space()
            actualizar_btn_dif = ui.button("Actualizar").props("unelevated dense no-caps icon=refresh").classes("text-xs")
            ultima_sync_lbl_dif = ui.label("").classes("text-xs text-gray-600")

        with ui.row().classes("w-full items-center gap-2 flex-wrap"):
            lote_precio_btn = ui.button("Corregir precios seleccionados").props(
                "dense no-caps outlined color=primary icon=price_change disabled"
            ).classes("text-xs")
            lote_cero_btn = ui.button("Poner en 0 los AGOTADO_ML seleccionados").props(
                "dense no-caps outlined color=negative icon=production_quantity_limits disabled"
            ).classes("text-xs")
            lote_sel_lbl = ui.label("0 seleccionados").classes("text-xs text-gray-500")

        contadores_container_dif = ui.row().classes("w-full gap-2 flex-wrap")
        header_div_dif = ui.element("div").style("width:100%;overflow:hidden")
        table_container_dif = ui.element("div").style("width:100%;height:calc(100vh - 454px);overflow-y:scroll;overflow-x:auto")
        _hid_d = header_div_dif.id
        _cid_d = table_container_dif.id
        _sync_dif_client = context.client

        async def _setup_sync_dif() -> None:
            with _sync_dif_client:
                await ui.run_javascript(
                    f"(function(){{"
                    f"var body=document.getElementById('c{_cid_d}');"
                    f"var hdr=document.getElementById('c{_hid_d}');"
                    f"if(!body||!hdr)return;"
                    f"body.addEventListener('scroll',function(){{hdr.scrollLeft=body.scrollLeft;}});"
                    f"function _sg(){{hdr.style.paddingRight=(body.offsetWidth-body.clientWidth)+'px';}}"
                    f"_sg();new ResizeObserver(_sg).observe(body);"
                    f"}})();"
                )
        background_tasks.create(_setup_sync_dif())

        columns_dif = [
            {"name": "sel", "label": "", "field": "sel", "align": "center", "sortable": False},
            {"name": "sku", "label": "SKU", "field": "sku", "align": "left"},
            {"name": "nombre", "label": "Nombre", "field": "nombre", "align": "left"},
            {"name": "ml_contado", "label": "ML — Contado", "field": "ml_contado", "align": "right"},
            {"name": "tn_objetivo", "label": "TN — Objetivo", "field": "tn_objetivo", "align": "right"},
            {"name": "tn_precio", "label": "TN — Actual", "field": "tn_precio", "align": "right"},
            {"name": "precio_estado", "label": "Precio — Estado", "field": "precio_estado", "align": "center", "sortable": False},
            {"name": "ml_stock", "label": "ML — Stock", "field": "ml_stock", "align": "right"},
            {"name": "tn_stock", "label": "TN — Stock", "field": "tn_stock", "align": "right"},
            {"name": "stock_estado", "label": "Stock — Estado", "field": "stock_estado", "align": "center", "sortable": False},
            {"name": "descuento", "label": "Desc. %", "field": "descuento", "align": "center", "sortable": False},
            {"name": "acciones", "label": "Acciones", "field": "acciones", "align": "center", "sortable": False},
        ]
        _col_w_dif = {
            "sel": "32px", "sku": "110px", "nombre": "220px", "ml_contado": "100px", "tn_objetivo": "100px",
            "tn_precio": "100px", "precio_estado": "170px", "ml_stock": "70px", "tn_stock": "70px",
            "stock_estado": "150px", "descuento": "90px", "acciones": "150px",
        }

        def _build_colgroup_dif() -> None:
            with ui.element("colgroup"):
                for col in columns_dif:
                    ui.element("col").style(f"width:{_col_w_dif.get(col['name'], '90px')}")

        sort_col_ref_dif: Dict[str, Any] = {"val": "sku"}
        sort_asc_ref_dif: Dict[str, bool] = {"val": True}

        def _sort_key_dif(row: dict, col_name: str) -> Any:
            if col_name in ("ml_contado", "tn_objetivo", "ml_stock", "tn_stock"):
                v = row.get(col_name)
                return float(v) if v is not None else -1.0
            if col_name == "tn_precio":
                try:
                    return float(str(row.get("tn_precio")).replace(",", "."))
                except (ValueError, TypeError):
                    return -1.0
            return str(row.get(col_name) or "").lower()

        def _on_sort_click_dif(col_name: str) -> None:
            if sort_col_ref_dif.get("val") == col_name:
                sort_asc_ref_dif["val"] = not sort_asc_ref_dif["val"]
            else:
                sort_col_ref_dif["val"] = col_name
                sort_asc_ref_dif["val"] = True
            _render_tabla_dif()

        def _render_status_dif() -> None:
            status_container.clear()
            st = get_tiendanube_sync_status(uid)
            with status_container:
                if not st or not st.get("last_sync_at"):
                    ultima_sync_lbl_dif.set_text("Nunca se sincronizó — apretá Actualizar")
                    ultima_sync_lbl_dif.classes(replace="text-xs text-warning")
                    return
                relativo = _formatear_ultima_sync(st["last_sync_at"])
                if st.get("ok"):
                    ultima_sync_lbl_dif.set_text(f"Última sincronización: {relativo}")
                    ultima_sync_lbl_dif.classes(replace="text-xs text-gray-600")
                else:
                    ultima_sync_lbl_dif.set_text(f"Última sincronización: {relativo} — FALLÓ")
                    ultima_sync_lbl_dif.classes(replace="text-xs text-negative")
                    with ui.row().classes("w-full items-center gap-2 p-2 rounded").style("background:#fef2f2;border:1px solid #fecaca"):
                        ui.icon("error", color="negative", size="sm")
                        ui.label(f"Sincronización incompleta/fallida: {st.get('error') or 'sin detalle'}").classes("text-sm text-negative")

        def _actualizar_lote_btns() -> None:
            n = len(seleccionados_ref["ids"])
            lote_sel_lbl.set_text(f"{n} seleccionados")
            habilitado = filas_ref["escritura_habilitada"] and n > 0
            for btn in (lote_precio_btn, lote_cero_btn):
                if habilitado:
                    btn.props(remove="disabled")
                else:
                    btn.props("disabled")

        def _render_tabla_dif() -> None:
            ml_data = ml_get_my_items(access_token, include_paused=True)
            ml_items_actuales = ml_data.get("results", [])
            tn_rows = get_tiendanube_productos(uid)
            filas = _construir_filas_diferencias(ml_items_actuales, tn_rows, uid)
            filas_ref["data"] = filas

            # Guardrail: una lectura de ML que vuelve vacía, o donde casi todos los
            # SKUs vinculados aparecen sin stock, es una lectura FALLIDA, no la
            # realidad -- no se escribe nada mientras esto no se resuelva.
            n_total = len(filas)
            n_sin_ml_stock = sum(1 for r in filas if r["ml_stock"] in (0, None))
            lectura_sospechosa = len(ml_items_actuales) == 0 or (n_total >= 5 and n_sin_ml_stock / n_total >= 0.9)
            filas_ref["escritura_habilitada"] = not lectura_sospechosa

            guardrail_container.clear()
            with guardrail_container:
                if lectura_sospechosa:
                    with ui.row().classes("w-full items-center gap-2 p-2 rounded").style("background:#fef2f2;border:1px solid #fecaca"):
                        ui.icon("warning", color="negative", size="sm")
                        ui.label(
                            f"Lectura de MercadoLibre sospechosa ({len(ml_items_actuales)} publicaciones traídas, "
                            f"{n_sin_ml_stock}/{n_total} SKUs vinculados sin stock ML) -- no se escribe nada hasta "
                            f"que esto se confirme como real. Probá 'Actualizar' de nuevo."
                        ).classes("text-sm text-negative")
            seleccionados_ref["ids"] = {s for s in seleccionados_ref["ids"] if s in {r["sku"] for r in filas}}
            _actualizar_lote_btns()

            n_alineados = sum(1 for r in filas if r["precio_alineado"] and r["stock_alineado"])
            n_dif_precio = sum(1 for r in filas if not r["precio_alineado"])
            n_dif_stock = sum(1 for r in filas if not r["stock_alineado"])
            n_promo = sum(1 for r in filas if r["precio_estado"] == "PROMO_ACTIVA")
            n_promo_alerta = sum(1 for r in filas if r["promo_alerta"])

            contadores_container_dif.clear()
            with contadores_container_dif:
                ui.badge(f"Alineados: {n_alineados}", color="positive").props("outline")
                ui.badge(f"Con diferencia de precio: {n_dif_precio}", color="negative" if n_dif_precio else "positive").props("outline")
                ui.badge(f"Con diferencia de stock: {n_dif_stock}", color="negative" if n_dif_stock else "positive").props("outline")
                ui.badge(f"Total en ambas plataformas: {len(filas)}", color="secondary").props("outline")
                if n_promo:
                    ui.badge(f"Con promo activa (no se toca): {n_promo}", color="info").props("outline")
                if n_promo_alerta:
                    ui.badge(f"⚠ Promo por encima del objetivo: {n_promo_alerta}", color="negative")

            filtro = filtro_sel_dif.value
            if filtro == "alineados":
                visibles = [r for r in filas if r["precio_alineado"] and r["stock_alineado"]]
            elif filtro == "dif_precio":
                visibles = [r for r in filas if not r["precio_alineado"]]
            elif filtro == "dif_stock":
                visibles = [r for r in filas if not r["stock_alineado"]]
            elif filtro == "todos":
                visibles = filas
            else:  # "con_diferencias" (default)
                visibles = [r for r in filas if not r["precio_alineado"] or not r["stock_alineado"]]

            busqueda = (busqueda_input_dif.value or "").strip().lower()
            if busqueda:
                visibles = [
                    r for r in visibles
                    if busqueda in r["sku"] or busqueda in (r["nombre"] or "").lower()
                ]

            visibles = sorted(
                visibles,
                key=lambda r: _sort_key_dif(r, sort_col_ref_dif.get("val", "sku")),
                reverse=not sort_asc_ref_dif.get("val", True),
            )

            header_div_dif.clear()
            table_container_dif.clear()
            if not visibles:
                with table_container_dif:
                    ui.label("Sin resultados para este filtro.").classes("text-sm text-gray-400")
                return

            with header_div_dif:
                with ui.element("table").style("table-layout:fixed;width:100%;border-collapse:separate;border-spacing:0"):
                    _build_colgroup_dif()
                    with ui.element("thead"):
                        with ui.element("tr").classes("bg-primary text-white font-semibold"):
                            for col in columns_dif:
                                with ui.element("th").classes("px-2 py-1 border text-center").style("line-height:1.1"):
                                    if col["name"] == "sel":
                                        skus_visibles = {r["sku"] for r in visibles}
                                        todos_marcados = bool(skus_visibles) and skus_visibles.issubset(seleccionados_ref["ids"])
                                        chk_all = ui.checkbox(value=todos_marcados).props("dense color=white").classes("scale-90")
                                        def _on_toggle_all(e: Any, _visibles=visibles) -> None:
                                            if e.value:
                                                seleccionados_ref["ids"].update(r["sku"] for r in _visibles)
                                            else:
                                                seleccionados_ref["ids"].difference_update(r["sku"] for r in _visibles)
                                            _actualizar_lote_btns()
                                            _render_tabla_dif()
                                        chk_all.on_value_change(_on_toggle_all)
                                    elif col.get("sortable", True):
                                        ui.button(
                                            col["label"], on_click=lambda c=col["name"]: _on_sort_click_dif(c)
                                        ).props("flat dense no-caps").classes(
                                            "text-white hover:bg-white/20 cursor-pointer font-semibold"
                                        ).style(
                                            "white-space:nowrap;overflow:hidden;text-overflow:ellipsis;"
                                            "max-width:100%;min-height:0;padding:2px 6px;line-height:1.1"
                                        )
                                    else:
                                        ui.label(col["label"]).classes("font-semibold").style("line-height:1.1")

            with table_container_dif:
                with ui.element("table").style("table-layout:fixed;width:100%;border-collapse:separate;border-spacing:0"):
                    _build_colgroup_dif()
                    with ui.element("tbody"):
                        for row in visibles:
                            with ui.element("tr").classes("border-t border-gray-200 hover:bg-gray-50"):
                                for col in columns_dif:
                                    align = "text-right" if col["align"] == "right" else "text-center" if col["align"] == "center" else "text-left"
                                    with ui.element("td").classes(f"px-2 py-1 border-b border-gray-100 {align} text-xs").style("white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:0"):
                                        if col["name"] == "sel":
                                            chk_row = ui.checkbox(value=row["sku"] in seleccionados_ref["ids"]).props("dense").classes("scale-90")
                                            def _on_toggle_row(e: Any, _sku: str = row["sku"]) -> None:
                                                if e.value:
                                                    seleccionados_ref["ids"].add(_sku)
                                                else:
                                                    seleccionados_ref["ids"].discard(_sku)
                                                _actualizar_lote_btns()
                                            chk_row.on_value_change(_on_toggle_row)
                                        elif col["name"] == "precio_estado":
                                            if row["precio_estado"] == "PROMO_ACTIVA":
                                                ui.label("Promo activa").classes("text-xs text-info font-medium")
                                                if row["promo_alerta"]:
                                                    ui.label("⚠ promo > objetivo").classes("text-xs text-negative font-bold")
                                            elif row["precio_estado"] == "SIN_DATOS":
                                                ui.label("Sin datos").classes("text-xs text-gray-400")
                                            elif row["precio_estado"] == "ALINEADO":
                                                ui.label("Alineado").classes("text-xs text-positive font-medium")
                                            else:
                                                signo = "+" if row["precio_diff"] > 0 else ""
                                                monto = f"{row['precio_diff']:,}".replace(",", ".")
                                                ui.label(f"{signo}{monto} ({signo}{row['precio_diff_pct']:.1f}%)").classes("text-xs text-negative font-medium")
                                                if not row["precio_corregible"]:
                                                    ui.label("(>30%, revisar a mano)").classes("text-xs text-warning")
                                        elif col["name"] == "stock_estado":
                                            _ESTADO_LBL = {
                                                "OK": ("Alineado", "text-positive"),
                                                "AGOTADO_ML": ("AGOTADO EN ML", "text-negative font-bold"),
                                                "REPONER": ("Reponer (0 propio)", "text-warning"),
                                                "REVISAR_VENTA_TN": ("Revisar: venta TN?", "text-negative font-bold"),
                                                "REVISAR": ("Revisar", "text-negative"),
                                                "SIN_DATOS": ("Sin datos", "text-gray-400"),
                                            }
                                            lbl, cls = _ESTADO_LBL.get(row["stock_estado"], ("—", "text-gray-400"))
                                            ui.label(lbl).classes(f"text-xs font-medium {cls}")
                                            if row["stock_diff"] is not None and row["stock_estado"] != "OK":
                                                signo = "+" if row["stock_diff"] > 0 else ""
                                                ui.label(f"TN {row['tn_stock']} / ML {row['ml_stock']} ({signo}{row['stock_diff']})").classes("text-[10px] text-gray-400")
                                        elif col["name"] == "descuento":
                                            val_str = "" if row["descuento_override"] is None else f"{row['descuento_override']:g}"
                                            inp = ui.input(
                                                value=val_str,
                                                placeholder=f"{row['descuento_global']:g} (global)",
                                            ).props(
                                                'dense outlined hide-bottom-space input-style="text-align:right;font-size:11px;padding:0 4px"'
                                            ).style("width:100%")
                                            if row["descuento_override"] is not None:
                                                inp.classes("text-blue-700 font-semibold")

                                            def _on_blur_descuento(_e: Any = None, _sku: str = row["sku"], _inp: Any = inp) -> None:
                                                raw = str(_inp.value or "").strip()
                                                if raw == "":
                                                    set_producto_tn_descuento(_sku, uid, None)
                                                else:
                                                    try:
                                                        n = float(raw.replace(",", "."))
                                                    except (ValueError, TypeError):
                                                        ui.notify(f"{_sku}: valor inválido", type="negative")
                                                        return
                                                    if n < 0 or n > 50:
                                                        ui.notify(f"{_sku}: debe estar entre 0 y 50", type="negative")
                                                        return
                                                    set_producto_tn_descuento(_sku, uid, n)
                                                _render_tabla_dif()

                                            inp.on("blur", _on_blur_descuento)
                                        elif col["name"] in ("ml_contado", "tn_objetivo"):
                                            v = row.get(col["name"])
                                            ui.label(_fmt_precio_ars(v) if v is not None else "—")
                                        elif col["name"] == "tn_precio":
                                            ui.label(_fmt_precio_ars(row.get("tn_precio")) if row.get("tn_precio") not in (None, "") else "—")
                                            if row.get("tn_promotional_price") is not None:
                                                ui.label(f"promo: {_fmt_precio_ars(row['tn_promotional_price'])}").classes("text-[10px] text-info")
                                        elif col["name"] == "acciones":
                                            if not filas_ref["escritura_habilitada"]:
                                                ui.label("—").classes("text-xs text-gray-400")
                                            elif row["stock_estado"] in ("REVISAR", "REVISAR_VENTA_TN"):
                                                with ui.row().classes("items-center gap-1 justify-center flex-nowrap"):
                                                    manual_inp = ui.input(placeholder="stock").props(
                                                        'dense outlined hide-bottom-space input-style="text-align:right;font-size:11px;padding:0 4px"'
                                                    ).style("width:56px")
                                                    ui.button(icon="save").props("dense flat size=sm color=primary").on_click(
                                                        lambda r=row, i=manual_inp: _accion_stock_manual(r, i)
                                                    )
                                            else:
                                                with ui.row().classes("items-center gap-1 justify-center flex-nowrap"):
                                                    if row["precio_corregible"]:
                                                        ui.button(icon="price_change").props("dense flat size=sm color=primary").tooltip(
                                                            f"Corregir precio: {_fmt_pesos_dif(row['tn_precio'])} → {_fmt_pesos_dif(row['tn_objetivo'])}"
                                                        ).on_click(lambda r=row: _accion_corregir_precio_fila(r))
                                                    if row["stock_estado"] == "AGOTADO_ML":
                                                        ui.button(icon="production_quantity_limits").props("dense flat size=sm color=negative").tooltip(
                                                            f"Poner en 0: TN {row['tn_stock']} → 0"
                                                        ).on_click(lambda r=row: _accion_poner_cero_fila(r))
                                                    if row["stock_estado"] == "REPONER":
                                                        ui.button(icon="restore").props("dense flat size=sm color=positive").tooltip(
                                                            f"Reponer a ML: TN 0 → {row['ml_stock']}"
                                                        ).on_click(lambda r=row: _accion_reponer_fila(r))
                                        else:
                                            v = row.get(col["name"])
                                            ui.label(str(v) if v is not None else "—")

            async def _recalc_padding_dif() -> None:
                with _sync_dif_client:
                    await ui.run_javascript(
                        f"(function(){{"
                        f"var body=document.getElementById('c{_cid_d}');"
                        f"var hdr=document.getElementById('c{_hid_d}');"
                        f"if(body&&hdr){{hdr.style.paddingRight=(body.offsetWidth-body.clientWidth)+'px';}}"
                        f"}})();"
                    )
            background_tasks.create(_recalc_padding_dif())

        def _dialogo_confirmar_dif(
            titulo: str, resumen: List[str], filas_preview: List[tuple], confirm_label: str, on_confirm,
        ) -> None:
            """Ninguna acción escribe sin mostrar antes exactamente qué va a escribir.
            resumen: líneas agregadas arriba de todo (conteo, variación promedio/máxima,
            impacto total) -- en un lote grande, la lista fila por fila no comunica
            escala, un renglón con el total sí. filas_preview: lista de
            (sku, 'TN $214.800 -> $193.900'), desplegable, colapsada por default."""
            with ui.dialog() as dlg, ui.card().classes("min-w-[460px] max-w-[640px]"):
                ui.label(titulo).classes("text-lg font-bold")
                if resumen:
                    with ui.column().classes("gap-1 w-full py-2 px-3 rounded mt-1").style(
                        "background:#f8fafc;border:1px solid #e5e7eb"
                    ):
                        for linea in resumen:
                            ui.label(linea).classes("text-sm font-medium text-gray-800")

                detalle_col = ui.column().classes("gap-1 max-h-[280px] overflow-y-auto w-full mt-2")
                detalle_col.set_visibility(False)
                detalle_visible = [False]
                with ui.row().classes("items-center gap-1 cursor-pointer mt-2") as toggle_row:
                    chev = ui.icon("chevron_right").classes("text-gray-500")
                    ui.label(f"Ver detalle ({len(filas_preview)} filas)").classes("text-xs text-gray-500")

                def _toggle_detalle() -> None:
                    detalle_visible[0] = not detalle_visible[0]
                    detalle_col.set_visibility(detalle_visible[0])
                    chev.props(f"name={'expand_more' if detalle_visible[0] else 'chevron_right'}")
                toggle_row.on("click", lambda: _toggle_detalle())

                with detalle_col:
                    for sku, desc in filas_preview:
                        with ui.row().classes("items-center gap-2 w-full"):
                            ui.label(sku).classes("text-xs font-mono text-gray-600 w-40 shrink-0")
                            ui.label(desc).classes("text-xs")
                with ui.row().classes("w-full justify-end gap-2 mt-3"):
                    ui.button("Cancelar", on_click=dlg.close).props("flat")
                    confirmar_btn = ui.button(confirm_label).props("unelevated color=primary")
                    async def _confirmado() -> None:
                        confirmar_btn.props("loading")
                        try:
                            await on_confirm()
                        finally:
                            dlg.close()
                    confirmar_btn.on_click(_confirmado)
            dlg.open()

        def _accion_corregir_precio_fila(row: dict) -> None:
            desc = f"TN {_fmt_pesos_dif(row['tn_precio'])} → {_fmt_pesos_dif(row['tn_objetivo'])}"
            async def _hacer() -> None:
                ok, msg = await run.io_bound(
                    escribir_tn_verificado, uid, tn_creds, row["sku"], row["tn_product_id"], row["tn_variant_id"],
                    "precio", row["tn_precio"], round(row["tn_objetivo"]), "manual",
                )
                ui.notify(f"{row['sku']}: {'precio corregido' if ok else 'ERROR — ' + msg}", type="positive" if ok else "negative")
                _render_tabla_dif()
            _dialogo_confirmar_dif("Corregir precio", [], [(row["sku"], desc)], "Aplicar", _hacer)

        def _accion_poner_cero_fila(row: dict) -> None:
            desc = f"TN {row['tn_stock']} → 0"
            async def _hacer() -> None:
                ok, msg = await run.io_bound(
                    escribir_tn_verificado, uid, tn_creds, row["sku"], row["tn_product_id"], row["tn_variant_id"],
                    "stock", row["tn_stock"], 0, "manual",
                )
                ui.notify(f"{row['sku']}: {'puesto en 0' if ok else 'ERROR — ' + msg}", type="positive" if ok else "negative")
                _render_tabla_dif()
            _dialogo_confirmar_dif("Poner stock en 0 (agotado en ML)", [], [(row["sku"], desc)], "Aplicar", _hacer)

        def _accion_reponer_fila(row: dict) -> None:
            desc = f"TN 0 → {row['ml_stock']}"
            async def _hacer() -> None:
                ok, msg = await run.io_bound(
                    escribir_tn_verificado, uid, tn_creds, row["sku"], row["tn_product_id"], row["tn_variant_id"],
                    "stock", 0, row["ml_stock"], "manual",
                )
                ui.notify(f"{row['sku']}: {'repuesto' if ok else 'ERROR — ' + msg}", type="positive" if ok else "negative")
                _render_tabla_dif()
            _dialogo_confirmar_dif("Reponer a stock de ML", [], [(row["sku"], desc)], "Aplicar", _hacer)

        def _accion_stock_manual(row: dict, inp: Any) -> None:
            raw = str(inp.value or "").strip()
            try:
                nuevo = int(raw)
                if nuevo < 0:
                    raise ValueError
            except (ValueError, TypeError):
                ui.notify(f"{row['sku']}: stock inválido (entero, ≥ 0)", type="negative")
                return
            desc = f"TN {row['tn_stock']} → {nuevo} (carga manual, criterio de Diego)"
            async def _hacer() -> None:
                ok, msg = await run.io_bound(
                    escribir_tn_verificado, uid, tn_creds, row["sku"], row["tn_product_id"], row["tn_variant_id"],
                    "stock", row["tn_stock"], nuevo, "manual",
                )
                ui.notify(f"{row['sku']}: {'guardado' if ok else 'ERROR — ' + msg}", type="positive" if ok else "negative")
                _render_tabla_dif()
            _dialogo_confirmar_dif("Cargar stock manual", [], [(row["sku"], desc)], "Aplicar", _hacer)

        def _accion_lote_precios() -> None:
            filas = filas_ref["data"]
            marcadas = [r for r in filas if r["sku"] in seleccionados_ref["ids"]]
            corregibles = [r for r in marcadas if r["precio_corregible"]]
            excluidas = [r for r in marcadas if not r["precio_corregible"]]
            if not corregibles:
                ui.notify("Ninguna fila seleccionada es corregible (promo activa, >30%, sin datos o ya alineada)", type="warning")
                return
            n = len(corregibles)
            # cambio = lo que se le suma al precio actual para llegar al objetivo
            # (negativo = baja). Usa precio_diff/precio_diff_pct ya calculados en
            # _construir_filas_diferencias (Decimal, no float) -- no reimplementa la resta.
            cambios = [(-r["precio_diff"], -r["precio_diff_pct"]) for r in corregibles]
            promedio_pct = sum(c[1] for c in cambios) / n
            idx_max = max(range(n), key=lambda i: abs(cambios[i][1]))
            impacto_total = sum(c[0] for c in cambios)
            resumen = [
                f"{n} productos se van a modificar",
                f"Variación promedio: {promedio_pct:+.1f}%",
                f"Variación máxima: {corregibles[idx_max]['sku']} {cambios[idx_max][1]:+.1f}% ({_fmt_delta_pesos_dif(cambios[idx_max][0])})",
                f"Impacto total sobre el precio de lista del catálogo: {_fmt_delta_pesos_dif(impacto_total)}",
            ]
            preview = [(r["sku"], f"TN {_fmt_pesos_dif(r['tn_precio'])} → {_fmt_pesos_dif(r['tn_objetivo'])}") for r in corregibles]
            if excluidas:
                preview.append(("(excluidas)", f"{len(excluidas)} de las seleccionadas NO se tocan -- ver motivo en la columna Precio — Estado"))
            async def _hacer() -> None:
                ok_n = err_n = 0
                for r in corregibles:
                    ok, msg = await run.io_bound(
                        escribir_tn_verificado, uid, tn_creds, r["sku"], r["tn_product_id"], r["tn_variant_id"],
                        "precio", r["tn_precio"], round(r["tn_objetivo"]), "lote",
                    )
                    if ok:
                        ok_n += 1
                    else:
                        err_n += 1
                ui.notify(f"Lote precios: {ok_n} OK, {err_n} error", type="positive" if err_n == 0 else "warning")
                seleccionados_ref["ids"].clear()
                _render_tabla_dif()
            _dialogo_confirmar_dif("Corregir precios", resumen, preview, f"Aplicar a {n} productos", _hacer)

        def _accion_lote_cero() -> None:
            filas = filas_ref["data"]
            marcadas = [r for r in filas if r["sku"] in seleccionados_ref["ids"]]
            agotados = [r for r in marcadas if r["stock_estado"] == "AGOTADO_ML"]
            excluidas = [r for r in marcadas if r["stock_estado"] != "AGOTADO_ML"]
            if not agotados:
                ui.notify("Ninguna fila seleccionada está en AGOTADO_ML", type="warning")
                return
            n = len(agotados)
            unidades_totales = sum(int(r["tn_stock"] or 0) for r in agotados)
            resumen = [
                f"{n} productos se van a poner en 0",
                f"Unidades de stock que dejan de verse en TN: {unidades_totales}",
            ]
            preview = [(r["sku"], f"TN {r['tn_stock']} → 0") for r in agotados]
            if excluidas:
                preview.append(("(excluidas)", f"{len(excluidas)} de las seleccionadas no están en AGOTADO_ML, no se tocan"))
            async def _hacer() -> None:
                ok_n = err_n = 0
                for r in agotados:
                    ok, msg = await run.io_bound(
                        escribir_tn_verificado, uid, tn_creds, r["sku"], r["tn_product_id"], r["tn_variant_id"],
                        "stock", r["tn_stock"], 0, "lote",
                    )
                    if ok:
                        ok_n += 1
                    else:
                        err_n += 1
                ui.notify(f"Lote stock=0: {ok_n} OK, {err_n} error", type="positive" if err_n == 0 else "warning")
                seleccionados_ref["ids"].clear()
                _render_tabla_dif()
            _dialogo_confirmar_dif(
                "Poner en 0 (agotados en ML)", resumen, preview, f"Aplicar a {n} productos", _hacer,
            )

        lote_precio_btn.on_click(_accion_lote_precios)
        lote_cero_btn.on_click(_accion_lote_cero)

        async def _actualizar_dif() -> None:
            actualizar_btn_dif.props("loading")
            ui.notify("Leyendo Tienda Nube...", color="info")
            try:
                filas_tn, error = await run.io_bound(
                    tiendanube_list_products_with_variants,
                    tn_creds["store_id"], tn_creds["access_token"], tn_creds["auth_header_style"],
                )
                replace_tiendanube_productos(uid, filas_tn)
                set_tiendanube_sync_status(uid, ok=(error is None), error=error, items_leidos=len(filas_tn))
                if error:
                    ui.notify("Sincronización incompleta -- ver detalle en pantalla", color="negative")
                else:
                    ui.notify(f"OK: {len(filas_tn)} variantes leídas de Tienda Nube", color="positive")
            finally:
                actualizar_btn_dif.props(remove="loading")
            _render_status_dif()
            _render_tabla_dif()

        actualizar_btn_dif.on_click(_actualizar_dif)
        filtro_sel_dif.on_value_change(lambda: _render_tabla_dif())
        busqueda_input_dif.on_value_change(lambda: _render_tabla_dif())

        _render_status_dif()
        _render_tabla_dif()
