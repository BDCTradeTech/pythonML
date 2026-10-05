"""
video_ml.py
Lógica de "Descargar video de ML" (pestaña Búsqueda): lee un master .m3u8 público de ML
(http2.mlstatic.com/storage/shorts-api/...), elige la variante de mayor resolución, la mide con
ffprobe y la baja con ffmpeg -c copy. Sin nicegui: lo usa tabs/busqueda.py y los tests.

Seguridad: el servidor SOLO toca https://*.mlstatic.com (master, variantes, segmentos y claves,
validados uno por uno antes de dárselos a ffmpeg), sin cookies ni tokens de ML, con timeouts y tope
de tamaño. Los redirects de nuestros pedidos no se siguen.
"""
from __future__ import annotations

import logging
import os
import re
import secrets
import subprocess
import tempfile
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import unquote, urljoin, urlparse

import requests

MAX_BYTES = 200 * 1024 * 1024          # tope de la descarga
HTTP_TIMEOUT = 15
PROBE_TIMEOUT = 40
DOWNLOAD_TIMEOUT = 240
MAX_URLS = 10
SHORT_MAX_SEG = 60.0
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
_FF_FLAGS = ["-protocol_whitelist", "http,https,tcp,tls,crypto"]
_URI_ATTR = re.compile(r'URI="([^"]+)"')

# Dominio de producción: el marcador abre la pestaña Búsqueda de acá.
BASE_URL_APP = os.getenv("VIDEO_ML_BASE_URL", "https://bdctechtrade.com").rstrip("/")


class VideoError(Exception):
    """Error mostrable al usuario."""


# ---------------------------------------------------------------------------
# URLs
# ---------------------------------------------------------------------------

def url_permitida(url: str) -> bool:
    """https y host http2.mlstatic.com o *.mlstatic.com; nada de credenciales ni puertos raros."""
    try:
        u = urlparse(url.strip())
    except ValueError:
        return False
    if u.scheme != "https" or not u.hostname or u.username or u.password:
        return False
    if u.port not in (None, 443):
        return False
    h = u.hostname.lower()
    return h == "mlstatic.com" or h.endswith(".mlstatic.com")


def es_link_ml(url: str) -> bool:
    """Link de catálogo (/p/MLA...), de publicación o de cualquier página de ML (no se puede leer desde el servidor)."""
    s = url.strip()
    try:
        host = (urlparse(s).hostname or "").lower()
    except ValueError:
        host = ""
    if re.search(r"(^|\.)mercadolibre\.[a-z.]+$|(^|\.)mercadolivre\.com\.br$|(^|\.)meli\.", host):
        return True
    return bool(re.search(r"/p/MLA\d+|MLA-?\d{6,}", s, re.I)) and not url_permitida(s)


def parsear_entrada(texto: str) -> Tuple[List[str], List[str], List[str]]:
    """(m3u8_validas, links_ml, rechazadas) a partir de un texto con una URL por línea
    (también separadas por coma, como llegan por ?video_m3u8=)."""
    validas: List[str] = []
    links: List[str] = []
    rechazadas: List[str] = []
    for crudo in re.split(r"[\n\r,]+", texto or ""):
        u = crudo.strip()
        if not u:
            continue
        if url_permitida(u) and urlparse(u).path.lower().endswith(".m3u8"):
            if u not in validas:
                validas.append(u)
        elif es_link_ml(u):
            links.append(u)
        else:
            rechazadas.append(u)
    return validas, links, rechazadas


def codigo_de(url: str) -> str:
    """Código para el nombre del archivo: último tramo del path sin .m3u8 (Ov4D5C)."""
    base = unquote(urlparse(url).path.rsplit("/", 1)[-1])
    base = re.sub(r"\.m3u8$", "", base, flags=re.I)
    base = re.sub(r"[^A-Za-z0-9_-]", "", base)
    return base[:40] or "video"


# ---------------------------------------------------------------------------
# Playlists
# ---------------------------------------------------------------------------

def _get_texto(url: str) -> str:
    if not url_permitida(url):
        raise VideoError("URL fuera de mlstatic.com: no se descarga")
    try:
        r = requests.get(url, headers={"User-Agent": _UA}, timeout=HTTP_TIMEOUT, allow_redirects=False, stream=True)
        if r.status_code != 200:
            raise VideoError(f"ML respondió HTTP {r.status_code} para la playlist")
        cuerpo = r.raw.read(2_000_000, decode_content=True)
    except requests.RequestException as e:
        raise VideoError(f"no se pudo leer la playlist: {e}") from e
    texto = cuerpo.decode("utf-8", "replace")
    if not texto.lstrip().startswith("#EXTM3U"):
        raise VideoError("la URL no es una playlist HLS (.m3u8) válida")
    return texto


def _variantes(master: str, base_url: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    lineas = [ln.strip() for ln in master.splitlines() if ln.strip()]
    for i, ln in enumerate(lineas):
        if not ln.startswith("#EXT-X-STREAM-INF:"):
            continue
        attrs = ln.split(":", 1)[1]
        res = re.search(r"RESOLUTION=(\d+)x(\d+)", attrs)
        bw = re.search(r"(?<![A-Z-])BANDWIDTH=(\d+)", attrs)
        avg = re.search(r"AVERAGE-BANDWIDTH=(\d+)", attrs)
        nxt = next((x for x in lineas[i + 1:] if not x.startswith("#")), None)
        if not nxt:
            continue
        w, h = (int(res.group(1)), int(res.group(2))) if res else (0, 0)
        out.append({"url": urljoin(base_url, nxt), "w": w, "h": h,
                    "bw": int(bw.group(1)) if bw else 0, "avg_bw": int(avg.group(1)) if avg else 0})
    return out


def _validar_media_playlist(texto: str, base_url: str) -> None:
    """Todos los segmentos / claves / mapas de la media playlist tienen que ser https *.mlstatic.com
    (ffmpeg los va a pedir; no puede terminar bajando otra cosa)."""
    for ln in texto.splitlines():
        ln = ln.strip()
        if not ln:
            continue
        if ln.startswith("#"):
            for m in _URI_ATTR.finditer(ln):
                if not url_permitida(urljoin(base_url, m.group(1))):
                    raise VideoError("la playlist referencia un host fuera de mlstatic.com: no se descarga")
        elif not url_permitida(urljoin(base_url, ln)):
            raise VideoError("la playlist referencia un host fuera de mlstatic.com: no se descarga")


def elegir_variante(m3u8_url: str) -> Dict[str, Any]:
    """Lee el master, elige la variante de mayor resolución (desempate: mayor bandwidth) y valida su
    media playlist. Si la URL ya es una media playlist, esa es la variante."""
    master = _get_texto(m3u8_url)
    if "#EXT-X-STREAM-INF" in master:
        vs = _variantes(master, m3u8_url)
        if not vs:
            raise VideoError("el master no trae variantes")
        for v in vs:
            if not url_permitida(v["url"]):
                raise VideoError("el master apunta a un host fuera de mlstatic.com: no se descarga")
        mejor = max(vs, key=lambda v: (v["w"] * v["h"], v["bw"]))
        media_url = mejor["url"]
        variantes = [f'{v["w"]}x{v["h"]}' for v in sorted(vs, key=lambda v: v["w"] * v["h"])]
    else:
        mejor = {"url": m3u8_url, "w": 0, "h": 0, "bw": 0, "avg_bw": 0}
        media_url, variantes = m3u8_url, []
        vs = []
    media = master if media_url == m3u8_url else _get_texto(media_url)
    _validar_media_playlist(media, media_url)
    return {"media_url": media_url, "w": mejor["w"], "h": mejor["h"], "bw": mejor["avg_bw"] or mejor["bw"],
            "variantes": variantes}


# ---------------------------------------------------------------------------
# ffprobe / ffmpeg
# ---------------------------------------------------------------------------

def _probe(media_url: str) -> Dict[str, Any]:
    import json
    try:
        p = subprocess.run(
            ["ffprobe", "-v", "error", *_FF_FLAGS, "-print_format", "json", "-show_format", "-show_streams", media_url],
            capture_output=True, text=True, timeout=PROBE_TIMEOUT)
    except FileNotFoundError as e:
        raise VideoError("ffprobe no está instalado en el servidor") from e
    except subprocess.TimeoutExpired as e:
        raise VideoError("ffprobe tardó demasiado leyendo el video") from e
    if p.returncode != 0:
        raise VideoError("ffprobe no pudo leer el video: " + (p.stderr or "").strip()[:200])
    return json.loads(p.stdout or "{}")


def analizar(m3u8_url: str) -> Dict[str, Any]:
    """Ficha del video: variante elegida, resolución, duración, orientación, tamaño estimado y si cumple Short."""
    v = elegir_variante(m3u8_url)
    info = _probe(v["media_url"])
    vs = next((s for s in info.get("streams", []) if s.get("codec_type") == "video"), None)
    if not vs:
        raise VideoError("el stream no tiene video")
    w, h = int(vs.get("width") or v["w"]), int(vs.get("height") or v["h"])
    dur = float((info.get("format") or {}).get("duration") or vs.get("duration") or 0)
    bw = v["bw"] or int(float((info.get("format") or {}).get("bit_rate") or 0))
    est = int(bw * dur / 8) if (bw and dur) else 0
    orient = "vertical" if h > w else ("horizontal" if w > h else "cuadrado")
    return {"url": m3u8_url, "codigo": codigo_de(m3u8_url), "media_url": v["media_url"], "w": w, "h": h,
            "duracion": dur, "orientacion": orient, "tam_estimado": est, "variantes": v["variantes"],
            "codec": vs.get("codec_name"), "cumple_short": bool(h > w and 0 < dur <= SHORT_MAX_SEG),
            "excede_tope": bool(est and est > MAX_BYTES)}


def descargar(media_url: str, codigo: str) -> str:
    """ffmpeg -c copy de la media playlist a un mp4 temporal; devuelve la ruta. Revalida la URL."""
    if not url_permitida(media_url):
        raise VideoError("URL fuera de mlstatic.com: no se descarga")
    _validar_media_playlist(_get_texto(media_url), media_url)
    limpiar_temporales()
    fd, ruta = tempfile.mkstemp(prefix="videoml_", suffix=".mp4")
    os.close(fd)
    try:
        p = subprocess.run(
            ["ffmpeg", "-y", "-loglevel", "error", *_FF_FLAGS, "-i", media_url, "-c", "copy",
             "-bsf:a", "aac_adtstoasc", "-fs", str(MAX_BYTES), "-movflags", "+faststart", ruta],
            capture_output=True, text=True, timeout=DOWNLOAD_TIMEOUT)
        if p.returncode != 0:
            raise VideoError("ffmpeg falló: " + (p.stderr or "").strip()[:200])
        tam = os.path.getsize(ruta)
        if tam <= 0:
            raise VideoError("la descarga quedó vacía")
        if tam >= MAX_BYTES:
            raise VideoError("el video supera el tope de 200 MB y se cortó")
        return ruta
    except subprocess.TimeoutExpired as e:
        _borrar(ruta)
        raise VideoError("la descarga tardó demasiado y se canceló") from e
    except Exception:
        _borrar(ruta)
        raise


def _borrar(ruta: str) -> None:
    try:
        os.remove(ruta)
    except OSError:
        pass


_TEMP_MAX_EDAD_S = 3600


def limpiar_temporales(max_edad_s: int = _TEMP_MAX_EDAD_S) -> int:
    """Borra los videoml_*.mp4 del directorio temporal con más de `max_edad_s` (default 1 hora): restos de
    descargas que no se entregaron o de un reinicio. Se llama al cargar el módulo y en cada descarga."""
    n = 0
    limite = time.time() - max_edad_s
    try:
        for nombre in os.listdir(tempfile.gettempdir()):
            if nombre.startswith("videoml_") and nombre.endswith(".mp4"):
                ruta = os.path.join(tempfile.gettempdir(), nombre)
                try:
                    if os.path.isfile(ruta) and os.path.getmtime(ruta) < limite:
                        os.remove(ruta)
                        n += 1
                except OSError:
                    pass
    except OSError:
        logging.exception("[VIDEO_ML] limpiar_temporales")
    if n:
        logging.info("[VIDEO_ML] %d temporales viejos borrados", n)
    return n


# ---------------------------------------------------------------------------
# Entrega al navegador: token de un solo uso (se emite a un usuario logueado al tocar "Descargar")
# ---------------------------------------------------------------------------

_TOKEN_TTL_S = 600
_TOKENS: Dict[str, Tuple[float, str, str]] = {}   # token -> (vence, ruta, nombre)


def registrar_descarga(ruta: str, codigo: str) -> str:
    _limpiar_vencidos()
    token = secrets.token_urlsafe(32)
    _TOKENS[token] = (time.time() + _TOKEN_TTL_S, ruta, f"{codigo}.mp4")
    return token


def tomar_descarga(token: str) -> Optional[Tuple[str, str]]:
    """(ruta, nombre) y consume el token (un solo uso); None si no existe o venció."""
    _limpiar_vencidos()
    t = _TOKENS.pop(token, None)
    if not t or not os.path.exists(t[1]):
        return None
    return t[1], t[2]


def _limpiar_vencidos() -> None:
    ahora = time.time()
    for k in [k for k, v in _TOKENS.items() if v[0] < ahora]:
        _borrar(_TOKENS.pop(k)[1])


# ---------------------------------------------------------------------------
# Marcador (bookmarklet)
# ---------------------------------------------------------------------------

def bookmarklet_js(base_url: Optional[str] = None) -> str:
    """Código del marcador 'Video ML': busca las .m3u8 de shorts-api en la página de ML
    (recursos de red + HTML normalizando \\u002F y \\/) y abre Búsqueda de PythonML con ?video_m3u8=."""
    base = (base_url or BASE_URL_APP).rstrip("/") + "/"
    js = (
        "(function(){"
        "var re=/https:\\/\\/[a-z0-9.-]*mlstatic\\.com\\/storage\\/shorts-api\\/[^\"'\\s\\\\<>]+?\\.m3u8/g,s={};"
        "function add(t){var m=(t||'').match(re);if(m)m.forEach(function(u){s[u]=1;});}"
        "try{performance.getEntriesByType('resource').forEach(function(e){add(e.name);});}catch(e){}"
        "try{add(document.documentElement.innerHTML.replace(/\\\\u002F/gi,'/').replace(/\\\\\\//g,'/'));}catch(e){}"
        "var l=Object.keys(s);"
        "if(!l.length){alert('Abrí el video de la galería, dale play y tocá el marcador de nuevo');return;}"
        "var u=" + _js_str(base) + "+'?video_m3u8='+encodeURIComponent(l.join(','));"
        "var w=window.open(u,'_blank');if(!w){location.href=u;}"
        "})();"
    )
    return "javascript:" + js


def _js_str(s: str) -> str:
    return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"


def fmt_tam(n: int) -> str:
    return f"{n / 1048576:.1f} MB" if n else "—"


def fmt_dur(seg: float) -> str:
    return f"{seg:.1f} s" if seg < 120 else f"{int(seg // 60)}:{int(seg % 60):02d}"
