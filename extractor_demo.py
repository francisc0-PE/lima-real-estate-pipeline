"""
extractor_demo.py
=================
Extractor sintetico para ejecucion local y pruebas de arquitectura (sin red).

Proposito en el repositorio publico
-----------------------------------
Reemplaza a los conectores especificos de portales comerciales permitiendo
ejecutar el pipeline de extremo a extremo (``python main.py --modo local --demo``)
con costo $0 y sin realizar peticiones a sitios externos.

Cumple el contrato de modulos del orquestador:
    extraer(opt, limite=None) -> list[dict]
    vincular_empresa(aviso, empresas) -> dict | None
    fusionar_empresa(aviso, empresa) -> dict
"""

from __future__ import annotations

import re
import unicodedata
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence
from urllib.parse import urlsplit

from logger import obtener_logger
from optimizador import Optimizador

__all__ = [
    "extraer",
    "extraer_sunat",
    "vincular_empresa",
    "fusionar_empresa",
]

log = obtener_logger("extractor_demo")

CAMPOS_EMPRESA = (
    "ruc",
    "razon_social",
    "estado_sunat",
    "actividad",
    "distrito_fiscal",
    "correo_corporativo",
    "web",
    "condicion",
    "ciiu",
)

_VENDEDORES_GENERICOS = {
    "duenodirecto",
    "propietariodirecto",
    "particular",
    "sincomision",
    "propietario",
    "agente",
}

_FORMA_SOCIETARIA = re.compile(
    r"\b(s\.?\s?a\.?\s?c\.?|s\.?\s?a\.?|s\.?\s?r\.?\s?l\.?|e\.?\s?i\.?\s?r\.?\s?l\.?|sac|sa|srl|eirl)\s*$",
    re.IGNORECASE,
)


# =========================================================================== #
#  Utilidades de normalizacion y cruce (GRUPO 1 <-> GRUPO 2)
# =========================================================================== #
def _normalizar(valor: Any) -> str:
    """Convierte un texto a clave alfanumerica en minusculas sin acentos."""
    if valor is None:
        return ""
    texto = unicodedata.normalize("NFKD", str(valor))
    texto = "".join(c for c in texto if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", "", texto.lower())


def _dominio(url: Any) -> str:
    """Extrae el dominio base de una URL o correo."""
    if not url:
        return ""
    texto = str(url).strip().lower()
    if "@" in texto and "://" not in texto:
        texto = texto.split("@", 1)[-1]
    elif "://" not in texto:
        texto = "http://" + texto
    try:
        host = urlsplit(texto).hostname or ""
    except ValueError:
        return ""
    return host[4:] if host.startswith("www.") else host


def _tokens_nucleo(nombre: Any) -> list:
    """Palabras del nombre sin acentos ni forma societaria final (S.A.C., E.I.R.L., ...)."""
    if nombre is None:
        return []
    texto = unicodedata.normalize("NFKD", str(nombre))
    texto = "".join(c for c in texto if not unicodedata.combining(c)).lower().strip()
    texto = _FORMA_SOCIETARIA.sub("", texto).strip()
    return re.findall(r"[a-z0-9]+", texto)


def vincular_empresa(
    aviso: Dict[str, Any],
    empresas: Sequence[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Asocia un aviso inmobiliario (GRUPO 2) con su empresa formal (GRUPO 1).

    Orden de prioridad:
      1. RUC explicito de 11 digitos.
      2. Dominio web o de correo corporativo.
      3. Nombre comercial / razon social normalizada.
    """
    if not isinstance(aviso, dict) or not empresas:
        return None

    # 1) Coincidencia exacta por RUC
    ruc_aviso = _normalizar(aviso.get("ruc"))
    if len(ruc_aviso) == 11:
        for empresa in empresas:
            if _normalizar(empresa.get("ruc")) == ruc_aviso:
                return empresa

    # 2) Coincidencia por dominio web
    dom_aviso = _dominio(aviso.get("web") or aviso.get("correo_corporativo"))
    if dom_aviso:
        for empresa in empresas:
            dom_emp = _dominio(empresa.get("web") or empresa.get("correo_corporativo"))
            if dom_emp and dom_aviso == dom_emp:
                return empresa

    # 3) Coincidencia por razon social / nombre de vendedor
    #    - Igualdad del nombre sin forma societaria, o
    #    - contencion solo si el nombre mas corto tiene 2+ palabras
    #      (evita que "Inversiones" vincule con cualquier "Inversiones X").
    vendedor = _normalizar(aviso.get("vendedor") or aviso.get("razon_social"))
    if not vendedor or vendedor in _VENDEDORES_GENERICOS:
        return None

    tok_v = _tokens_nucleo(aviso.get("vendedor") or aviso.get("razon_social"))
    if not tok_v:
        return None

    for empresa in empresas:
        tok_e = _tokens_nucleo(empresa.get("razon_social"))
        if not tok_e:
            continue
        if tok_v == tok_e:
            return empresa
        corto, largo = sorted((tok_v, tok_e), key=len)
        if len(corto) >= 2 and " ".join(corto) in " ".join(largo):
            return empresa

    return None


def fusionar_empresa(aviso: Dict[str, Any], empresa: Dict[str, Any]) -> Dict[str, Any]:
    """Combina los datos fiscales de la empresa (GRUPO 1) dentro del aviso (GRUPO 2)."""
    combinado = dict(aviso)
    if not isinstance(empresa, dict):
        combinado["empresa_vinculada"] = False
        return combinado

    for campo in CAMPOS_EMPRESA:
        valor_empresa = empresa.get(campo)
        if valor_empresa not in (None, "") and combinado.get(campo) in (None, ""):
            combinado[campo] = valor_empresa

    combinado["empresa_vinculada"] = True
    return combinado


# =========================================================================== #
#  Generadores de datos sinteticos (Contrato del Orquestador)
# =========================================================================== #
def _hace_dias(dias: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=dias)).isoformat(timespec="seconds")


def extraer_sunat(opt: Optional[Optimizador] = None, limite: Optional[int] = None) -> List[Dict[str, Any]]:
    """Devuelve empresas sinteticas (GRUPO 1) para pruebas offline."""
    empresas: List[Dict[str, Any]] = [
        {
            "ruc": "20512345671",
            "razon_social": "INVERSIONES LIMA NORTE S.A.C.",
            "estado_sunat": "ACTIVO",
            "condicion": "HABIDO",
            "actividad": "ACTIVIDADES INMOBILIARIAS REALIZADAS CON BIENES PROPIOS",
            "ciiu": "6810",
            "distrito_fiscal": "San Isidro",
            "correo_corporativo": "contacto@limanorte.pe",
            "web": "https://www.limanorte.pe",
        },
        {
            "ruc": "20487654320",
            "razon_social": "CONSTRUCTORA ANDINA E.I.R.L.",
            "estado_sunat": "ACTIVO",
            "condicion": "HABIDO",
            "actividad": "CONSTRUCCION DE EDIFICIOS COMPLETOS",
            "ciiu": "4100",
            "distrito_fiscal": "Callao",
            "correo_corporativo": "proyectos@andina-construye.com",
            "web": "https://andina-construye.com",
        },
        {
            "ruc": "20601234565",
            "razon_social": "GRUPO MIRAFLORES PROPIEDADES S.A.C.",
            "estado_sunat": "ACTIVO",
            "condicion": "HABIDO",
            "actividad": "ACTIVIDADES INMOBILIARIAS",
            "ciiu": "6820",
            "distrito_fiscal": "Miraflores",
            "correo_corporativo": "ventas@grupomiraflores.pe",
            "web": "https://www.grupomiraflores.pe",
        },
    ]
    if limite is not None and limite > 0:
        empresas = empresas[: int(limite)]
    log.info("Extractor demo (GRUPO 1): %d empresas sinteticas generadas.", len(empresas))
    return empresas


def extraer(opt: Optional[Optimizador] = None, limite: Optional[int] = None) -> List[Dict[str, Any]]:
    """Devuelve avisos inmobiliarios sinteticos (GRUPO 2) cumpliendo el contrato de main.py."""
    avisos: List[Dict[str, Any]] = [
        {
            "tipo_inmueble": "Departamento",
            "zona": "San Isidro, Lima",
            "m2": 120.0,
            "precio_usd": 185_000.0,
            "vendedor": "INVERSIONES LIMA NORTE S.A.C.",
            "enlace_aviso": "https://portal-ejemplo.pe/aviso/123456",
            "ruc": "20512345671",
            "fecha_publicacion": _hace_dias(25),
            "portal": "demo",
        },
        {
            "tipo_inmueble": "Casa",
            "zona": "Miraflores, Lima",
            "m2": 210.5,
            "precio_usd": 320_000.0,
            "vendedor": "INVERSIONES LIMA NORTE S.A.C.",
            "enlace_aviso": "https://portal-ejemplo.pe/aviso/987654",
            "ruc": "20512345671",
            "fecha_publicacion": _hace_dias(12),
            "portal": "demo",
        },
        {
            "tipo_inmueble": "Departamento",
            "zona": "La Molina, Lima",
            "m2": 150.0,
            "precio_usd": 240_000.0,
            "vendedor": "GRUPO MIRAFLORES PROPIEDADES S.A.C.",
            "enlace_aviso": "https://portal-ejemplo.pe/aviso/222111",
            "ruc": "20601234565",
            "fecha_publicacion": _hace_dias(5),
            "portal": "demo",
        },
        {
            "tipo_inmueble": "Departamento",
            "zona": "Bellavista, Callao",
            "m2": 95.0,
            "precio_usd": 128_000.0,
            "vendedor": "CONSTRUCTORA ANDINA E.I.R.L.",
            "enlace_aviso": "https://portal-ejemplo.pe/aviso/444333",
            "ruc": "20487654320",
            "fecha_publicacion": _hace_dias(18),
            "portal": "demo",
        },
        {
            "tipo_inmueble": "Terreno",
            "zona": "Cieneguilla, Lima",
            "m2": 1_000.0,
            "precio_usd": 95_000.0,
            "vendedor": "CONSTRUCTORA ANDINA E.I.R.L.",
            "enlace_aviso": "https://portal-ejemplo.pe/aviso/555111",
            "ruc": "20487654320",
            "fecha_publicacion": _hace_dias(40),
            "portal": "demo",
        },
        {
            "tipo_inmueble": "Local comercial",
            "zona": "Surquillo, Lima",
            "m2": 80.0,
            "precio_usd": 210_000.0,
            "vendedor": "CONSTRUCTORA ANDINA E.I.R.L.",
            "enlace_aviso": "https://portal-ejemplo.pe/aviso/111000",
            "ruc": "20487654320",
            "fecha_publicacion": _hace_dias(3),
            "portal": "demo",
        },
    ]
    if limite is not None and limite > 0:
        avisos = avisos[: int(limite)]
    log.info("Extractor demo (GRUPO 2): %d avisos sinteticos generados.", len(avisos))
    return avisos
