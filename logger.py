"""
logger.py
=========
Logging unificado y **consola limpia** para el pipeline.

Filosofia (produccion)
----------------------
* La **consola** es para el operador: barras de progreso, avisos de exito y el
  dashboard final. Nada de ruido tecnico. Por eso el handler de consola filtra a
  ``LOG_CONSOLE_LEVEL`` (por defecto ``ERROR``): solo se ve algo si de verdad hay
  un problema grave.
* El **archivo** es para el ingeniero: ``LOG_FILE_LEVEL`` (por defecto ``DEBUG``)
  con TODO el detalle (peticiones HTTP, proxies, reintentos, enrutador...).
* Se neutralizan los handlers propios de librerias de terceros (scrapling,
  playwright, urllib3...) para que su salida **no** se cuele por pantalla: sus
  registros se propagan al logger raiz y terminan en el archivo.

Uso
---
    from logger import configurar_logging, obtener_logger

    configurar_logging(nivel="INFO", carpeta="logs",
                       consola_nivel="ERROR", archivo_nivel="DEBUG")
    log = obtener_logger("extractor_sunat")
    log.info("Esto va SOLO al archivo")        # invisible en pantalla
    log.error("Esto SI se ve en pantalla")
"""

from __future__ import annotations

import logging
import logging.handlers
import sys
from pathlib import Path
from typing import Optional

_FORMATO = "%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s"
_FORMATO_FECHA = "%Y-%m-%d %H:%M:%S"

# Librerias que suelen traer su PROPIO handler (y por tanto escriben en pantalla
# saltandose nuestro filtro). Se les quita el handler y se fuerza la propagacion.
_LIBRERIAS_CON_HANDLER = (
    "scrapling", "playwright", "urllib3", "requests", "curl_cffi",
    "asyncio", "charset_normalizer", "httpx", "httpcore",
)

_configurado = False
_consola_compartida = None


def obtener_consola_rich():
    """Consola ``rich`` compartida por logs y interfaz (evita intercalados).

    Devuelve ``None`` si rich no esta instalado.
    """
    global _consola_compartida
    if _consola_compartida is None:
        try:
            from rich.console import Console

            _consola_compartida = Console(highlight=False, soft_wrap=False)
        except Exception:
            return None
    return _consola_compartida


def _rich_disponible() -> bool:
    try:
        import rich.logging  # noqa: F401

        return True
    except Exception:
        return False


def _neutralizar_librerias() -> None:
    """Quita handlers de terceros y deja que todo se propague al logger raiz.

    Sin esto, ``scrapling`` imprime sus "Fetched (200)" directamente en pantalla
    aunque nuestro handler de consola este en ERROR.
    """
    for nombre in _LIBRERIAS_CON_HANDLER:
        logger = logging.getLogger(nombre)
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            try:
                handler.close()
            except Exception:
                pass
        logger.propagate = True
        logger.setLevel(logging.DEBUG)  # el filtrado se hace en los handlers


def configurar_logging(
    nivel: str = "INFO",
    carpeta: Optional[str | Path] = None,
    archivo: str = "pipeline.log",
    consola: bool = True,
    rotacion_mb: int = 10,
    respaldos: int = 5,
    forzar: bool = False,
    usar_rich: bool = True,
    consola_nivel: str = "ERROR",
    archivo_nivel: str = "DEBUG",
) -> logging.Logger:
    """Configura el logger raiz del pipeline.

    Es seguro llamarlo varias veces: solo la primera (o con ``forzar=True``)
    tiene efecto, por lo que cada modulo puede invocarlo sin duplicar handlers.
    """
    global _configurado
    raiz = logging.getLogger()

    if _configurado and not forzar:
        return raiz

    nivel_num = getattr(logging, str(nivel).upper(), logging.INFO)
    nivel_consola = getattr(logging, str(consola_nivel).upper(), logging.ERROR)
    nivel_archivo = getattr(logging, str(archivo_nivel).upper(), logging.DEBUG)

    # El logger RAIZ debe ser lo mas permisivo de los tres: cada handler filtra
    # despues. Si no, poner la consola en ERROR silenciaria el archivo tambien.
    permisivos = [nivel_num]
    if consola:
        permisivos.append(nivel_consola)
    if carpeta:
        permisivos.append(nivel_archivo)
    raiz.setLevel(min(permisivos))

    for handler in list(raiz.handlers):
        raiz.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass

    formateador = logging.Formatter(_FORMATO, datefmt=_FORMATO_FECHA)

    if consola:
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
        except Exception:
            pass
        nivel_consola = getattr(logging, str(consola_nivel).upper(), logging.ERROR)
        handler_consola: logging.Handler
        if usar_rich and _rich_disponible():
            try:
                from rich.logging import RichHandler

                handler_consola = RichHandler(
                    console=obtener_consola_rich(),
                    level=nivel_consola,
                    show_path=False,
                    rich_tracebacks=True,
                    markup=False,
                    log_time_format="%H:%M:%S",
                )
                handler_consola.setFormatter(logging.Formatter("%(message)s"))
            except Exception:
                handler_consola = logging.StreamHandler(stream=sys.stdout)
                handler_consola.setFormatter(formateador)
        else:
            handler_consola = logging.StreamHandler(stream=sys.stdout)
            handler_consola.setFormatter(formateador)
        handler_consola.setLevel(nivel_consola)
        raiz.addHandler(handler_consola)

    if carpeta:
        try:
            ruta = Path(carpeta)
            ruta.mkdir(parents=True, exist_ok=True)
            handler_archivo = logging.handlers.RotatingFileHandler(
                ruta / archivo,
                maxBytes=max(1, int(rotacion_mb)) * 1024 * 1024,
                backupCount=max(0, int(respaldos)),
                encoding="utf-8",
            )
            handler_archivo.setFormatter(formateador)
            handler_archivo.setLevel(getattr(logging, str(archivo_nivel).upper(), logging.DEBUG))
            raiz.addHandler(handler_archivo)
        except Exception:
            # Sin permisos de escritura: se sigue logueando por consola.
            pass

    _neutralizar_librerias()
    _configurado = True
    return raiz


def obtener_logger(nombre: str, nivel: Optional[str] = None) -> logging.Logger:
    """Devuelve un logger con nombre; auto-configura un fallback si hace falta."""
    if not _configurado:
        configurar_logging()
    logger = logging.getLogger(nombre)
    if nivel:
        logger.setLevel(getattr(logging, str(nivel).upper(), logging.INFO))
    return logger


def silenciar(nombre: str) -> None:
    """Sube el nivel de una libreria molesta (solo si de verdad molesta)."""
    logging.getLogger(nombre).setLevel(logging.WARNING)


__all__ = ["configurar_logging", "obtener_logger", "silenciar", "obtener_consola_rich"]
