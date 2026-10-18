"""
optimizador.py
==============
Nucleo de ahorro de recursos y resiliencia de red del pipeline B2B inmobiliario.

Responsabilidades (y solo estas)
--------------------------------
1. :class:`RotadorProxies`  -> rotacion, cuarentena y salud del pool de proxies.
2. :class:`GestorCache`     -> cache persistente con TTL por tipo de dato.
                              Evita pagar dos veces por el mismo RUC/empresa.
3. :class:`ClienteSerper`   -> unico punto de salida hacia la Serper API, con
                              rate-limit, backoff exponencial y circuit breaker.
4. :class:`Optimizador`     -> fachada que compone las piezas anteriores y que
                              los extractores consumen en una linea.

Contrato para los extractores
-----------------------------
    from optimizador import get_optimizador, Namespace

    opt = get_optimizador()                       # singleton, thread-safe
    if opt.empresa_consultada(ruc):               # no gastamos ni un credito
        datos = opt.obtener_empresa(ruc)
    else:
        datos = opt.serper.buscar(f'"{razon_social}" {distrito} contacto')
        opt.guardar_empresa(ruc, datos)

    # Descarga de paginas con proxy rotado + cache de HTML
    html = opt.descargar_html(url)                # str | None, nunca lanza

Notas de diseno
---------------
* Ningun metodo publico propaga excepciones de red: devuelven ``None``/``False``
  y dejan el motivo en los logs. La extraccion nunca se cae por un proxy inactivo.
* Compatible con Scrapling 0.4.x: ``proxy`` acepta ``str`` o
  ``dict {"server", "username", "password"}``.
"""

from __future__ import annotations

import abc
import hashlib
import json
import logging
import os
import random
import re
import sqlite3
import threading
import time
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, TypeVar
from urllib.parse import unquote, urlsplit

from config import Settings, get_settings, mascarar
from logger import obtener_logger

try:
    import requests  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    requests = None  # type: ignore[assignment]

T = TypeVar("T")

__all__ = [
    "Namespace",
    "Optimizador",
    "RotadorProxies",
    "GestorCache",
    "ClienteSerper",
    "EstadoProxy",
    "get_optimizador",
    "OptimizadorError",
]


# =========================================================================== #
#  Excepciones del dominio
# =========================================================================== #
class OptimizadorError(Exception):
    """Error base del optimizador."""


class ErrorSerper(OptimizadorError):
    """Fallo al comunicarse con la Serper API."""


class ErrorCache(OptimizadorError):
    """Fallo del backend de cache."""


class ErrorProxy(OptimizadorError):
    """Fallo relacionado con el pool de proxies."""


# =========================================================================== #
#  Namespaces de cache (evitan colisiones y permiten TTL por tipo de dato)
# =========================================================================== #
class Namespace:
    """Espacios de nombres de la cache. Un TTL distinto para cada uno."""

    SERPER = "serper"        # respuestas crudas de busquedas web
    SUNAT = "sunat"          # GRUPO 1: datos fiscales por RUC
    PORTALES = "portales"    # GRUPO 2: avisos inmobiliarios
    WEB = "web"              # HTML de paginas corporativas
    EMPRESA = "empresa"      # ficha consolidada por RUC
    META = "meta"            # contadores persistentes (telemetria, sin expiracion)


_AUSENTE = object()


# =========================================================================== #
#  Utilidades
# =========================================================================== #
def normalizar_texto(valor: Any) -> str:
    """Normaliza texto para usarlo como clave: sin acentos, minusculas, sin ruido.

    >>> normalizar_texto("  ACMÉ  Inversiones S.A.C. ")
    'acmeinversionessac'
    """
    if valor is None:
        return ""
    texto = unicodedata.normalize("NFKD", str(valor))
    texto = "".join(caracter for caracter in texto if not unicodedata.combining(caracter))
    return re.sub(r"[^a-z0-9]+", "", texto.lower())


def clave_estable(*partes: Any) -> str:
    """Clave de cache estable, legible y con longitud acotada."""
    base = "::".join(normalizar_texto(parte) for parte in partes if parte is not None)
    if len(base) <= 180:
        return base or "vacio"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


# =========================================================================== #
#  1) LIMITADOR DE TASA
# =========================================================================== #
class LimitadorTasa:
    """Limita a `rps` peticiones por segundo (thread-safe, sin dependencias)."""

    def __init__(self, rps: float):
        self.rps = max(0.0, float(rps))
        self._intervalo = (1.0 / self.rps) if self.rps > 0 else 0.0
        self._lock = threading.Lock()
        self._siguiente = 0.0

    def esperar(self) -> float:
        """Bloquea lo necesario. Devuelve los segundos efectivamente esperados."""
        if self._intervalo <= 0:
            return 0.0
        with self._lock:
            ahora = time.monotonic()
            espera = max(0.0, self._siguiente - ahora)
            self._siguiente = max(ahora, self._siguiente) + self._intervalo
        if espera > 0:
            time.sleep(espera)
        return espera


# =========================================================================== #
#  2) ROTADOR DE PROXIES
# =========================================================================== #
@dataclass
class EstadoProxy:
    """Estado individual de un proxy dentro del pool."""

    url: str
    server: str
    username: Optional[str] = None
    password: Optional[str] = None
    usos: int = 0
    exitos: int = 0
    fallos: int = 0
    racha_fallos: int = 0
    ultimo_uso: float = 0.0
    bloqueado_hasta: float = 0.0

    @property
    def disponible(self) -> bool:
        return time.monotonic() >= self.bloqueado_hasta

    @property
    def publico(self) -> str:
        """Representacion sin credenciales, apta para logs."""
        return self.server if (self.username or self.password) else self.url

    def como_requests(self) -> Dict[str, str]:
        """Formato para `requests` / `Fetcher.get(..., proxies=...)`."""
        return {"http": self.url, "https": self.url}

    def como_playwright(self) -> Dict[str, str]:
        """Formato para Scrapling: `Fetcher.get(..., proxy={...})`."""
        detalle: Dict[str, str] = {"server": self.server}
        if self.username:
            detalle["username"] = self.username
        if self.password:
            detalle["password"] = self.password
        return detalle

    def como_scrapling(self) -> Any:
        """Proxy para fetchers basados en NAVEGADOR (dict con credenciales).

        Para la clase ``Fetcher`` (curl_cffi) use :attr:`url` directamente:
        curl_cffi no acepta diccionarios.
        """
        return self.como_playwright() if (self.username or self.password) else self.url


def _parsear_proxy(crudo: str) -> Optional[EstadoProxy]:
    """Convierte ``user:pass@host:port`` en un :class:`EstadoProxy`."""
    crudo = (crudo or "").strip()
    if not crudo:
        return None
    if "://" not in crudo:
        crudo = f"http://{crudo}"
    partes = urlsplit(crudo)
    if not partes.hostname:
        return None
    server = f"{partes.scheme}://{partes.hostname}"
    if partes.port:
        server = f"{server}:{partes.port}"
    return EstadoProxy(
        url=crudo,
        server=server,
        username=unquote(partes.username) if partes.username else None,
        password=unquote(partes.password) if partes.password else None,
    )


class RotadorProxies:
    """Pool de proxies con rotacion, cuarentena y metricas.

    Estrategias: ``round_robin`` (por defecto), ``random``, ``least_used``.

    Comportamiento fail-open: si todos los proxies estan en cuarentena se
    reutiliza el que antes se libera, porque quedarse sin red cuesta mas caro
    que arriesgar un request con un proxy degradado.
    """

    ESTRATEGIAS = {"round_robin", "random", "least_used", "menos_usado"}

    def __init__(
        self,
        proxies: Sequence[str] = (),
        estrategia: str = "round_robin",
        max_fallos: int = 3,
        cooldown: float = 300.0,
        logger: Optional[logging.Logger] = None,
    ):
        self._logger = logger or obtener_logger("optimizador.proxies")
        self._lock = threading.RLock()
        self.estrategia = estrategia if estrategia in self.ESTRATEGIAS else "round_robin"
        self.max_fallos = max(1, int(max_fallos))
        self.cooldown = max(0.0, float(cooldown))
        self._estados: List[EstadoProxy] = [
            estado for estado in (_parsear_proxy(p) for p in proxies or ()) if estado
        ]
        self._indice = 0

        if self._estados:
            self._logger.info(
                "Pool de proxies listo: %d proxies (estrategia=%s).",
                len(self._estados),
                self.estrategia,
            )
        else:
            self._logger.info("Sin proxies configurados: modo conexion directa.")

    # ---------------------------- API publica ---------------------------- #
    @property
    def hay_proxies(self) -> bool:
        return bool(self._estados)

    def __len__(self) -> int:
        return len(self._estados)

    def disponibles(self) -> List[EstadoProxy]:
        with self._lock:
            self._liberar_cuarentenas()
            return [estado for estado in self._estados if estado.disponible]

    def obtener(self) -> Optional[EstadoProxy]:
        """Entrega el siguiente proxy segun la estrategia. ``None`` = conexion directa."""
        with self._lock:
            if not self._estados:
                return None
            self._liberar_cuarentenas()
            candidatos = [estado for estado in self._estados if estado.disponible]
            if candidatos:
                elegido = self._elegir(candidatos)
            else:
                elegido = min(self._estados, key=lambda estado: estado.bloqueado_hasta)
                self._logger.warning(
                    "Todos los proxies en cuarentena; se reutiliza %s (fail-open).", elegido.publico
                )
            elegido.usos += 1
            elegido.ultimo_uso = time.monotonic()
            return elegido

    def reportar_exito(self, proxy: Optional[Any]) -> None:
        estado = self._resolver(proxy)
        if estado is None:
            return
        with self._lock:
            estado.exitos += 1
            estado.racha_fallos = 0

    def reportar_fallo(self, proxy: Optional[Any]) -> None:
        estado = self._resolver(proxy)
        if estado is None:
            return
        with self._lock:
            estado.fallos += 1
            estado.racha_fallos += 1
            if estado.racha_fallos >= self.max_fallos:
                estado.bloqueado_hasta = time.monotonic() + self.cooldown
                self._logger.warning(
                    "Proxy %s a cuarentena por %.0fs (%d fallos consecutivos).",
                    estado.publico,
                    self.cooldown,
                    estado.racha_fallos,
                )

    def conexion_requests(self) -> Optional[Dict[str, str]]:
        """Devuelve ``proxies=`` para requests, o ``None`` si no hay pool."""
        estado = self.obtener()
        return estado.como_requests() if estado else None

    def healthcheck(self, timeout: float = 8.0, url: str = "https://api.ipify.org?format=json") -> int:
        """Verifica que proxy responde. Devuelve la cantidad de proxies sanos."""
        if requests is None or not self._estados:
            return len(self._estados)
        sanos = 0
        for estado in list(self._estados):
            try:
                respuesta = requests.get(url, proxies=estado.como_requests(), timeout=timeout)
                if respuesta.ok:
                    sanos += 1
                    self.reportar_exito(estado)
                else:
                    self.reportar_fallo(estado)
            except Exception:
                self.reportar_fallo(estado)
        self._logger.info("Healthcheck: %d/%d proxies operativos.", sanos, len(self._estados))
        return sanos

    def estadisticas(self) -> Dict[str, Any]:
        with self._lock:
            self._liberar_cuarentenas()
            return {
                "total": len(self._estados),
                "disponibles": sum(1 for e in self._estados if e.disponible),
                "en_cuarentena": sum(1 for e in self._estados if not e.disponible),
                "estrategia": self.estrategia,
                "detalle": [
                    {
                        "proxy": e.publico,
                        "usos": e.usos,
                        "exitos": e.exitos,
                        "fallos": e.fallos,
                        "disponible": e.disponible,
                    }
                    for e in self._estados
                ],
            }

    # ----------------------------- Internos ------------------------------ #
    def _liberar_cuarentenas(self) -> None:
        ahora = time.monotonic()
        for estado in self._estados:
            if estado.bloqueado_hasta and ahora >= estado.bloqueado_hasta:
                estado.bloqueado_hasta = 0.0
                estado.racha_fallos = 0

    def _elegir(self, candidatos: Sequence[EstadoProxy]) -> EstadoProxy:
        if self.estrategia == "random":
            return random.choice(list(candidatos))
        if self.estrategia in {"least_used", "menos_usado"}:
            return min(candidatos, key=lambda estado: (estado.usos, estado.racha_fallos))
        elegido = candidatos[self._indice % len(candidatos)]
        self._indice += 1
        return elegido

    def _resolver(self, proxy: Optional[Any]) -> Optional[EstadoProxy]:
        if proxy is None:
            return None
        if isinstance(proxy, EstadoProxy):
            return proxy
        texto = str(proxy)
        for estado in self._estados:
            if texto in (estado.url, estado.server):
                return estado
        return None


# =========================================================================== #
#  2.bis) ENRUTADOR DE MOTORES POR DOMINIO
# =========================================================================== #
class EnrutadorMotores:
    """Elige el motor de descarga segun el dominio, con auto-escalado.

    Problema que resuelve: un mismo pipeline debe procesar portales que
    responden a HTTP puro (barato y rapido) y otros que requieren navegador
    porque renderizan el listado dinamicamente con JavaScript.

    Politica (``SCRAPER_MOTORES``)::

        urbania.pe|fetcher;adondevivir.com|dynamic

    * Si el dominio tiene politica, se usa **solo para ese dominio**: la
      variable global ``SCRAPER_FETCHER`` se ignora para el.
    * Si el motor no devuelve contenido util (respuesta incompleta o timeout), se
      **auto-escala** al siguiente de la cadena: fetcher -> dynamic.
    * **Aprende**: el motor que funciono con un dominio se prueba primero la
      proxima vez (en memoria y persistido en la cache).
    """

    ORDEN_ESCALADA = ("fetcher", "dynamic")

    def __init__(
        self,
        politicas: Optional[Dict[str, Sequence[str]]] = None,
        por_defecto: str = "fetcher",
        autoescalar: bool = True,
        memoria: Optional[GestorCache] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self._logger = logger or obtener_logger("optimizador.enrutador")
        self.politicas: Dict[str, Tuple[str, ...]] = {
            self.normalizar_dominio(dominio): tuple(
                motor for motor in (cadena or ()) if motor in self.ORDEN_ESCALADA
            )
            for dominio, cadena in (politicas or {}).items()
        }
        self.politicas = {dominio: cadena for dominio, cadena in self.politicas.items() if cadena}
        self.por_defecto = por_defecto if por_defecto in self.ORDEN_ESCALADA else "fetcher"
        self.autoescalar = bool(autoescalar)
        self.memoria = memoria
        self._lock = threading.RLock()
        self._aprendido: Dict[str, str] = {}
        self.metricas: Dict[str, int] = {"descargas": 0, "escalados": 0, "fallos": 0, "aciertos_directos": 0}
        if self.politicas:
            self._logger.info(
                "Enrutador de motores: %s",
                "; ".join(f"{dominio} -> {'>'.join(cadena)}" for dominio, cadena in sorted(self.politicas.items())),
            )

    # ------------------------------------------------------------------ #
    @staticmethod
    def normalizar_dominio(url_o_dominio: Any) -> str:
        """Dominio normalizado ('www.' fuera, minusculas)."""
        texto = str(url_o_dominio or "").strip().lower()
        if not texto:
            return ""
        if "://" in texto:
            try:
                texto = urlsplit(texto).hostname or ""
            except ValueError:
                texto = ""
        if texto.startswith("www."):
            texto = texto[4:]
        return texto.strip(".")

    def dominio(self, url: Any) -> str:
        return self.normalizar_dominio(url)

    def politica(self, url: Any) -> Optional[Tuple[str, ...]]:
        """Cadena configurada para el dominio (None si no tiene politica propia)."""
        dominio = self.dominio(url)
        if not dominio:
            return None
        if dominio in self.politicas:
            return self.politicas[dominio]
        # Subdominios heredan la politica del dominio raiz (proyectos.urbania.pe).
        for conocido, cadena in self.politicas.items():
            if dominio.endswith("." + conocido):
                return cadena
        return None

    def cadena(self, url: Any, motor_forzado: Optional[str] = None) -> List[str]:
        """Motores a intentar, en orden, para una URL.

        Prioridad: motor forzado por el llamador > politica del dominio >
        global por defecto. Con ``autoescalar`` se anaden los motores restantes.
        """
        if motor_forzado:
            return [motor_forzado.strip().lower()]
        dominio = self.dominio(url)
        politica = self.politica(url)
        if politica:
            cadena = list(politica)
        else:
            cadena = [self.por_defecto]
            if self.autoescalar:
                for motor in self.ORDEN_ESCALADA:
                    if motor not in cadena:
                        cadena.append(motor)
        aprendido = self._leer_aprendido(dominio)
        if aprendido and aprendido in cadena:
            cadena.remove(aprendido)
            cadena.insert(0, aprendido)
        return cadena

    # --------------------------- Aprendizaje --------------------------- #
    def motor_aprendido(self, url: Any) -> Optional[str]:
        return self._leer_aprendido(self.dominio(url))

    def registrar_exito(self, url: Any, motor: str) -> None:
        dominio = self.dominio(url)
        with self._lock:
            self.metricas["descargas"] += 1
            if not self._aprendido.get(dominio):
                self.metricas["aciertos_directos"] += 1
            self._aprendido[dominio] = motor
        if self.memoria is not None and dominio:
            try:
                self.memoria.guardar(Namespace.META, f"motor:{dominio}", motor, ttl_dias=0)
            except Exception as error:
                self._logger.debug("No se pudo persistir el motor de %s: %s", dominio, error)

    def registrar_fallo(self, url: Any, motor: str) -> None:
        with self._lock:
            self.metricas["fallos"] += 1
            dominio = self.dominio(url)
            if self._aprendido.get(dominio) == motor:
                self._aprendido.pop(dominio, None)
                if self.memoria is not None:
                    try:
                        self.memoria.eliminar(Namespace.META, f"motor:{dominio}")
                    except Exception:
                        pass

    def _leer_aprendido(self, dominio: str) -> Optional[str]:
        if not dominio:
            return None
        with self._lock:
            aprendido = self._aprendido.get(dominio)
        if aprendido:
            return aprendido
        if self.memoria is not None:
            try:
                guardado = self.memoria.obtener(Namespace.META, f"motor:{dominio}", None)
            except Exception:
                guardado = None
            if isinstance(guardado, str) and guardado in self.ORDEN_ESCALADA:
                with self._lock:
                    self._aprendido[dominio] = guardado
                return guardado
        return None

    def estadisticas(self) -> Dict[str, Any]:
        with self._lock:
            for dominio in list(self.politicas):
                self._leer_aprendido(dominio)
            return {
                "politicas": {dominio: list(cadena) for dominio, cadena in sorted(self.politicas.items())},
                "por_defecto": self.por_defecto,
                "autoescalar": self.autoescalar,
                "motores_aprendidos": dict(sorted(self._aprendido.items())),
                **self.metricas,
            }


# =========================================================================== #
#  3) CACHE PERSISTENTE
# =========================================================================== #
class _BackendCache(abc.ABC):
    """Contrato minimo que debe cumplir todo backend de cache."""

    nombre = "base"

    @abc.abstractmethod
    def obtener(self, namespace: str, clave: str, ahora: float) -> Any:
        """Devuelve el valor o ``_AUSENTE`` (distinto de un valor ``None``)."""

    @abc.abstractmethod
    def guardar(self, namespace: str, clave: str, valor: Any, creado: float, expira: Optional[float]) -> None: ...

    @abc.abstractmethod
    def existe(self, namespace: str, clave: str, ahora: float) -> bool: ...

    @abc.abstractmethod
    def eliminar(self, namespace: str, clave: str) -> bool: ...

    @abc.abstractmethod
    def purgar(self, ahora: float) -> int: ...

    @abc.abstractmethod
    def estadisticas(self, ahora: float) -> Dict[str, Any]: ...

    def cerrar(self) -> None:  # pragma: no cover - opcional
        return None


class CacheSQLite(_BackendCache):
    """Backend recomendado: rapido, con WAL y seguro entre hilos."""

    nombre = "sqlite"

    def __init__(self, ruta: Path, logger: Optional[logging.Logger] = None, max_entradas: int = 0):
        self._logger = logger or obtener_logger("optimizador.cache")
        self.ruta = Path(ruta)
        self.ruta.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._max_entradas = max(0, int(max_entradas))
        self._inserciones = 0
        self._conn = sqlite3.connect(str(self.ruta), timeout=30.0, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._crear_esquema()

    def _crear_esquema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=NORMAL;
                CREATE TABLE IF NOT EXISTS cache (
                    namespace TEXT NOT NULL,
                    clave     TEXT NOT NULL,
                    valor     TEXT,
                    creado_en REAL NOT NULL,
                    expira_en REAL,
                    aciertos  INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (namespace, clave)
                );
                CREATE INDEX IF NOT EXISTS ix_cache_expira ON cache (expira_en);
                """
            )
            self._conn.commit()

    # ------------------------------ interno ------------------------------ #
    def _serializar(self, valor: Any) -> str:
        return json.dumps(valor, ensure_ascii=False, default=str)

    def _deserializar(self, crudo: Any) -> Any:
        if crudo is None:
            return None
        try:
            return json.loads(crudo)
        except (TypeError, ValueError):
            return None

    # -------------------------------- API -------------------------------- #
    def obtener(self, namespace: str, clave: str, ahora: float) -> Any:
        with self._lock:
            fila = self._conn.execute(
                "SELECT valor, expira_en FROM cache WHERE namespace = ? AND clave = ?",
                (namespace, clave),
            ).fetchone()
            if fila is None:
                return _AUSENTE
            expira = fila["expira_en"]
            if expira is not None and expira <= ahora:
                self._conn.execute(
                    "DELETE FROM cache WHERE namespace = ? AND clave = ?", (namespace, clave)
                )
                self._conn.commit()
                return _AUSENTE
            self._conn.execute(
                "UPDATE cache SET aciertos = aciertos + 1 WHERE namespace = ? AND clave = ?",
                (namespace, clave),
            )
            self._conn.commit()
            return self._deserializar(fila["valor"])

    def guardar(
        self, namespace: str, clave: str, valor: Any, creado: float, expira: Optional[float]
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO cache (namespace, clave, valor, creado_en, expira_en, aciertos)
                VALUES (?, ?, ?, ?, ?, 0)
                ON CONFLICT(namespace, clave) DO UPDATE SET
                    valor = excluded.valor,
                    creado_en = excluded.creado_en,
                    expira_en = excluded.expira_en
                """,
                (namespace, clave, self._serializar(valor), creado, expira),
            )
            self._conn.commit()
            self._inserciones += 1
            if self._max_entradas and self._inserciones % 100 == 0:
                self._podar()

    def existe(self, namespace: str, clave: str, ahora: float) -> bool:
        with self._lock:
            fila = self._conn.execute(
                "SELECT expira_en FROM cache WHERE namespace = ? AND clave = ?", (namespace, clave)
            ).fetchone()
            if fila is None:
                return False
            expira = fila["expira_en"]
            return expira is None or expira > ahora

    def eliminar(self, namespace: str, clave: str) -> bool:
        with self._lock:
            cursor = self._conn.execute(
                "DELETE FROM cache WHERE namespace = ? AND clave = ?", (namespace, clave)
            )
            self._conn.commit()
            return cursor.rowcount > 0

    def purgar(self, ahora: float) -> int:
        with self._lock:
            cursor = self._conn.execute(
                "DELETE FROM cache WHERE expira_en IS NOT NULL AND expira_en <= ?", (ahora,)
            )
            self._conn.commit()
            return max(0, cursor.rowcount)

    def _podar(self) -> None:
        """Elimina las entradas mas antiguas cuando se supera el limite duro."""
        try:
            self._conn.execute(
                """
                DELETE FROM cache WHERE (namespace, clave) IN (
                    SELECT namespace, clave FROM cache
                    ORDER BY creado_en DESC LIMIT -1 OFFSET ?
                )
                """,
                (self._max_entradas,),
            )
            self._conn.commit()
            self._logger.info("Cache podada al limite de %d entradas.", self._max_entradas)
        except sqlite3.Error as error:
            self._logger.debug("No se pudo podar la cache: %s", error)

    def estadisticas(self, ahora: float) -> Dict[str, Any]:
        with self._lock:
            filas = self._conn.execute(
                """
                SELECT namespace,
                       COUNT(*) AS entradas,
                       SUM(CASE WHEN expira_en IS NULL OR expira_en > ? THEN 1 ELSE 0 END) AS vigentes,
                       SUM(aciertos) AS aciertos
                FROM cache GROUP BY namespace
                """,
                (ahora,),
            ).fetchall()
            por_ns = {
                fila["namespace"]: {
                    "entradas": fila["entradas"] or 0,
                    "vigentes": fila["vigentes"] or 0,
                    "aciertos": fila["aciertos"] or 0,
                }
                for fila in filas
            }
            total = sum(item["entradas"] for item in por_ns.values())
            return {
                "backend": self.nombre,
                "ruta": str(self.ruta),
                "total": total,
                "vigentes": sum(item["vigentes"] for item in por_ns.values()),
                "aciertos": sum(item["aciertos"] for item in por_ns.values()),
                "por_namespace": por_ns,
            }

    def cerrar(self) -> None:
        with self._lock:
            try:
                self._conn.commit()
                self._conn.close()
            except sqlite3.Error:
                pass


class CacheJSON(_BackendCache):
    """Backend alternativo (legible por humanos) para volumenes pequenos."""

    nombre = "json"

    def __init__(self, ruta: Path, logger: Optional[logging.Logger] = None, max_entradas: int = 0):
        self._logger = logger or obtener_logger("optimizador.cache")
        self.ruta = Path(ruta)
        if self.ruta.suffix.lower() != ".json":
            self.ruta = self.ruta.with_suffix(".json")
        self.ruta.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._max_entradas = max(0, int(max_entradas))
        self._datos: Dict[str, Dict[str, Any]] = self._cargar()

    def _cargar(self) -> Dict[str, Dict[str, Any]]:
        if not self.ruta.exists():
            return {}
        try:
            with self.ruta.open("r", encoding="utf-8") as archivo:
                contenido = json.load(archivo)
            entradas = contenido.get("entradas", {})
            return entradas if isinstance(entradas, dict) else {}
        except Exception as error:
            self._logger.warning("Cache JSON ilegible (%s); se reinicia vacio.", error)
            return {}

    def _persistir(self) -> None:
        temporal = self.ruta.with_suffix(".tmp")
        try:
            with temporal.open("w", encoding="utf-8") as archivo:
                json.dump(
                    {"version": 1, "entradas": self._datos},
                    archivo,
                    ensure_ascii=False,
                    default=str,
                )
            os.replace(temporal, self.ruta)
        except OSError as error:
            self._logger.error("No se pudo persistir la cache JSON: %s", error)

    @staticmethod
    def _llave(namespace: str, clave: str) -> str:
        return f"{namespace}::{clave}"

    def obtener(self, namespace: str, clave: str, ahora: float) -> Any:
        with self._lock:
            registro = self._datos.get(self._llave(namespace, clave))
            if registro is None:
                return _AUSENTE
            expira = registro.get("expira_en")
            if expira is not None and expira <= ahora:
                self._datos.pop(self._llave(namespace, clave), None)
                self._persistir()
                return _AUSENTE
            registro["aciertos"] = int(registro.get("aciertos", 0)) + 1
            return registro.get("valor")

    def guardar(
        self, namespace: str, clave: str, valor: Any, creado: float, expira: Optional[float]
    ) -> None:
        with self._lock:
            self._datos[self._llave(namespace, clave)] = {
                "valor": valor,
                "creado_en": creado,
                "expira_en": expira,
                "aciertos": 0,
            }
            if self._max_entradas and len(self._datos) > self._max_entradas:
                self._podar()
            self._persistir()

    def _podar(self) -> None:
        if len(self._datos) <= self._max_entradas:
            return
        ordenado = sorted(self._datos.items(), key=lambda item: item[1].get("creado_en", 0), reverse=True)
        self._datos = dict(ordenado[: self._max_entradas])

    def existe(self, namespace: str, clave: str, ahora: float) -> bool:
        with self._lock:
            registro = self._datos.get(self._llave(namespace, clave))
            if registro is None:
                return False
            expira = registro.get("expira_en")
            return expira is None or expira > ahora

    def eliminar(self, namespace: str, clave: str) -> bool:
        with self._lock:
            eliminado = self._datos.pop(self._llave(namespace, clave), None) is not None
            if eliminado:
                self._persistir()
            return eliminado

    def purgar(self, ahora: float) -> int:
        with self._lock:
            expiradas = [
                llave
                for llave, registro in self._datos.items()
                if registro.get("expira_en") is not None and registro["expira_en"] <= ahora
            ]
            for llave in expiradas:
                self._datos.pop(llave, None)
            if expiradas:
                self._persistir()
            return len(expiradas)

    def estadisticas(self, ahora: float) -> Dict[str, Any]:
        with self._lock:
            por_ns: Dict[str, Dict[str, int]] = {}
            for llave, registro in self._datos.items():
                namespace = llave.split("::", 1)[0]
                expira = registro.get("expira_en")
                vigente = expira is None or expira > ahora
                datos = por_ns.setdefault(namespace, {"entradas": 0, "vigentes": 0, "aciertos": 0})
                datos["entradas"] += 1
                datos["vigentes"] += 1 if vigente else 0
                datos["aciertos"] += int(registro.get("aciertos", 0))
            return {
                "backend": self.nombre,
                "ruta": str(self.ruta),
                "total": len(self._datos),
                "vigentes": sum(item["vigentes"] for item in por_ns.values()),
                "aciertos": sum(item["aciertos"] for item in por_ns.values()),
                "por_namespace": por_ns,
            }


class GestorCache:
    """Fachada de cache con TTL por namespace. El corazon del ahorro.

    Uso tipico::

        if not cache.existe(Namespace.SUNAT, ruc):
            datos = extraer_de_sunat(ruc)
            cache.guardar(Namespace.SUNAT, ruc, datos)
    """

    def __init__(
        self,
        backend: str = "sqlite",
        ruta: Path | str = "./data/cache/pipeline_cache.db",
        ttl_dias: float = 30.0,
        ttl_por_namespace: Optional[Dict[str, float]] = None,
        habilitado: bool = True,
        max_entradas: int = 0,
        logger: Optional[logging.Logger] = None,
    ):
        self._logger = logger or obtener_logger("optimizador.cache")
        self.habilitado = bool(habilitado)
        self.ttl_por_namespace = {k.lower(): float(v) for k, v in (ttl_por_namespace or {}).items()}
        self.ttl_dias = float(ttl_dias)
        self.aciertos = 0
        self.fallos = 0
        self.escrituras = 0

        if not self.habilitado:
            self._logger.warning("Cache DESHABILITADA por configuracion (CACHE_ENABLED=false).")
            self._backend: Optional[_BackendCache] = None
            return

        if backend == "json":
            self._backend = CacheJSON(ruta, logger=self._logger, max_entradas=max_entradas)
        else:
            try:
                self._backend = CacheSQLite(ruta, logger=self._logger, max_entradas=max_entradas)
            except sqlite3.Error as error:
                self._logger.error("SQLite no disponible (%s); se usa cache JSON.", error)
                self._backend = CacheJSON(Path(ruta).with_suffix(".json"), logger=self._logger)

        self._logger.info(
            "Cache %s lista en %s (TTL por namespace: %s).",
            self._backend.nombre,
            self._backend.ruta,
            self.ttl_por_namespace,
        )

    # ---------------------------- TTL helpers ---------------------------- #
    def ttl_segundos(self, namespace: str) -> Optional[float]:
        """Segundos de vida para un namespace. ``None`` = sin expiracion."""
        dias = self.ttl_por_namespace.get(namespace.lower(), self.ttl_dias)
        return None if dias is None or dias <= 0 else dias * 86_400.0

    def clave(self, *partes: Any) -> str:
        return clave_estable(*partes)

    # ------------------------------- API -------------------------------- #
    def obtener(self, namespace: str, clave: Any, por_defecto: Any = None) -> Any:
        """Lee un valor vigente. Devuelve ``por_defecto`` si no existe/expiró."""
        if not self._backend:
            return por_defecto
        try:
            valor = self._backend.obtener(namespace, self.clave(clave), time.time())
        except Exception as error:
            self.fallos += 1
            self._logger.debug("Fallo leyendo cache [%s/%s]: %s", namespace, clave, error)
            return por_defecto
        if valor is _AUSENTE:
            return por_defecto
        self.aciertos += 1
        return valor

    def guardar(self, namespace: str, clave: Any, valor: Any, ttl_dias: Optional[float] = None) -> bool:
        """Persiste un valor no nulo. Devuelve ``True`` si se almaceno."""
        if not self._backend or valor is None:
            return False
        ttl = self.ttl_segundos(namespace) if ttl_dias is None else (
            None if ttl_dias <= 0 else float(ttl_dias) * 86_400.0
        )
        ahora = time.time()
        expira = None if ttl is None else ahora + ttl
        try:
            self._backend.guardar(namespace, self.clave(clave), valor, ahora, expira)
            self.escrituras += 1
            return True
        except Exception as error:
            self.fallos += 1
            self._logger.debug("Fallo escribiendo cache [%s/%s]: %s", namespace, clave, error)
            return False

    def existe(self, namespace: str, clave: Any) -> bool:
        if not self._backend:
            return False
        try:
            return self._backend.existe(namespace, self.clave(clave), time.time())
        except Exception:
            return False

    def recordar(
        self,
        namespace: str,
        clave: Any,
        productor: Callable[[], T],
        ttl_dias: Optional[float] = None,
        por_defecto: Optional[T] = None,
        silenciar_errores: bool = True,
    ) -> Optional[T]:
        """Patron cache-aside blindado: "devuelve lo cacheado o calculalo y guardalo"."""
        if self._backend:
            cacheado = self.obtener(namespace, clave, _AUSENTE)
            if cacheado is not _AUSENTE:
                return cacheado  # type: ignore[return-value]
        try:
            resultado = productor()
        except Exception as error:
            if silenciar_errores:
                self._logger.debug(
                    "Productor fallo [%s/%s]: %s", namespace, clave, error, exc_info=True
                )
                return por_defecto
            raise
        if resultado is not None:
            self.guardar(namespace, clave, resultado, ttl_dias=ttl_dias)
        return resultado

    def eliminar(self, namespace: str, clave: Any) -> bool:
        if not self._backend:
            return False
        try:
            return self._backend.eliminar(namespace, self.clave(clave))
        except Exception:
            return False

    def purgar_expirados(self) -> int:
        if not self._backend:
            return 0
        try:
            eliminadas = self._backend.purgar(time.time())
            if eliminadas:
                self._logger.info("Cache: %d registros expirados eliminados.", eliminadas)
            return eliminadas
        except Exception:
            return 0

    def estadisticas(self) -> Dict[str, Any]:
        base: Dict[str, Any] = {
            "habilitada": self.habilitado,
            "aciertos_proceso": self.aciertos,
            "escrituras_proceso": self.escrituras,
            "fallos_proceso": self.fallos,
            "ttl_segundos": {
                namespace: self.ttl_segundos(namespace) for namespace in sorted(self.ttl_por_namespace)
            },
        }
        if self._backend:
            try:
                base.update(self._backend.estadisticas(time.time()))
            except Exception:
                pass
        return base

    def cerrar(self) -> None:
        if self._backend:
            try:
                self._backend.cerrar()
            except Exception:
                pass


# =========================================================================== #
#  4) CLIENTE SERPER
# =========================================================================== #
class ClienteSerper:
    """Unico punto de salida hacia la Serper API.

    Garantias:
      * Rate-limit configurable (controla el consumo de creditos).
      * Backoff exponencial + jitter y respeto de ``Retry-After``.
      * Circuit breaker: tras N fallos seguidos pausa las llamadas.
      * Cache-first: si la consulta ya existe, NO se toca la red.
      * Nunca lanza excepciones: siempre devuelve un diccionario con ``ok``.
    """

    def __init__(
        self,
        api_key: str,
        endpoint: str = "https://google.serper.dev",
        cache: Optional[GestorCache] = None,
        rotador: Optional[RotadorProxies] = None,
        rate_limit_rps: float = 1.0,
        timeout: float = 15.0,
        max_reintentos: int = 3,
        retry_base: float = 1.5,
        retry_max: float = 30.0,
        breaker_umbral: int = 5,
        breaker_espera: float = 60.0,
        session: Optional[Any] = None,
        permitir_red: bool = True,
        al_consumir_creditos: Optional[Callable[[int], None]] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self._logger = logger or obtener_logger("optimizador.serper")
        self.api_key = api_key or ""
        self.endpoint = (endpoint or "https://google.serper.dev").rstrip("/")
        self.cache = cache
        self.rotador = rotador
        self.timeout = float(timeout)
        self.permitir_red = bool(permitir_red)
        self._al_consumir = al_consumir_creditos
        self.max_reintentos = max(1, int(max_reintentos))
        self.retry_base = max(0.1, float(retry_base))
        self.retry_max = max(0.1, float(retry_max))
        self.breaker_umbral = max(1, int(breaker_umbral))
        self.breaker_espera = max(1.0, float(breaker_espera))
        self._limitador = LimitadorTasa(rate_limit_rps)
        self._lock = threading.RLock()
        self._fallos_consecutivos = 0
        self._circuito_abierto_hasta = 0.0
        self.metricas: Dict[str, Any] = {
            "consultas_red": 0,
            "aciertos_cache": 0,
            "errores": 0,
            "reintentos": 0,
            "creditos_estimados": 0,
            "ultima_consulta": None,
        }
        self._session = session if session is not None else (requests.Session() if requests is not None else None)

    # ---------------------------- Propiedades ---------------------------- #
    @property
    def disponible(self) -> bool:
        return bool(self.api_key) and self._session is not None

    @property
    def operativo(self) -> bool:
        """Puede salir a la red ahora mismo (key + sesion + modo + circuito)."""
        return self.disponible and self.permitir_red and not self.circuito_abierto

    @property
    def circuito_abierto(self) -> bool:
        return time.monotonic() < self._circuito_abierto_hasta

    # ------------------------------- Buscar ------------------------------ #
    def buscar(
        self,
        consulta: str,
        tipo: str = "search",
        gl: str = "pe",
        hl: str = "es",
        num: int = 10,
        usar_cache: bool = True,
        ttl_dias: Optional[float] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Ejecuta una busqueda en Serper con cache, rate-limit y reintentos."""
        consulta = (consulta or "").strip()
        respuesta: Dict[str, Any] = {
            "ok": False,
            "consulta": consulta,
            "tipo": tipo,
            "desde_cache": False,
            "organico": [],
            "knowledge_graph": None,
            "answer_box": None,
            "personas_tambien_preguntan": [],
            "resultado": None,
            "creditos": 0,
            "error": None,
        }

        if not consulta:
            respuesta["error"] = "Consulta vacia"
            return respuesta

        # 1) CACHE-FIRST: vale incluso en modo local o sin API key (costo $0).
        clave_cache = clave_estable("serper", tipo, consulta, gl, hl, num)
        if usar_cache and self.cache is not None:
            cacheado = self.cache.obtener(Namespace.SERPER, clave_cache, _AUSENTE)
            if cacheado is not _AUSENTE and isinstance(cacheado, dict):
                respuesta.update(cacheado)
                respuesta["desde_cache"] = True
                self.metricas["aciertos_cache"] += 1
                self._logger.debug("Serper CACHE-HIT: %s", consulta[:120])
                return respuesta

        # 2) A partir de aqui hace falta red: se aplican los candados.
        if not self.permitir_red:
            respuesta["error"] = "modo LOCAL: red deshabilitada (costo $0, solo cache)"
            self._logger.debug("Consulta '%s' bloqueada por modo local.", consulta[:80])
            return respuesta
        if not self.disponible:
            respuesta["error"] = "SERPER_API_KEY no configurada o requests ausente"
            self._logger.warning("Serper no disponible: %s", respuesta["error"])
            return respuesta

        if self.circuito_abierto:
            restante = int(self._circuito_abierto_hasta - time.monotonic())
            respuesta["error"] = f"Circuit breaker abierto ({restante}s restantes)"
            self._logger.warning(respuesta["error"])
            return respuesta

        payload: Dict[str, Any] = {"q": consulta, "gl": gl, "hl": hl, "num": num}
        if extra:
            payload.update(extra)

        datos, error = self._peticion_con_reintentos(tipo, payload)
        if datos is None:
            respuesta["error"] = error or "Fallo desconocido en Serper"
            self.metricas["errores"] += 1
            return respuesta

        respuesta.update(
            {
                "ok": True,
                "organico": self._normalizar_organico(datos.get("organic")),
                "knowledge_graph": datos.get("knowledgeGraph"),
                "answer_box": datos.get("answerBox"),
                "personas_tambien_preguntan": datos.get("peopleAlsoAsk") or [],
                "resultado": datos,
                "creditos": int(datos.get("credits", 1) or 1),
            }
        )
        self.metricas["creditos_estimados"] += respuesta["creditos"]
        self._notificar_consumo(respuesta["creditos"])

        if usar_cache and self.cache is not None:
            self.cache.guardar(
                Namespace.SERPER,
                clave_cache,
                {k: v for k, v in respuesta.items() if k != "desde_cache"},
                ttl_dias=ttl_dias,
            )
        return respuesta

    # ---------------------------- Internos ------------------------------ #
    @staticmethod
    def _normalizar_organico(items: Any) -> List[Dict[str, Any]]:
        resultado: List[Dict[str, Any]] = []
        for indice, item in enumerate(items or [], start=1):
            if not isinstance(item, dict):
                continue
            resultado.append(
                {
                    "posicion": item.get("position", indice),
                    "titulo": item.get("title"),
                    "enlace": item.get("link"),
                    "snippet": item.get("snippet"),
                    "fecha": item.get("date"),
                    "fuente": item.get("source"),
                }
            )
        return resultado

    def _peticion_con_reintentos(
        self, tipo: str, payload: Dict[str, Any]
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        url = f"{self.endpoint}/{tipo.lstrip('/')}"
        cabeceras = {"X-API-KEY": self.api_key, "Content-Type": "application/json"}
        ultimo_error: Optional[str] = None

        for intento in range(1, self.max_reintentos + 1):
            estado_proxy = self.rotador.obtener() if self.rotador else None
            proxies = estado_proxy.como_requests() if estado_proxy else None
            self._limitador.esperar()

            try:
                self.metricas["consultas_red"] += 1
                self.metricas["ultima_consulta"] = time.strftime("%Y-%m-%d %H:%M:%S")
                self._logger.debug(
                    "Serper POST %s (intento %d/%d, proxy=%s)",
                    url,
                    intento,
                    self.max_reintentos,
                    estado_proxy.publico if estado_proxy else "directo",
                )
                respuesta = self._session.post(
                    url, json=payload, headers=cabeceras, timeout=self.timeout, proxies=proxies
                )

                if respuesta.status_code == 200:
                    try:
                        datos = respuesta.json()
                    except ValueError:
                        return None, "Respuesta de Serper no es JSON valido"
                    self._registrar_exito(estado_proxy)
                    return (datos if isinstance(datos, dict) else {"organic": datos}), None

                if respuesta.status_code in (400, 401, 403, 404):
                    detalle = (respuesta.text or "")[:200]
                    self._logger.error("Serper %s: %s", respuesta.status_code, detalle)
                    self._registrar_fallo()
                    return None, f"HTTP {respuesta.status_code}: {detalle}"

                if respuesta.status_code == 429:
                    ultimo_error = "HTTP 429 (rate limit)"
                    espera = self._espera_retry_after(respuesta, intento)
                    if self.rotador and estado_proxy:
                        self.rotador.reportar_fallo(estado_proxy)
                elif respuesta.status_code >= 500:
                    ultimo_error = f"HTTP {respuesta.status_code} (servidor)"
                    espera = self._backoff(intento)
                else:
                    ultimo_error = f"HTTP {respuesta.status_code}"
                    espera = self._backoff(intento)

                if intento < self.max_reintentos:
                    self.metricas["reintentos"] += 1
                    self._logger.warning("Serper %s; reintento en %.1fs", ultimo_error, espera)
                    time.sleep(espera)
                    continue
                self._registrar_fallo()
                return None, ultimo_error

            except Exception as error:
                ultimo_error = f"{type(error).__name__}: {error}"
                if self.rotador and estado_proxy:
                    self.rotador.reportar_fallo(estado_proxy)
                    self._logger.warning(
                        "Proxy %s fallo (%s); rotando.", estado_proxy.publico, ultimo_error
                    )
                else:
                    self._logger.warning("Error de red en Serper: %s", ultimo_error)
                if intento < self.max_reintentos:
                    self.metricas["reintentos"] += 1
                    time.sleep(self._backoff(intento))
                    continue
                self._registrar_fallo()
                return None, ultimo_error

        self._registrar_fallo()
        return None, ultimo_error or "Se agotaron los reintentos"

    def _backoff(self, intento: int) -> float:
        """Backoff exponencial con jitter, acotado por ``retry_max``."""
        base = min(self.retry_base * (2 ** (intento - 1)), self.retry_max)
        return base + random.uniform(0, base * 0.25)

    def _espera_retry_after(self, respuesta: Any, intento: int) -> float:
        """Respeta ``Retry-After`` de Serper; si no viene, usa el backoff normal."""
        crudo = None
        try:
            crudo = respuesta.headers.get("Retry-After")
        except Exception:
            crudo = None
        if not crudo:
            return self._backoff(intento)
        try:
            return min(max(0.0, float(crudo)), 60.0)
        except (TypeError, ValueError):
            return self._backoff(intento)

    def _registrar_exito(self, estado_proxy: Optional[EstadoProxy]) -> None:
        with self._lock:
            self._fallos_consecutivos = 0
            self._circuito_abierto_hasta = 0.0
        if self.rotador and estado_proxy:
            self.rotador.reportar_exito(estado_proxy)

    def _registrar_fallo(self) -> None:
        with self._lock:
            self._fallos_consecutivos += 1
            if self._fallos_consecutivos >= self.breaker_umbral:
                self._circuito_abierto_hasta = time.monotonic() + self.breaker_espera
                self._logger.error(
                    "Circuit breaker ABIERTO por %ds tras %d fallos consecutivos.",
                    int(self.breaker_espera),
                    self._fallos_consecutivos,
                )
                self._fallos_consecutivos = 0

    def _notificar_consumo(self, creditos: int) -> None:
        """Informa al orquestador los creditos realmente consumidos (persistencia)."""
        cantidad = max(0, int(creditos or 0))
        if cantidad and self._al_consumir is not None:
            try:
                self._al_consumir(cantidad)
            except Exception as error:
                self._logger.debug("No se pudo registrar el consumo de creditos: %s", error)

    def estadisticas(self) -> Dict[str, Any]:
        return {
            **self.metricas,
            "disponible": self.disponible,
            "permitir_red": self.permitir_red,
            "operativo": self.operativo,
            "circuito_abierto": self.circuito_abierto,
            "endpoint": self.endpoint,
            "api_key": mascarar(self.api_key),
        }

    def cerrar(self) -> None:
        if self._session is not None:
            try:
                self._session.close()
            except Exception:
                pass


# =========================================================================== #
#  5) FACHADA: OPTIMIZADOR
# =========================================================================== #
class Optimizador:
    """Fachada unica que los extractores importan y usan.

    Compone cache + proxies + Serper + descarga de paginas con Scrapling.

        from optimizador import get_optimizador
        opt = get_optimizador()
        opt.diagnostico()
    """

    def __init__(
        self,
        settings: Optional[Settings] = None,
        logger: Optional[logging.Logger] = None,
        solo_local: bool = False,
    ):
        self.settings = settings or get_settings()
        self.settings.asegurar_directorios()
        self._logger = logger or obtener_logger("optimizador")
        self.solo_local = bool(solo_local)

        self.cache = GestorCache(
            backend=self.settings.cache.backend,
            ruta=self.settings.cache.ruta,
            ttl_dias=self.settings.cache.ttl_dias,
            ttl_por_namespace=self.settings.cache.ttl_por_namespace,
            habilitado=self.settings.cache.habilitado,
            max_entradas=self.settings.cache.max_entradas,
            logger=obtener_logger("optimizador.cache"),
        )
        self.proxies = RotadorProxies(
            proxies=self.settings.proxies.urls,
            estrategia=self.settings.proxies.estrategia,
            max_fallos=self.settings.proxies.max_fallos,
            cooldown=self.settings.proxies.cooldown,
            logger=obtener_logger("optimizador.proxies"),
        )
        self.serper = ClienteSerper(
            api_key=self.settings.serper.api_key,
            endpoint=self.settings.serper.endpoint,
            cache=self.cache,
            rotador=self.proxies if self.proxies.hay_proxies else None,
            rate_limit_rps=self.settings.serper.rate_limit_rps,
            timeout=self.settings.serper.timeout,
            max_reintentos=self.settings.serper.max_reintentos,
            retry_base=self.settings.serper.retry_base,
            retry_max=self.settings.serper.retry_max,
            breaker_umbral=self.settings.serper.breaker_umbral,
            breaker_espera=self.settings.serper.breaker_espera,
            permitir_red=not self.solo_local,
            al_consumir_creditos=self.registrar_creditos,
            logger=obtener_logger("optimizador.serper"),
        )
        self._ultima_descarga = 0.0
        # Enrutador de motores por dominio (fetcher / dynamic).
        self.enrutador = EnrutadorMotores(
            politicas=self.settings.scraper.motores_por_dominio,
            por_defecto=self.settings.scraper.fetcher,
            autoescalar=self.settings.scraper.autoescalar,
            memoria=self.cache,
            logger=obtener_logger("optimizador.enrutador"),
        )
        self.metricas_enrutador: Dict[str, int] = self.enrutador.metricas
        if self.solo_local:
            self._logger.info("Optimizador en MODO LOCAL: sin red, solo cache y SQLite (costo $0).")
        for aviso in self.settings.advertencias():
            self._logger.warning("Config: %s", aviso)

    # ------------------------- Modo de operacion ------------------------- #
    @property
    def puede_usar_red(self) -> bool:
        return not self.solo_local

    def establecer_modo(self, solo_local: bool) -> None:
        """Alterna entre Modo Local ($0) y Modo OSINT/Live."""
        solo_local = bool(solo_local)
        if solo_local == self.solo_local:
            return
        self.solo_local = solo_local
        self.serper.permitir_red = not solo_local
        self._logger.info(
            "Modo de operacion: %s",
            "LOCAL (solo cache/BD, costo $0)" if solo_local else "OSINT/LIVE (Serper + Scrapling + proxies)",
        )

    # ------------------- Contadores persistentes (telemetria) ------------ #
    def registrar_creditos(self, cantidad: int = 1) -> int:
        """Acumula creditos consumidos de Serper en la base local (persistente)."""
        cantidad = max(0, int(cantidad))
        total = int(self.cache.obtener(Namespace.META, "serper_creditos", 0) or 0) + cantidad
        self.cache.guardar(Namespace.META, "serper_creditos", total, ttl_dias=0)
        self.cache.guardar(
            Namespace.META, "serper_ultimo_uso", time.strftime("%Y-%m-%d %H:%M:%S"), ttl_dias=0
        )
        return total

    def creditos_gastados(self) -> int:
        """Creditos de Serper consumidos en toda la historia del proyecto."""
        try:
            return int(self.cache.obtener(Namespace.META, "serper_creditos", 0) or 0)
        except (TypeError, ValueError):
            return 0

    def pronunciar_modo(self) -> str:
        return "LOCAL (costo $0)" if self.solo_local else "OSINT/LIVE"

    # ---------------------------- Ciclo de vida -------------------------- #
    def __enter__(self) -> "Optimizador":
        return self

    def __exit__(self, *_excepcion: Any) -> bool:
        self.cerrar()
        return False

    def cerrar(self) -> None:
        """Libera recursos. Seguro de llamar varias veces."""
        self.serper.cerrar()
        self.cache.cerrar()

    # ---------------------- Ficha de empresa (GRUPO 1) ------------------- #
    def empresa_consultada(self, ruc: str, namespace: str = Namespace.SUNAT) -> bool:
        """¿Ya tenemos datos vigentes de este RUC? Evita gastar creditos de nuevo."""
        return bool(ruc) and self.cache.existe(namespace, ruc)

    def obtener_empresa(self, ruc: str, namespace: str = Namespace.SUNAT) -> Optional[Dict[str, Any]]:
        datos = self.cache.obtener(namespace, ruc, None)
        return datos if isinstance(datos, dict) else None

    def guardar_empresa(
        self, ruc: str, datos: Dict[str, Any], namespace: str = Namespace.SUNAT, ttl_dias: Optional[float] = None
    ) -> bool:
        if not isinstance(datos, dict) or not datos:
            return False
        return self.cache.guardar(namespace, ruc, datos, ttl_dias=ttl_dias)

    def buscar(self, consulta: str, **kwargs: Any) -> Dict[str, Any]:
        """Atajo a :meth:`ClienteSerper.buscar`."""
        return self.serper.buscar(consulta, **kwargs)

    # ------------------- Descarga de paginas (Scrapling) ---------------- #
    def _pausa_cortesia(self) -> None:
        """Pausa entre requests con jitter para respetar la carga del servidor."""
        base = self.settings.scraper.delay_segundos
        jitter = self.settings.scraper.delay_jitter
        espera = base + (random.uniform(0, jitter) if jitter else 0.0)
        if espera > 0:
            time.sleep(espera)

    def _fetcher(self, clase: Optional[str] = None) -> Any:
        """Devuelve la clase de Scrapling configurada (import perezoso)."""
        from scrapling.fetchers import DynamicFetcher, Fetcher  # noqa: WPS433

        nombre = (clase or self.settings.scraper.fetcher or "fetcher").lower()
        return {
            "fetcher": Fetcher,
            "dynamic": DynamicFetcher,
        }.get(nombre, Fetcher)

    def descargar(
        self,
        url: str,
        fetcher: Optional[str] = None,
        page_action: Optional[Any] = None,
        page_setup: Optional[Any] = None,
        wait_selector: Optional[str] = None,
        wait_selector_state: Optional[str] = None,
        espera_ms: Optional[int] = None,
        timeout_ms: Optional[int] = None,
        cookies: Optional[Dict[str, Any]] = None,
        locale: Optional[str] = None,
        timezone_id: Optional[str] = None,
        bloquear_recursos: Optional[bool] = None,
        reintentos: Optional[int] = None,
        usar_proxy: bool = True,
        pausa_extra: float = 0.0,
        validador: Optional[Any] = None,
        **kwargs: Any,
    ) -> Optional[Any]:
        """Descarga una pagina con Scrapling rotando proxy. ``None`` si falla.

        **Enrutamiento inteligente**: si no se fuerza un motor, se usa el que
        corresponde al dominio (``SCRAPER_MOTORES``) y, si ese no devuelve
        contenido util, se **auto-escala** al siguiente de la cadena
        (``fetcher`` -> ``dynamic``). Asi el mismo pipeline puede procesar un
        portal estatico con HTTP puro y otro que requiere renderizado JS.
        """
        if not url or not str(url).startswith(("http://", "https://")):
            self._logger.debug("URL invalida para descarga: %r", url)
            return None
        if self.solo_local:
            self._logger.debug("Descarga de %s bloqueada: MODO LOCAL (costo $0).", url)
            return None

        cadena = self.enrutador.cadena(url, motor_forzado=fetcher)
        ultimo_motor = cadena[-1]
        for indice, motor in enumerate(cadena):
            if indice:
                self._logger.info(
                    "Enrutador: escalando a '%s' para %s (el motor anterior no devolvio contenido util).",
                    motor,
                    self.enrutador.dominio(url),
                )
            respuesta = self._descargar_con_motor(
                url,
                motor,
                page_action=page_action,
                page_setup=page_setup,
                wait_selector=wait_selector,
                wait_selector_state=wait_selector_state,
                espera_ms=espera_ms,
                timeout_ms=timeout_ms,
                cookies=cookies,
                locale=locale,
                timezone_id=timezone_id,
                bloquear_recursos=bloquear_recursos,
                reintentos=reintentos,
                usar_proxy=usar_proxy,
                pausa_extra=pausa_extra if indice == 0 else 0.0,
                **kwargs,
            )
            if respuesta is not None and validador is not None:
                try:
                    if not validador(respuesta):
                        self._logger.info(
                            "Contenido insuficiente en %s con motor '%s' (validador): se escala.",
                            url,
                            motor,
                        )
                        respuesta = None
                except Exception as error:
                    self._logger.debug("Validador con error en %s: %s", url, error)
            if respuesta is not None:
                self.enrutador.registrar_exito(url, motor)
                return respuesta
            self.enrutador.registrar_fallo(url, motor)
            if motor != ultimo_motor:
                self.metricas_enrutador["escalados"] += 1
        return None

    def _descargar_con_motor(self, url: str, clase: str, **opciones: Any) -> Optional[Any]:
        """Un intento de descarga con un motor concreto (sin escalado)."""
        pausa_extra = float(opciones.pop("pausa_extra", 0.0) or 0.0)
        usar_proxy = bool(opciones.pop("usar_proxy", True))
        self._pausa_cortesia()
        if pausa_extra > 0:
            time.sleep(pausa_extra)

        estado = self.proxies.obtener() if usar_proxy else None

        # curl_cffi (clase Fetcher) exige el proxy como CADENA; el navegador
        # (DynamicFetcher) lo prefiere como dict con credenciales separadas.
        if estado is None:
            proxy_param: Any = None
        elif clase == "fetcher":
            proxy_param = estado.url
        else:
            proxy_param = estado.como_playwright()

        reintentos_pedidos = opciones.pop("reintentos", None)
        parametros: Dict[str, Any] = {
            "timeout": opciones.pop("timeout_ms", None) or self.settings.scraper.timeout_ms,
            "retries": (
                self.settings.scraper.reintentos
                if reintentos_pedidos is None
                else max(1, int(reintentos_pedidos))
            ),
            "proxy": proxy_param,
        }

        solo_navegador = {
            "headless": self.settings.scraper.headless,
            "disable_resources": opciones.pop("bloquear_recursos", None)
            if opciones.get("bloquear_recursos") is not None
            else self.settings.scraper.bloquear_recursos,
            "network_idle": self.settings.scraper.network_idle,
        }
        opciones.pop("bloquear_recursos", None)

        if clase == "fetcher":
            for clave in ("wait_selector", "wait_selector_state", "espera_ms", "page_action",
                          "page_setup", "cookies", "locale", "timezone_id",
                          "headless", "disable_resources", "network_idle"):
                opciones.pop(clave, None)
        else:
            parametros.update(solo_navegador)
            if not opciones.get("espera_ms") and self.settings.scraper.espera_dinamica_ms:
                parametros["wait"] = self.settings.scraper.espera_dinamica_ms
            for clave in ("page_action", "page_setup", "cookies"):
                valor = opciones.pop(clave, None)
                if valor is not None:
                    parametros[clave] = valor
            selector = opciones.pop("wait_selector", None)
            if selector:
                parametros["wait_selector"] = selector
                parametros["wait_selector_state"] = opciones.pop("wait_selector_state", None) or "attached"
            else:
                opciones.pop("wait_selector_state", None)
            espera = opciones.pop("espera_ms", None)
            if espera:
                parametros["wait"] = espera
            for clave in ("locale", "timezone_id"):
                valor = opciones.pop(clave, None)
                if valor:
                    parametros[clave] = valor

        parametros.update(opciones)

        try:
            clase_scrapling = self._fetcher(clase)
            # `Fetcher` (curl_cffi) expone .get(); `DynamicFetcher` expone .fetch().
            invocador = getattr(clase_scrapling, "get", None) or getattr(clase_scrapling, "fetch", None)
            if invocador is None:
                self._logger.error("La clase %s no expone get/fetch.", clase_scrapling)
                return None
            respuesta = invocador(url, **parametros)
        except Exception as error:
            es_fallo_de_proxy = not isinstance(error, (TypeError, ValueError, AttributeError))
            if es_fallo_de_proxy and self.proxies.hay_proxies:
                self.proxies.reportar_fallo(estado)
            if es_fallo_de_proxy:
                self._logger.warning("Descarga fallida %s [%s]: %s", url, clase, error)
            else:
                self._logger.error("Error de invocacion al descargar %s: %s", url, error)
            return None

        estado_http = getattr(respuesta, "status", None)
        if estado_http and int(estado_http) >= 400:
            codigo = int(estado_http)
            # Solo los codigos imputables a la pasarela o limite de tasa penalizan al proxy.
            if codigo in (407, 429, 502, 503, 504):
                if self.proxies.hay_proxies:
                    self.proxies.reportar_fallo(estado)
                self._logger.warning("HTTP %s en %s (posible proxy saturado).", codigo, url)
            else:
                self._logger.info("HTTP %s en %s (el servidor rechazo la peticion).", codigo, url)
            return None

        # Validacion de longitud minima: detecta respuestas HTTP 200 con cuerpo vacio
        # (frecuente en sitios que requieren renderizado JS en navegador).
        if self.settings.scraper.validar_contenido:
            try:
                cuerpo = respuesta.html_content or ""
            except Exception:
                cuerpo = ""
            if len(cuerpo) < self.settings.scraper.min_bytes:
                self._logger.info(
                    "Respuesta incompleta en %s con motor '%s' (%d bytes).", url, clase, len(cuerpo)
                )
                return None

        if self.proxies.hay_proxies and estado is not None:
            self.proxies.reportar_exito(estado)
        return respuesta

    def descargar_html(
        self,
        url: str,
        usar_cache: bool = True,
        ttl_dias: Optional[float] = None,
        namespace: str = Namespace.WEB,
        validador: Optional[Any] = None,
        **kwargs: Any,
    ) -> Optional[str]:
        """Igual que :meth:`descargar` pero cacheando el HTML (ahorra red y proxy)."""
        if usar_cache:
            cacheado = self.cache.obtener(namespace, url, _AUSENTE)
            if cacheado is not _AUSENTE and isinstance(cacheado, str):
                return cacheado

        respuesta = self.descargar(url, validador=validador, **kwargs)
        if respuesta is None:
            return None

        try:
            html = respuesta.html_content
        except Exception:
            try:
                cuerpo = getattr(respuesta, "body", b"")
                html = cuerpo.decode("utf-8", errors="replace") if isinstance(cuerpo, bytes) else str(cuerpo)
            except Exception:
                return None

        if usar_cache and html:
            self.cache.guardar(namespace, url, html, ttl_dias=ttl_dias)
        return html

    # ----------------------------- Diagnostico --------------------------- #
    def diagnostico(self, verificar_proxies: bool = False) -> Dict[str, Any]:
        """Informe de salud del optimizador (sin exponer secretos)."""
        if verificar_proxies and self.settings.proxies.healthcheck:
            self.proxies.healthcheck(
                timeout=self.settings.proxies.healthcheck_timeout,
                url=self.settings.proxies.healthcheck_url,
            )
        informe = {
            "configuracion": self.settings.resumen_publico(),
            "cache": self.cache.estadisticas(),
            "proxies": self.proxies.estadisticas(),
            "serper": self.serper.estadisticas(),
        }
        self._logger.info("Diagnostico del optimizador completado.")
        return informe

    def telemetria(self) -> Dict[str, Any]:
        """Payload completo para el dashboard de telemetria (opcion 3 del menu)."""
        gastados = self.creditos_gastados()
        plan = max(0, int(self.settings.serper.plan_creditos))
        restantes = max(0, plan - gastados)
        proxies = self.proxies.estadisticas()
        ahorro = self.resumen_ahorro()

        return {
            "modo": "LOCAL (costo $0)" if self.solo_local else "OSINT/LIVE",
            "puede_usar_red": self.puede_usar_red,
            "entorno": self.settings.entorno,
            "alcance": self.settings.alcance.etiqueta,
            "sufijo_busqueda": self.settings.alcance.sufijo_busqueda,
            "meta_prospectos": self.settings.meta.limite_prospectos,
            "serper": {
                "api_key": mascarar(self.settings.serper.api_key),
                "endpoint": self.settings.serper.endpoint,
                "operativo": self.serper.operativo,
                "plan_creditos": plan,
                "creditos_gastados": gastados,
                "creditos_restantes": restantes,
                "consumo_pct": round((gastados / plan) * 100, 2) if plan else 0.0,
                "ultimo_uso": self.cache.obtener(Namespace.META, "serper_ultimo_uso", None),
                "llamadas_red": int(self.serper.metricas.get("consultas_red", 0)),
                "aciertos_cache": int(self.serper.metricas.get("aciertos_cache", 0)),
                "tasa_ahorro_pct": ahorro["tasa_ahorro_pct"],
                "reintentos": int(self.serper.metricas.get("reintentos", 0)),
                "errores": int(self.serper.metricas.get("errores", 0)),
                "circuito_abierto": self.serper.circuito_abierto,
            },
            "proxies": {
                "total": proxies["total"],
                "vivos": proxies["disponibles"],
                "baneados": proxies["en_cuarentena"],
                "estrategia": proxies["estrategia"],
                "detalle": proxies["detalle"],
            },
            "cache": self.cache.estadisticas(),
            "ahorro": ahorro,
            "enrutador": self.enrutador.estadisticas(),
            "advertencias": self.settings.advertencias(),
        }

    def resumen_ahorro(self) -> Dict[str, Any]:
        """Metricas de eficiencia: cuanto trafico/costo se evito."""
        metricas = self.serper.metricas
        red = int(metricas.get("consultas_red", 0))
        cache = int(metricas.get("aciertos_cache", 0))
        total = red + cache
        return {
            "consultas_totales": total,
            "consultas_red": red,
            "ahorradas_por_cache": cache,
            "tasa_ahorro_pct": round((cache / total) * 100, 2) if total else 0.0,
            "creditos_gastados": int(metricas.get("creditos_estimados", 0)),
            "reintentos": int(metricas.get("reintentos", 0)),
            "errores": int(metricas.get("errores", 0)),
            "proxies_disponibles": self.proxies.estadisticas()["disponibles"],
        }


# =========================================================================== #
#  Singleton de proceso
# =========================================================================== #
_instancia: Optional[Optimizador] = None
_lock_instancia = threading.Lock()


def get_optimizador(settings: Optional[Settings] = None) -> Optimizador:
    """Devuelve el optimizador compartido del proceso (creado una sola vez)."""
    global _instancia
    if _instancia is None:
        with _lock_instancia:
            if _instancia is None:
                _instancia = Optimizador(settings=settings)
    return _instancia


def cerrar_optimizador() -> None:
    """Cierra el singleton (util al final del pipeline y en tests)."""
    global _instancia
    with _lock_instancia:
        if _instancia is not None:
            _instancia.cerrar()
            _instancia = None


# =========================================================================== #
#  Autotest:  python optimizador.py
# =========================================================================== #
if __name__ == "__main__":  # pragma: no cover
    import sys

    from logger import configurar_logging

    configurar_logging(nivel="INFO", carpeta="logs")

    optimizador = get_optimizador()
    print(json.dumps(optimizador.diagnostico(), indent=2, ensure_ascii=False, default=str))

    if optimizador.serper.disponible and "--con-serper" in sys.argv:
        primera = optimizador.buscar("inmobiliarias Lima Peru", num=5)
        segunda = optimizador.buscar("inmobiliarias Lima Peru", num=5)
        print("\nPrimera  -> ok=%s desde_cache=%s" % (primera["ok"], primera["desde_cache"]))
        print("Segunda  -> ok=%s desde_cache=%s" % (segunda["ok"], segunda["desde_cache"]))
    print("\nAhorro:", json.dumps(optimizador.resumen_ahorro(), indent=2, ensure_ascii=False))

    cerrar_optimizador()
