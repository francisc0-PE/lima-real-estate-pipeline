"""
registro_ventas.py
==================
Modulo de **negocio comercial** (no de scraping): control de exclusividad y
duplicados sobre SQLite (``ventas.db``).

Esquema
-------
``avisos_extraidos``  (ID, fecha, ...)   -> avisos que ya salieron en un CSV
``avisos_vendidos``   (ID, cliente, fecha, ...) -> a quien y cuando se vendio
``auditoria``         (movimientos inmutables, para disputas con clientes)
``meta``              (version del esquema, permite migraciones futuras)

Regla de negocio (calidad A1)
-----------------------------
Si un aviso fue **entregado** o **vendido a cualquier cliente** en los ultimos
``EXCLUSIVIDAD_VENTANA_DIAS`` (30 dias por defecto), NO puede volver a salir en
un CSV: se garantiza exclusividad comercial al comprador.

Uso obligatorio en dos fases (desde ``main.py``)
-----------------------------------------------
    # FASE 1 - preguntar (NO escribe nada)
    resultado = registro.filtrar(registros, lote="csv_2026_10_13")
    exportar(resultado.disponibles)

    # FASE 2 - confirmar SOLO lo que realmente se entrego
    registro.confirmar_entrega(resultado.ids, lote="csv_2026_10_13")

Por que en dos fases: si se registrara antes de exportar y la escritura del CSV
fallara, el aviso quedaria "quemado" (bloqueado 30 dias) sin haberse entregado
a nadie. Es una perdida comercial silenciosa.

Tolerancia a fallos
-------------------
* Lecturas: si la base no responde y ``politica_error='cerrado'`` (por defecto)
  el aviso se considera **NO disponible** (fail-closed: ante la duda, no se
  entrega un aviso que podria estar vendido).
* Escrituras: devuelven ``False`` / ``ResultadoRegistro(ok=False)`` con el
  motivo, nunca propagan la excepcion de SQLite.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import logging
import re
import sqlite3
import sys
import threading
import time
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlsplit

from config import Settings, get_settings
from logger import obtener_logger

__all__ = [
    "RegistroVentas",
    "ResultadoFiltro",
    "ResultadoRegistro",
    "DecisionExclusividad",
    "ErrorRegistroVentas",
    "get_registro_ventas",
    "cerrar_registro_ventas",
    "generar_aviso_id",
    "normalizar_url",
]

VENTANA_DIAS_POR_DEFECTO = 30.0
ESQUEMA_VERSION = 1

# Campos que pueden identificar un aviso (en orden de prioridad).
CAMPOS_ID: Tuple[str, ...] = (
    "id_aviso",
    "aviso_id",
    "codigo_aviso",
    "codigo",
    "enlace_aviso",
    "enlace",
    "url",
    "link",
)
CAMPOS_URL: Tuple[str, ...] = ("enlace_aviso", "enlace", "url", "link")
# Un id ya canonico NO se vuelve a prefijar (idempotencia entre etapas).
PREFIJOS_CANONICOS: Tuple[str, ...] = ("id:", "url:", "ruc:", "empresa:", "huella:")
# Suficientes para reconocer la misma propiedad publicada en dos portales.
CAMPOS_HUELLA: Tuple[str, ...] = ("tipo_inmueble", "zona", "distrito", "m2", "precio_usd", "vendedor")
MIN_CAMPOS_HUELLA = 3  # menos que esto arriesga falsos duplicados


class ErrorRegistroVentas(Exception):
    """Error del registro comercial."""


# =========================================================================== #
#  Utilidades de identificacion de avisos
# =========================================================================== #
def _normalizar_texto(valor: Any) -> str:
    """Normaliza a [a-z0-9] para usar como clave (autocontenido a proposito).

    Se define aqui (y no se importa de ``optimizador``) para que este modulo de
    negocio no dependa de la capa de scraping.
    """
    if valor is None:
        return ""
    texto = unicodedata.normalize("NFKD", str(valor))
    texto = "".join(caracter for caracter in texto if not unicodedata.combining(caracter))
    return re.sub(r"[^a-z0-9]+", "", texto.lower())


def normalizar_url(url: Any) -> str:
    """Normaliza una URL para deduplicar: sin ``www``, sin query ni fragmento.

    Los parametros de campaña (``utm_source``, ``fbclid``...) hacen que la
    *misma* propiedad parezca un aviso distinto; aqui se eliminan.
    """
    if not url:
        return ""
    texto = str(url).strip()
    if "://" not in texto:
        texto = "http://" + texto
    try:
        partes = urlsplit(texto)
    except ValueError:
        return ""
    host = (partes.hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    ruta = partes.path.rstrip("/").lower()
    return f"{host}{ruta}" if host else ""


def generar_aviso_id(
    registro: Dict[str, Any],
    campos_id: Sequence[str] = CAMPOS_ID,
    campos_huella: Sequence[str] = CAMPOS_HUELLA,
) -> str:
    """Construye el identificador canonico de un prospecto.

    Prioridad:
      1. ``id_aviso`` / ``codigo`` del portal  -> ``"id:<valor>"``
      2. ``enlace_aviso`` normalizada          -> ``"url:<sha1>"``
      3. Empresa sin aviso (GRUPO 1): RUC       -> ``"ruc:<11 digitos>"``
         o razon social                         -> ``"empresa:<sha1>"``
      4. Huella del inmueble (>= 3 campos)     -> ``"huella:<sha1>"``
      5. ``""`` cuando no hay evidencia suficiente.
    """
    if not isinstance(registro, dict):
        return ""

    for campo in campos_id:
        valor = registro.get(campo)
        if valor is None or str(valor).strip() == "":
            continue
        # Idempotencia: si ya es un id canonico ("url:...", "ruc:..."), se
        # devuelve tal cual. Sin esto, el id se re-prefijaba en cada etapa
        # ("url:abc" -> "id:urlabc") y rompia la exclusividad y el historico.
        texto = str(valor).strip()
        if texto.startswith(PREFIJOS_CANONICOS):
            return texto
        if campo in CAMPOS_URL:
            limpio = normalizar_url(valor)
            if limpio:
                return "url:" + hashlib.sha1(limpio.encode("utf-8")).hexdigest()[:16]
        else:
            limpio = _normalizar_texto(valor)
            if limpio:
                return "id:" + limpio

    # Prospecto de empresa (GRUPO 1) sin aviso asociado: el RUC o el nombre
    # comercial son su identidad, y sin identidad no hay control de exclusividad.
    ruc = _normalizar_texto(registro.get("ruc"))
    if len(ruc) == 11:
        return "ruc:" + ruc
    razon_social = _normalizar_texto(registro.get("razon_social"))
    if len(razon_social) >= 6:
        return "empresa:" + hashlib.sha1(razon_social.encode("utf-8")).hexdigest()[:16]

    partes: List[str] = []
    for campo in campos_huella:
        valor = registro.get(campo)
        if valor is None or str(valor).strip() == "":
            continue
        partes.append(f"{campo}={_normalizar_texto(valor)}")
    if len(partes) >= MIN_CAMPOS_HUELLA:
        return "huella:" + hashlib.sha1("|".join(partes).encode("utf-8")).hexdigest()[:20]
    return ""


# =========================================================================== #
#  Estructuras de respuesta
# =========================================================================== #
@dataclass
class DecisionExclusividad:
    """Veredicto sobre si un aviso puede entregarse hoy."""

    aviso_id: str
    disponible: bool
    estado: str                       # disponible | extraido | vendido | sin_id | error
    motivo: str
    dias: Optional[float] = None      # antiguedad del ultimo evento
    dias_restantes: Optional[float] = None
    cliente: Optional[str] = None
    ultima_entrega: Optional[str] = None
    clientes_vigentes: int = 0
    permanente: bool = False          # True = bloqueo por venta (nunca se revende)

    @property
    def bloqueado(self) -> bool:
        return not self.disponible

    def como_dict(self) -> Dict[str, Any]:
        return {
            "aviso_id": self.aviso_id,
            "disponible": self.disponible,
            "estado": self.estado,
            "motivo": self.motivo,
            "dias": round(self.dias, 1) if self.dias is not None else None,
            "dias_restantes": round(self.dias_restantes, 1) if self.dias_restantes is not None else None,
            "cliente": self.cliente,
            "ultima_entrega": self.ultima_entrega,
            "clientes_vigentes": self.clientes_vigentes,
            "permanente": self.permanente,
        }


@dataclass
class ResultadoRegistro:
    """Resultado de una operacion de escritura (venta, anulacion)."""

    ok: bool
    motivo: str
    aviso_id: str = ""
    cliente: Optional[str] = None

    def __bool__(self) -> bool:
        return self.ok


@dataclass
class ResultadoFiltro:
    """Salida de :meth:`RegistroVentas.filtrar`."""

    disponibles: List[Dict[str, Any]] = field(default_factory=list)
    omitidos: List[Dict[str, Any]] = field(default_factory=list)
    duplicados_lote: List[Dict[str, Any]] = field(default_factory=list)
    sin_id: List[Dict[str, Any]] = field(default_factory=list)
    decisiones: List[DecisionExclusividad] = field(default_factory=list)

    @property
    def ids(self) -> List[str]:
        """IDs canonicos de los avisos aptos para entrega (fase 2)."""
        return [str(registro.get("id_aviso")) for registro in self.disponibles if registro.get("id_aviso")]

    @property
    def total_entrada(self) -> int:
        return len(self.disponibles) + len(self.omitidos)

    def motivos(self) -> Dict[str, int]:
        conteo: Dict[str, int] = {}
        for registro in self.omitidos:
            razon = str(registro.get("_exclusividad", {}).get("estado", "desconocido"))
            conteo[razon] = conteo.get(razon, 0) + 1
        return conteo

    def resumen(self) -> Dict[str, Any]:
        return {
            "entrada": self.total_entrada,
            "disponibles": len(self.disponibles),
            "omitidos": len(self.omitidos),
            "duplicados_en_lote": len(self.duplicados_lote),
            "sin_id": len(self.sin_id),
            "motivos": self.motivos(),
        }


# =========================================================================== #
#  Registro comercial
# =========================================================================== #
class RegistroVentas:
    """Control de exclusividad comercial y duplicados sobre SQLite."""

    def __init__(
        self,
        ruta: Optional[Path | str] = None,
        ventana_dias: float = VENTANA_DIAS_POR_DEFECTO,
        retencion_dias: float = 730.0,
        politica_error: str = "cerrado",
        bloqueo_venta_permanente: bool = True,
        logger: Optional[logging.Logger] = None,
    ):
        self._logger = logger or obtener_logger("registro_ventas")
        self.ventana_dias = max(1.0, float(ventana_dias))
        self.retencion_dias = max(self.ventana_dias, float(retencion_dias))
        self.politica_error = politica_error if politica_error in {"cerrado", "abierto"} else "cerrado"
        # Regla comercial: una venta NO expira (exclusividad total por cliente).
        self.bloqueo_venta_permanente = bool(bloqueo_venta_permanente)
        self.ruta = Path(ruta) if ruta else get_settings().ventas.ruta
        self.ruta.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.metricas: Dict[str, int] = {
            "consultas": 0,
            "entregas": 0,
            "ventas": 0,
            "anulaciones": 0,
            "bloqueos": 0,
            "errores": 0,
        }

        self._conn = sqlite3.connect(str(self.ruta), timeout=30.0, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._crear_esquema()
        self._logger.info(
            "Registro de ventas listo en %s (ventana de exclusividad: %.0f dias).",
            self.ruta,
            self.ventana_dias,
        )

    # ------------------------------------------------------------------ #
    #  Ciclo de vida
    # ------------------------------------------------------------------ #
    def __enter__(self) -> "RegistroVentas":
        return self

    def __exit__(self, *_excepcion: Any) -> bool:
        self.cerrar()
        return False

    def cerrar(self) -> None:
        with self._lock:
            try:
                self._conn.commit()
                self._conn.close()
            except sqlite3.Error:
                pass

    # ------------------------------------------------------------------ #
    #  Esquema
    # ------------------------------------------------------------------ #
    def _crear_esquema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=NORMAL;
                PRAGMA busy_timeout=30000;

                CREATE TABLE IF NOT EXISTS meta (
                    clave TEXT PRIMARY KEY,
                    valor TEXT NOT NULL
                );

                -- TABLA 1: avisos extraidos / entregados
                CREATE TABLE IF NOT EXISTS avisos_extraidos (
                    aviso_id     TEXT PRIMARY KEY,
                    fecha        TEXT NOT NULL,
                    timestamp    REAL NOT NULL,
                    veces        INTEGER NOT NULL DEFAULT 1,
                    ultima_fecha TEXT NOT NULL,
                    ultima_ts    REAL NOT NULL,
                    ultimo_lote  TEXT,
                    fuente       TEXT,
                    url          TEXT
                );
                CREATE INDEX IF NOT EXISTS ix_extraidos_ultima ON avisos_extraidos (ultima_ts);

                -- TABLA 2: avisos vendidos
                CREATE TABLE IF NOT EXISTS avisos_vendidos (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    aviso_id   TEXT NOT NULL,
                    cliente    TEXT NOT NULL COLLATE NOCASE,
                    fecha      TEXT NOT NULL,
                    timestamp  REAL NOT NULL,
                    precio_usd REAL,
                    lote       TEXT,
                    nota       TEXT,
                    anulada    INTEGER NOT NULL DEFAULT 0,
                    anulada_ts REAL
                );
                CREATE INDEX IF NOT EXISTS ix_vendidos_aviso ON avisos_vendidos (aviso_id, timestamp);
                CREATE INDEX IF NOT EXISTS ix_vendidos_cliente ON avisos_vendidos (cliente, timestamp);

                -- Auditoria inmutable de movimientos
                CREATE TABLE IF NOT EXISTS auditoria (
                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    tipo      TEXT NOT NULL,
                    aviso_id  TEXT,
                    cliente   TEXT,
                    detalle   TEXT,
                    fecha     TEXT NOT NULL,
                    timestamp REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS ix_auditoria_ts ON auditoria (timestamp);
                """
            )
            version = self._conn.execute("SELECT valor FROM meta WHERE clave = 'version_esquema'").fetchone()
            if version is None:
                self._conn.execute(
                    "INSERT INTO meta (clave, valor) VALUES ('version_esquema', ?)", (str(ESQUEMA_VERSION),)
                )
            elif int(version["valor"] or 0) != ESQUEMA_VERSION:
                self._logger.warning(
                    "ventas.db en version de esquema %s (esperada %s): revise migraciones.",
                    version["valor"],
                    ESQUEMA_VERSION,
                )
            self._conn.commit()

    # ------------------------------------------------------------------ #
    #  Helpers internos
    # ------------------------------------------------------------------ #
    @staticmethod
    def _ahora() -> Tuple[str, float]:
        momento = datetime.now(timezone.utc)
        return momento.isoformat(timespec="seconds"), momento.timestamp()

    def _corte(self) -> float:
        """Timestamp limite de la ventana de exclusividad."""
        return time.time() - self.ventana_dias * 86_400.0

    @staticmethod
    def _dias(desde_ts: float) -> float:
        return max(0.0, (time.time() - desde_ts) / 86_400.0)

    def _auditar(self, tipo: str, aviso_id: Optional[str], cliente: Optional[str], detalle: str) -> None:
        fecha, momento = self._ahora()
        try:
            self._conn.execute(
                "INSERT INTO auditoria (tipo, aviso_id, cliente, detalle, fecha, timestamp) VALUES (?,?,?,?,?,?)",
                (tipo, aviso_id, cliente, detalle, fecha, momento),
            )
        except sqlite3.Error as error:  # la auditoria nunca debe tumbar el flujo
            self._logger.debug("No se pudo auditar %s: %s", tipo, error)

    def _error_decision(self, aviso_id: str, error: Exception, extra: str = "") -> DecisionExclusividad:
        """Aplica la politica ante fallo de verificacion (fail-closed por defecto)."""
        self.metricas["errores"] += 1
        self._logger.error("Error consultando la exclusividad de %s: %s", aviso_id, error)
        if self.politica_error == "abierto":
            return DecisionExclusividad(aviso_id, True, "error", f"no verificable (politica abierta): {error}")
        return DecisionExclusividad(
            aviso_id, False, "error", f"no verificable; se omite por seguridad {extra}".strip()
        )

    # ------------------------------------------------------------------ #
    #  LECTURA: estado y exclusividad
    # ------------------------------------------------------------------ #
    def _ventas_activas(self, aviso_id: str) -> List[sqlite3.Row]:
        """Ventas NO anuladas de un aviso.

        Con ``bloqueo_venta_permanente=True`` (por defecto) el tiempo no
        importa: vendido una vez = nunca mas se revende a otro cliente.
        """
        consulta = """
            SELECT id, aviso_id, cliente, fecha, timestamp, precio_usd
            FROM avisos_vendidos
            WHERE aviso_id = ? AND anulada = 0
        """
        parametros: List[Any] = [aviso_id]
        if not self.bloqueo_venta_permanente:
            consulta += " AND timestamp >= ?"
            parametros.append(self._corte())
        consulta += " ORDER BY timestamp DESC"
        return list(self._conn.execute(consulta, parametros).fetchall())

    def estado(self, aviso_id: Optional[str]) -> DecisionExclusividad:
        """Veredicto de exclusividad para un aviso. Nunca lanza excepcion.

        Prioridad del veredicto:
          1. Venta registrada -> bloqueo PERMANENTE (nunca se revende).
          2. Entrega/extraccion dentro de la ventana de ``ventana_dias``.
          3. Disponible.
        """
        clave = (aviso_id or "").strip()
        if not clave:
            return DecisionExclusividad("", False, "sin_id", "sin identificador de aviso")

        try:
            self.metricas["consultas"] += 1
            ventas = self._ventas_activas(clave)
            extraido = self._conn.execute(
                """
                SELECT ultima_fecha, ultima_ts, ultimo_lote
                FROM avisos_extraidos WHERE aviso_id = ?
                """,
                (clave,),
            ).fetchone()
        except sqlite3.Error as error:
            return self._error_decision(clave, error)

        if ventas:
            reciente = ventas[0]
            dias = self._dias(float(reciente["timestamp"]))
            fecha = str(reciente["fecha"])[:10]
            motivo = (
                f"vendido a {reciente['cliente']} el {fecha} (exclusividad permanente)"
                if self.bloqueo_venta_permanente
                else f"vendido a {reciente['cliente']} hace {dias:.0f} dias"
            )
            decision = DecisionExclusividad(
                aviso_id=clave,
                disponible=False,
                estado="vendido",
                motivo=motivo,
                dias=dias,
                dias_restantes=None if self.bloqueo_venta_permanente else max(0.0, self.ventana_dias - dias),
                cliente=reciente["cliente"],
                clientes_vigentes=len({str(v["cliente"]).lower() for v in ventas}),
                permanente=self.bloqueo_venta_permanente,
            )
            self.metricas["bloqueos"] += 1
            return decision

        if extraido is not None and float(extraido["ultima_ts"]) >= self._corte():
            dias = self._dias(float(extraido["ultima_ts"]))
            lote = extraido["ultimo_lote"] or "sin lote"
            decision = DecisionExclusividad(
                aviso_id=clave,
                disponible=False,
                estado="extraido",
                motivo=f"ya entregado hace {dias:.0f} dias (lote: {lote})",
                dias=dias,
                dias_restantes=max(0.0, self.ventana_dias - dias),
                ultima_entrega=extraido["ultima_fecha"],
            )
            self.metricas["bloqueos"] += 1
            return decision

        return DecisionExclusividad(
            aviso_id=clave,
            disponible=True,
            estado="disponible",
            motivo=(
                f"sin ventas registradas y no entregado en los ultimos {self.ventana_dias:.0f} dias"
            ),
        )

    def esta_disponible(self, aviso_id: Optional[str]) -> bool:
        """Atajo booleano de :meth:`estado`."""
        return self.estado(aviso_id).disponible

    # ------------------------------------------------------------------ #
    #  FASE 1: filtrar (no escribe nada)
    # ------------------------------------------------------------------ #
    def filtrar(
        self,
        registros: Sequence[Dict[str, Any]],
        lote: Optional[str] = None,
        campo_id: Optional[str] = None,
        campo_destino: str = "id_aviso",
        deduplicar_lote: bool = True,
        politica_sin_id: str = "conservar",
    ) -> ResultadoFiltro:
        """Aplica exclusividad + deduplicacion SIN registrar nada.

        Devuelve un :class:`ResultadoFiltro`. Cada registro apto sale *copiado*
        con su ID canonico en ``campo_destino`` (por defecto ``id_aviso``), de
        modo que el CSV final lo incluya y la venta posterior se pueda registrar
        contra ese mismo ID.
        """
        resultado = ResultadoFiltro()
        vistos: Dict[str, int] = {}

        for indice, registro in enumerate(registros or []):
            if not isinstance(registro, dict):
                self._logger.debug("Registro no valido en el lote (fila %d): se omite.", indice)
                continue

            if campo_id and registro.get(campo_id):
                aviso_id = "id:" + _normalizar_texto(registro[campo_id])
            else:
                aviso_id = generar_aviso_id(registro)

            # --- Sin identificador: no hay forma de garantizar exclusividad ---
            if not aviso_id:
                copia = dict(registro)
                resultado.sin_id.append(copia)
                if politica_sin_id == "descartar":
                    copia["_exclusividad"] = {
                        "estado": "sin_id",
                        "motivo": "sin identificador: no se puede garantizar exclusividad",
                        "aviso_id": "",
                    }
                    resultado.omitidos.append(copia)
                else:
                    resultado.disponibles.append(copia)
                continue

            # --- Duplicados dentro del mismo lote (independiente de la exclusividad) ---
            if deduplicar_lote and aviso_id in vistos:
                copia = dict(registro)
                copia["_exclusividad"] = {
                    "estado": "duplicado_lote",
                    "motivo": f"repetido en el mismo lote (fila {vistos[aviso_id]})",
                    "aviso_id": aviso_id,
                }
                resultado.duplicados_lote.append(copia)
                resultado.omitidos.append(copia)
                continue
            vistos[aviso_id] = indice

            # --- Regla de negocio: ventana de exclusividad ---
            decision = self.estado(aviso_id)
            resultado.decisiones.append(decision)
            copia = dict(registro)
            copia[campo_destino] = aviso_id

            if decision.disponible:
                resultado.disponibles.append(copia)
            else:
                copia["_exclusividad"] = decision.como_dict()
                resultado.omitidos.append(copia)

        self._logger.info(
            "Filtro de exclusividad: %d disponibles, %d omitidos (%s).",
            len(resultado.disponibles),
            len(resultado.omitidos),
            resultado.motivos() or "sin omisiones",
        )
        return resultado

    # ------------------------------------------------------------------ #
    #  FASE 2: confirmar entrega (escribe)
    # ------------------------------------------------------------------ #
    def _upsert_extraido(
        self, aviso_id: str, lote: Optional[str], fuente: Optional[str], url: Optional[str], cuenta: bool
    ) -> bool:
        fecha, momento = self._ahora()
        incremento = 1 if cuenta else 0
        self._conn.execute(
            """
            INSERT INTO avisos_extraidos
                (aviso_id, fecha, timestamp, veces, ultima_fecha, ultima_ts, ultimo_lote, fuente, url)
            VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?)
            ON CONFLICT(aviso_id) DO UPDATE SET
                veces        = avisos_extraidos.veces + ?,
                ultima_fecha = excluded.ultima_fecha,
                ultima_ts    = excluded.ultima_ts,
                ultimo_lote  = COALESCE(excluded.ultimo_lote, avisos_extraidos.ultimo_lote),
                fuente       = COALESCE(excluded.fuente, avisos_extraidos.fuente),
                url          = COALESCE(excluded.url, avisos_extraidos.url)
            """,
            (aviso_id, fecha, momento, fecha, momento, lote, fuente, url, incremento),
        )
        return True

    def confirmar_entrega(
        self,
        aviso_ids: Iterable[str],
        lote: Optional[str] = None,
        fuente: Optional[str] = None,
        urls: Optional[Dict[str, str]] = None,
    ) -> int:
        """Registra los avisos que REALMENTE se entregaron. Devuelve el conteo.

        Debe invocarse despues de escribir el CSV con exito.
        """
        limpios = [str(a).strip() for a in (aviso_ids or []) if a and str(a).strip()]
        if not limpios:
            return 0
        urls = urls or {}
        with self._lock:
            try:
                with self._conn:  # transaccion atomica
                    for aviso_id in limpios:
                        self._upsert_extraido(
                            aviso_id, lote, fuente, urls.get(aviso_id), cuenta=True
                        )
                    self._auditar("entrega", None, None, f"{len(limpios)} avisos en lote {lote or '-'}")
                self.metricas["entregas"] += len(limpios)
                self._logger.info("Entrega confirmada: %d avisos (lote %s).", len(limpios), lote or "-")
                return len(limpios)
            except sqlite3.Error as error:
                self.metricas["errores"] += 1
                self._logger.error("No se pudo confirmar la entrega: %s", error)
                return 0

    def registrar_extraccion(
        self, aviso_id: str, lote: Optional[str] = None, fuente: Optional[str] = None, url: Optional[str] = None
    ) -> bool:
        """Registra un unico aviso (util para pruebas o cargas manuales)."""
        return self.confirmar_entrega([aviso_id], lote=lote, fuente=fuente, urls={aviso_id: url} if url else None) == 1

    # ------------------------------------------------------------------ #
    #  Ventas
    # ------------------------------------------------------------------ #
    def registrar_venta(
        self,
        aviso_id: str,
        cliente: str,
        precio_usd: Optional[float] = None,
        lote: Optional[str] = None,
        nota: Optional[str] = None,
        forzar: bool = False,
    ) -> ResultadoRegistro:
        """Registra la venta de un aviso y lo bloquea de forma permanente.

        Reglas:
          * Idempotente: la misma venta al mismo cliente no se duplica.
          * ``forzar=False`` (por defecto): si el aviso ya fue vendido a OTRO
            cliente (en cualquier momento), se rechaza. La venta es definitiva.
          * Una venta implica entrega: el aviso queda bloqueado tambien para el
            control de extracciones.
        """
        clave = (aviso_id or "").strip()
        comprador = (cliente or "").strip()
        if not clave:
            return ResultadoRegistro(False, "falta el identificador del aviso")
        if not comprador:
            return ResultadoRegistro(False, "falta el nombre del cliente", aviso_id=clave)

        with self._lock:
            try:
                vigentes = self._ventas_activas(clave)
            except sqlite3.Error as error:
                self.metricas["errores"] += 1
                self._logger.error("Venta no registrada (%s): %s", clave, error)
                return ResultadoRegistro(False, f"error de base de datos: {error}", aviso_id=clave)

            mismo = [v for v in vigentes if str(v["cliente"]).lower() == comprador.lower()]
            if mismo:
                return ResultadoRegistro(
                    True, f"venta ya registrada para {comprador}", aviso_id=clave, cliente=comprador
                )

            otros = [v for v in vigentes if str(v["cliente"]).lower() != comprador.lower()]
            if otros and not forzar:
                fecha = str(otros[0]["fecha"])[:10]
                alcance = "exclusividad permanente" if self.bloqueo_venta_permanente else "exclusividad vigente"
                return ResultadoRegistro(
                    False,
                    f"{alcance}: ya vendido a {otros[0]['cliente']} el {fecha}",
                    aviso_id=clave,
                )

            fecha, momento = self._ahora()
            try:
                with self._conn:
                    self._conn.execute(
                        """
                        INSERT INTO avisos_vendidos
                            (aviso_id, cliente, fecha, timestamp, precio_usd, lote, nota)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (clave, comprador, fecha, momento, precio_usd, lote, nota),
                    )
                    # Una venta implica entrega: se refresca la ventana de exclusividad.
                    self._upsert_extraido(clave, lote=f"venta:{comprador}", fuente="venta", url=None, cuenta=False)
                    self._auditar(
                        "venta_forzada" if (otros and forzar) else "venta",
                        clave,
                        comprador,
                        f"precio={precio_usd} nota={nota or '-'}",
                    )
            except sqlite3.Error as error:
                self.metricas["errores"] += 1
                self._logger.error("No se pudo registrar la venta de %s: %s", clave, error)
                return ResultadoRegistro(False, f"error de base de datos: {error}", aviso_id=clave)

            self.metricas["ventas"] += 1
            if otros:
                self._logger.warning(
                    "VENTA FORZADA de %s a %s (ya vendido a %s).",
                    clave,
                    comprador,
                    otros[0]["cliente"],
                )
            self._logger.info("Venta registrada: %s -> %s (%s USD).", clave, comprador, precio_usd)
            return ResultadoRegistro(True, "venta registrada", aviso_id=clave, cliente=comprador)

    def anular_venta(self, aviso_id: str, cliente: Optional[str] = None, motivo: Optional[str] = None) -> int:
        """Anula ventas de un aviso (y opcionalmente de un cliente). Devuelve cuantas."""
        clave = (aviso_id or "").strip()
        if not clave:
            return 0
        consulta = "UPDATE avisos_vendidos SET anulada = 1, anulada_ts = ? WHERE aviso_id = ? AND anulada = 0"
        parametros: List[Any] = [time.time(), clave]
        if cliente:
            consulta += " AND cliente = ? COLLATE NOCASE"
            parametros.append(cliente)
        with self._lock:
            try:
                with self._conn:
                    cursor = self._conn.execute(consulta, parametros)
                    self._auditar("anulacion", clave, cliente, motivo or "sin motivo")
            except sqlite3.Error as error:
                self.metricas["errores"] += 1
                self._logger.error("No se pudo anular la venta de %s: %s", clave, error)
                return 0
        self.metricas["anulaciones"] += max(0, cursor.rowcount)
        self._logger.info("Ventas anuladas de %s: %d.", clave, cursor.rowcount)
        return max(0, cursor.rowcount)

    def clientes_de(self, aviso_id: str) -> List[Dict[str, Any]]:
        """Historial de clientes de un aviso (incluye anuladas y vencidas)."""
        try:
            filas = self._conn.execute(
                """
                SELECT cliente, fecha, precio_usd, anulada
                FROM avisos_vendidos WHERE aviso_id = ? ORDER BY timestamp DESC
                """,
                ((aviso_id or "").strip(),),
            ).fetchall()
        except sqlite3.Error:
            return []
        return [dict(fila) for fila in filas]

    def historial(self, aviso_id: str) -> Dict[str, Any]:
        """Ficha completa de un aviso: entrega + ventas + veredicto actual."""
        clave = (aviso_id or "").strip()
        extraido: Optional[Dict[str, Any]] = None
        try:
            fila = self._conn.execute(
                "SELECT * FROM avisos_extraidos WHERE aviso_id = ?", (clave,)
            ).fetchone()
            extraido = dict(fila) if fila else None
        except sqlite3.Error:
            extraido = None
        return {
            "aviso_id": clave,
            "entrega": extraido,
            "clientes": self.clientes_de(clave),
            "decision": self.estado(clave).como_dict(),
        }

    # ------------------------------------------------------------------ #
    #  Reportes y mantenimiento
    # ------------------------------------------------------------------ #
    def resumen(self) -> Dict[str, Any]:
        """Metricas comerciales (apto para logs, dashboard y telemetria)."""
        corte = self._corte()
        try:
            extraidos_total = self._conn.execute("SELECT COUNT(*) AS n FROM avisos_extraidos").fetchone()["n"]
            extraidos_vigentes = self._conn.execute(
                "SELECT COUNT(*) AS n FROM avisos_extraidos WHERE ultima_ts >= ?", (corte,)
            ).fetchone()["n"]
            ventas_total = self._conn.execute("SELECT COUNT(*) AS n FROM avisos_vendidos").fetchone()["n"]
            activas = self._conn.execute(
                """
                SELECT COUNT(*) AS n,
                       COALESCE(SUM(precio_usd), 0) AS monto,
                       COUNT(DISTINCT cliente) AS clientes
                FROM avisos_vendidos WHERE anulada = 0
                """
            ).fetchone()
            en_ventana = self._conn.execute(
                "SELECT COUNT(*) AS n FROM avisos_vendidos WHERE anulada = 0 AND timestamp >= ?", (corte,)
            ).fetchone()["n"]
            anuladas = self._conn.execute(
                "SELECT COUNT(*) AS n FROM avisos_vendidos WHERE anulada = 1"
            ).fetchone()["n"]
            top = self._conn.execute(
                """
                SELECT cliente, COUNT(*) AS ventas, COALESCE(SUM(precio_usd), 0) AS monto_usd
                FROM avisos_vendidos WHERE anulada = 0
                GROUP BY cliente ORDER BY ventas DESC LIMIT 5
                """
            ).fetchall()
        except sqlite3.Error as error:
            self.metricas["errores"] += 1
            return {"error": str(error), "db": str(self.ruta)}

        return {
            "db": str(self.ruta),
            "ventana_dias": self.ventana_dias,
            "venta_permanente": self.bloqueo_venta_permanente,
            "avisos_extraidos_total": extraidos_total,
            "avisos_extraidos_vigentes": extraidos_vigentes,
            "avisos_entregables_libres": max(0, extraidos_total - extraidos_vigentes),
            "ventas_total": ventas_total,
            "ventas_activas": activas["n"],            # bloqueo permanente
            "ventas_en_ventana": en_ventana,           # entregas recientes
            "clientes_unicos": activas["clientes"],
            "monto_activo_usd": round(float(activas["monto"] or 0), 2),
            "ventas_anuladas": anuladas,
            "top_clientes": [dict(fila) for fila in top],
            "metricas_proceso": dict(self.metricas),
        }

    def validar_integridad(self) -> Dict[str, Any]:
        """Auditoria de calidad: detecta inconsistencias comerciales graves."""
        alertas: List[Dict[str, Any]] = []
        try:
            # Con exclusividad permanente, CUALQUIER aviso con mas de un cliente
            # (no anulado) es una violacion de contrato comercial.
            conflictos = self._conn.execute(
                """
                SELECT aviso_id, COUNT(DISTINCT cliente) AS clientes,
                       GROUP_CONCAT(DISTINCT cliente) AS quienes, MIN(fecha) AS desde
                FROM avisos_vendidos
                WHERE anulada = 0
                GROUP BY aviso_id HAVING COUNT(DISTINCT cliente) > 1
                """
            ).fetchall()
            for fila in conflictos:
                alertas.append(
                    {
                        "tipo": "venta_multiple",
                        "aviso_id": fila["aviso_id"],
                        "detalle": f"{fila['clientes']} clientes ({fila['quienes']}) desde {str(fila['desde'])[:10]}",
                    }
                )
            repetidos = self._conn.execute(
                "SELECT aviso_id, veces FROM avisos_extraidos WHERE veces > 10"
            ).fetchall()
            for fila in repetidos:
                alertas.append(
                    {
                        "tipo": "entrega_reiterada",
                        "aviso_id": fila["aviso_id"],
                        "detalle": f"entregado {fila['veces']} veces: posible fuga de exclusividad",
                    }
                )
            vendidos_sin_entrega = self._conn.execute(
                """
                SELECT v.aviso_id FROM avisos_vendidos AS v
                LEFT JOIN avisos_extraidos AS e ON e.aviso_id = v.aviso_id
                WHERE v.anulada = 0 AND e.aviso_id IS NULL
                """
            ).fetchall()
            if vendidos_sin_entrega:
                alertas.append(
                    {
                        "tipo": "venta_sin_entrega",
                        "aviso_id": None,
                        "detalle": f"{len(vendidos_sin_entrega)} ventas sin registro de entrega",
                    }
                )
        except sqlite3.Error as error:
            alertas.append({"tipo": "error", "aviso_id": None, "detalle": str(error)})

        return {
            "ok": not alertas,
            "alertas": alertas,
            "revisado": "exclusividad permanente por venta + ventana de entregas",
        }

    def purgar_antiguos(self, dias: Optional[float] = None) -> Dict[str, int]:
        """Aplica retencion de datos.

        Solo elimina **extracciones** y **auditoria** antiguas. Las ventas
        jamas se purgan: son el respaldo comercial y legal del negocio.
        """
        limite = float(dias if dias is not None else self.retencion_dias)
        corte = time.time() - limite * 86_400.0
        eliminados = {"extraidos": 0, "auditoria": 0}
        with self._lock:
            try:
                with self._conn:
                    eliminados["extraidos"] = self._conn.execute(
                        "DELETE FROM avisos_extraidos WHERE ultima_ts < ?", (corte,)
                    ).rowcount
                    eliminados["auditoria"] = self._conn.execute(
                        "DELETE FROM auditoria WHERE timestamp < ?", (corte,)
                    ).rowcount
            except sqlite3.Error as error:
                self.metricas["errores"] += 1
                self._logger.error("No se pudo purgar el registro: %s", error)
        if any(eliminados.values()):
            self._logger.info("Retencion aplicada (>%.0f dias): %s", limite, eliminados)
        return eliminados

    def exportar_csv(self, ruta: Optional[Path | str] = None, dias: Optional[float] = None) -> Optional[Path]:
        """Exporta el reporte de ventas (para el area comercial / auditoria)."""
        limite = float(dias if dias is not None else self.ventana_dias)
        destino = Path(ruta) if ruta else self.ruta.parent / f"reporte_ventas_{time.strftime('%Y%m%d')}.csv"
        try:
            filas = self._conn.execute(
                """
                SELECT v.aviso_id, v.cliente, v.fecha, v.precio_usd, v.anulada,
                       e.fuente, e.ultimo_lote, e.veces
                FROM avisos_vendidos AS v
                LEFT JOIN avisos_extraidos AS e ON e.aviso_id = v.aviso_id
                WHERE v.timestamp >= ?
                ORDER BY v.timestamp DESC
                """,
                (time.time() - limite * 86_400.0,),
            ).fetchall()
        except sqlite3.Error as error:
            self._logger.error("No se pudo generar el reporte de ventas: %s", error)
            return None

        columnas = ["aviso_id", "cliente", "fecha", "precio_usd", "anulada", "fuente", "ultimo_lote", "veces"]
        try:
            destino.parent.mkdir(parents=True, exist_ok=True)
            with destino.open("w", encoding="utf-8-sig", newline="") as archivo:
                escritor = csv.DictWriter(archivo, fieldnames=columnas, delimiter=";")
                escritor.writeheader()
                for fila in filas:
                    escritor.writerow({clave: fila[clave] for clave in columnas})
        except OSError as error:
            self._logger.error("No se pudo escribir el reporte: %s", error)
            return None
        self._logger.info("Reporte de ventas: %s (%d filas).", destino, len(filas))
        return destino


# =========================================================================== #
#  Singleton de proceso
# =========================================================================== #
_instancia: Optional[RegistroVentas] = None
_lock_instancia = threading.Lock()


def get_registro_ventas(
    settings: Optional[Settings] = None, ruta: Optional[Path | str] = None
) -> RegistroVentas:
    """Devuelve el registro comercial compartido del proceso."""
    global _instancia
    if _instancia is None:
        with _lock_instancia:
            if _instancia is None:
                cfg = settings or get_settings()
                _instancia = RegistroVentas(
                    ruta=ruta or cfg.ventas.ruta,
                    ventana_dias=cfg.ventas.ventana_dias,
                    retencion_dias=cfg.ventas.retencion_dias,
                    politica_error=cfg.ventas.politica_error,
                    bloqueo_venta_permanente=cfg.ventas.bloqueo_venta_permanente,
                )
    return _instancia


def cerrar_registro_ventas() -> None:
    """Cierra el singleton (final del pipeline o en tests)."""
    global _instancia
    with _lock_instancia:
        if _instancia is not None:
            _instancia.cerrar()
            _instancia = None


# =========================================================================== #
#  CLI comercial:  python registro_ventas.py --help
# =========================================================================== #
def construir_parser() -> "argparse.ArgumentParser":
    import argparse

    parser = argparse.ArgumentParser(
        prog="registro_ventas.py",
        description="Registro comercial: exclusividad, ventas y auditoria (ventas.db).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Ejemplos:\n"
            '  python registro_ventas.py --estado "url:6b3fa0c1d2e3f4a5"\n'
            '  python registro_ventas.py --vender "url:6b3fa0c1d2e3f4a5" --cliente "Inmobiliaria del Pacifico" --precio 185000\n'
            "  python registro_ventas.py --resumen\n"
            "  python registro_ventas.py --reporte\n"
            "  python registro_ventas.py --integridad\n"
        ),
    )
    parser.add_argument("--db", metavar="RUTA", default=None, help="Ruta de ventas.db (por defecto, la del .env).")
    parser.add_argument("--ventana-dias", type=float, default=None, help="Dias de exclusividad de las ENTREGAS.")
    parser.add_argument("--vender", metavar="ID_AVISO", help="Registra la venta (bloqueo permanente).")
    parser.add_argument("--cliente", metavar="NOMBRE", help="Cliente que compra el aviso.")
    parser.add_argument("--precio", type=float, default=None, metavar="USD", help="Precio de venta en USD.")
    parser.add_argument("--lote", metavar="TEXTO", default=None, help="Referencia interna (factura, lote, CSV).")
    parser.add_argument("--nota", metavar="TEXTO", default=None, help="Nota libre para la auditoria.")
    parser.add_argument("--forzar", action="store_true", help="Fuerza la venta aunque exista otra activa.")
    parser.add_argument("--anular", metavar="ID_AVISO", help="Anula la(s) venta(s) de un aviso.")
    parser.add_argument("--estado", metavar="ID_AVISO", help="Consulta la exclusividad de un aviso.")
    parser.add_argument("--historial", metavar="ID_AVISO", help="Ficha completa de un aviso.")
    parser.add_argument("--resumen", action="store_true", help="Metricas comerciales.")
    parser.add_argument("--integridad", action="store_true", help="Auditoria de consistencia.")
    parser.add_argument(
        "--reporte", nargs="?", const="", metavar="RUTA", help="Exporta el reporte de ventas a CSV."
    )
    parser.add_argument(
        "--purgar", nargs="?", const=-1.0, type=float, metavar="DIAS", help="Aplica retencion al historial."
    )
    parser.add_argument("--json", action="store_true", help="Salida en JSON (para scripts).")
    parser.add_argument("--autotest", action="store_true", help="Ejecuta la prueba funcional interna.")
    return parser


def _abrir_registro(args: Any) -> RegistroVentas:
    cfg = get_settings()
    return RegistroVentas(
        ruta=args.db or cfg.ventas.ruta,
        ventana_dias=args.ventana_dias or cfg.ventas.ventana_dias,
        retencion_dias=cfg.ventas.retencion_dias,
        politica_error=cfg.ventas.politica_error,
        bloqueo_venta_permanente=cfg.ventas.bloqueo_venta_permanente,
    )


def _mostrar_resumen(datos: Dict[str, Any]) -> None:
    print(f"\n  REGISTRO COMERCIAL  {datos.get('db', '-')}")
    print("  " + "-" * 62)
    filas = [
        ("Ventana de entregas (dias)", datos.get("ventana_dias")),
        ("Venta = bloqueo permanente", "SI" if datos.get("venta_permanente") else "NO"),
        ("Avisos entregados (vigentes)", datos.get("avisos_extraidos_vigentes")),
        ("Avisos entregados (historico)", datos.get("avisos_extraidos_total")),
        ("Ventas activas (permanentes)", datos.get("ventas_activas")),
        ("Ventas en la ventana", datos.get("ventas_en_ventana")),
        ("Clientes unicos", datos.get("clientes_unicos")),
        ("Monto activo (USD)", datos.get("monto_activo_usd")),
        ("Ventas anuladas", datos.get("ventas_anuladas")),
    ]
    for etiqueta, valor in filas:
        print(f"  {etiqueta:32} : {valor}")
    top = datos.get("top_clientes") or []
    if top:
        print("\n  TOP CLIENTES")
        for fila in top:
            print(f"    - {fila['cliente'][:30]:32} {fila['ventas']} ventas  {fila['monto_usd']} USD")
    print()


def _mostrar_estado(decision: DecisionExclusividad) -> None:
    semaforo = "DISPONIBLE" if decision.disponible else "BLOQUEADO"
    print(f"\n  {decision.aviso_id}")
    print(f"  {'-' * 62}")
    print(f"  Estado            : {semaforo} ({decision.estado})")
    print(f"  Motivo            : {decision.motivo}")
    if decision.cliente:
        print(f"  Cliente           : {decision.cliente}")
    if decision.dias is not None:
        print(f"  Antiguedad        : {decision.dias:.1f} dias")
    if decision.dias_restantes is not None:
        print(f"  Se libera en      : {decision.dias_restantes:.1f} dias")
    if decision.clientes_vigentes:
        print(f"  Clientes vigentes : {decision.clientes_vigentes}")
    print()


def _cli(argv: Optional[Sequence[str]] = None) -> int:
    """Interfaz de linea de comandos del area comercial."""
    import json as _json

    from logger import configurar_logging

    argumentos = construir_parser().parse_args(argv)
    if argumentos.autotest:
        return _autotest()

    # CLI limpia: los mensajes informativos del motor no deben ensuciar la salida.
    configurar_logging(nivel="WARNING", consola=True, forzar=True)

    registro = _abrir_registro(argumentos)
    try:
        if argumentos.vender:
            if not argumentos.cliente:
                print("ERROR: --vender requiere --cliente.")
                return 2
            resultado = registro.registrar_venta(
                argumentos.vender,
                argumentos.cliente,
                precio_usd=argumentos.precio,
                lote=argumentos.lote,
                nota=argumentos.nota,
                forzar=argumentos.forzar,
            )
            if argumentos.json:
                print(_json.dumps(resultado.__dict__, ensure_ascii=False, indent=2))
            else:
                print(f"\n  {'OK  ' if resultado.ok else 'RECHAZADO'}: {resultado.motivo}")
                print(f"  Aviso: {resultado.aviso_id} | Cliente: {resultado.cliente or '-'}\n")
            return 0 if resultado.ok else 1

        if argumentos.anular:
            cantidad = registro.anular_venta(argumentos.anular, argumentos.cliente, argumentos.nota)
            print(f"\n  Ventas anuladas de {argumentos.anular}: {cantidad}\n")
            return 0 if cantidad else 1

        if argumentos.estado:
            decision = registro.estado(argumentos.estado)
            if argumentos.json:
                print(_json.dumps(decision.como_dict(), ensure_ascii=False, indent=2))
            else:
                _mostrar_estado(decision)
            return 0 if decision.disponible else 1

        if argumentos.historial:
            datos = registro.historial(argumentos.historial)
            print(_json.dumps(datos, ensure_ascii=False, indent=2, default=str))
            return 0

        if argumentos.reporte is not None:
            ruta = registro.exportar_csv(argumentos.reporte or None)
            print(f"\n  Reporte: {ruta or 'no generado'}\n")
            return 0 if ruta else 1

        if argumentos.purgar is not None:
            dias = None if argumentos.purgar < 0 else argumentos.purgar
            print(f"\n  Retencion aplicada: {registro.purgar_antiguos(dias)}\n")
            return 0

        integridad = registro.validar_integridad()
        if argumentos.integridad:
            print(_json.dumps(integridad, ensure_ascii=False, indent=2, default=str))
            return 0 if integridad["ok"] else 1

        # Sin accion explicita: resumen + estado de integridad.
        resumen = registro.resumen()
        if argumentos.json:
            print(_json.dumps({"resumen": resumen, "integridad": integridad}, ensure_ascii=False, indent=2, default=str))
        else:
            _mostrar_resumen(resumen)
            if integridad["ok"]:
                print("  Integridad comercial: OK (sin conflictos de exclusividad)\n")
            else:
                print(f"  ALERTAS DE INTEGRIDAD ({len(integridad['alertas'])}):")
                for alerta in integridad["alertas"]:
                    print(f"    - [{alerta['tipo']}] {alerta['aviso_id'] or '-'}: {alerta['detalle']}")
                print()
        return 0 if integridad["ok"] else 1
    finally:
        registro.cerrar()


# =========================================================================== #
#  Autotest:  python registro_ventas.py --autotest
# =========================================================================== #
def _autotest() -> int:  # pragma: no cover
    import json
    import tempfile

    from logger import configurar_logging

    configurar_logging(nivel="INFO", carpeta=None)
    tmp = Path(tempfile.mkdtemp(prefix="regventas_")) / "ventas_autotest.db"
    registro = RegistroVentas(ruta=tmp, ventana_dias=30)
    print(f"Autotest sobre {tmp}\n")

    avisos = [
        {"enlace_aviso": "https://portal.pe/aviso/101?utm_source=x", "zona": "Miraflores, Lima",
         "tipo_inmueble": "Departamento", "m2": 120, "precio_usd": 185000, "vendedor": "ACME"},
        {"enlace_aviso": "https://portal.pe/aviso/101", "zona": "Miraflores, Lima",
         "tipo_inmueble": "Departamento", "m2": 120, "precio_usd": 185000, "vendedor": "ACME"},
        {"enlace_aviso": "https://portal.pe/aviso/202", "zona": "Surco, Lima",
         "tipo_inmueble": "Casa", "m2": 210, "precio_usd": 320000, "vendedor": "dueno directo"},
        {"sin_datos": True},
    ]

    print("FASE 1 - filtro (no escribe):")
    resultado = registro.filtrar(avisos, lote="lote_demo")
    print(json.dumps(resultado.resumen(), indent=2, ensure_ascii=False))
    print("IDs aptos:", resultado.ids)

    print("\nFASE 2 - confirmar entrega:")
    print("  registrados:", registro.confirmar_entrega(resultado.ids, lote="lote_demo"))

    print("\nReintento del mismo lote (debe omitirse todo):")
    segundo = registro.filtrar(avisos, lote="lote_demo")
    print("  disponibles:", len(segundo.disponibles), "| omitidos:", len(segundo.omitidos))
    for omitido in segundo.omitidos:
        print("   -", omitido["_exclusividad"]["aviso_id"], "->", omitido["_exclusividad"]["motivo"])

    identificador = resultado.ids[0]
    print(f"\nVenta de {identificador} a INMOBILIARIA DEL PACIFICO:")
    print("  ", registro.registrar_venta(identificador, "Inmobiliaria del Pacifico", 185000, lote="venta_001"))
    print("  repetida (idempotente):", registro.registrar_venta(identificador, "inmobiliaria del pacifico"))
    print("  a otro cliente:", registro.registrar_venta(identificador, "Grupo Andino SA"))
    print("  a otro cliente FORZADA:", registro.registrar_venta(identificador, "Grupo Andino SA", forzar=True))

    # Regla permanente: aunque pasen 400 dias, la venta sigue bloqueando.
    registro._conn.execute("UPDATE avisos_vendidos SET timestamp = ?", (time.time() - 400 * 86400,))
    registro._conn.execute("UPDATE avisos_extraidos SET ultima_ts = ?", (time.time() - 400 * 86400,))
    registro._conn.commit()
    decision = registro.estado(identificador)
    print(f"\nA 400 dias de la venta -> disponible={decision.disponible} ({decision.motivo})")

    print("\nIntegridad:", json.dumps(registro.validar_integridad(), indent=2, ensure_ascii=False))
    print("\nResumen:", json.dumps(registro.resumen(), indent=2, ensure_ascii=False, default=str))
    print("\nHistorial:", json.dumps(registro.historial(identificador), indent=2, ensure_ascii=False, default=str))
    print("\nReporte CSV:", registro.exportar_csv())
    registro.cerrar()
    return 0


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(_cli())
