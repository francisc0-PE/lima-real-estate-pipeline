"""
procesador_historico.py  --  GRUPO 3 (histórico de precios y antigüedad)
=======================================================================

Convierte una lista de avisos en **prospectos con memoria**: para cada
``id_aviso`` guarda su serie de precios en SQLite y calcula dos campos que son
el argumento de venta del producto:

    dias_publicado   -> cuantos dias lleva publicado (desde la PRIMERA vez visto)
    bajadas_precio   -> cuanto ha bajado el precio acumulado, en USD

Regla de negocio
----------------
* Primera vez que se ve un aviso: se registra el precio inicial y arranca el
  contador de ``dias_publicado``.
* En una corrida posterior, si el mismo ``id_aviso`` aparece con un precio
  MENOR, se registra la observacion y se acumula la bajada.
* Si el precio sube, se registra igual (es informacion valiosa: el vendedor
  ajusto al alza) pero NO cuenta como bajada.

Por que una base propia (y no la cache de la clave/valor)
---------------------------------------------------------
La cache del optimizador es un almacen con TTL pensado para *evitar repetir
peticiones*. El historico de precios es una **serie temporal permanente** que
requiere consultas agregadas (primera/ultima/minimo, bajadas, antiguedad). Se
guarda en ``data/historico/precios.db`` con su propio esquema y sin expiracion.

Contrato con el orquestador
---------------------------
    procesar(opt, registros) -> list[dict]     # enriquece y persiste

Cada registro de salida agrega (ademas de los campos del GRUPO 2):

    id_aviso, dias_publicado, bajadas_precio (USD acumulados),
    bajadas_veces, precio_anterior, precio_ultimo, precio_minimo,
    precio_primero, bajada_pct, dias_ultima_bajada, primera_extraccion,
    veces_visto, moneda_original
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from config import Settings, get_settings
from logger import obtener_logger
from optimizador import Optimizador

__all__ = ["HistoricoPrecios", "procesar", "get_historico", "cerrar_historico", "dias_desde"]

log = obtener_logger("procesador_historico")

ESQUEMA_VERSION = 1


def dias_desde(timestamp: Optional[float]) -> Optional[int]:
    """Dias completos transcurridos desde un timestamp (None si no hay dato)."""
    if not timestamp:
        return None
    return max(0, int((time.time() - float(timestamp)) // 86_400))


def _a_timestamp(valor: Any) -> Optional[float]:
    """Convierte ISO/epoch a timestamp; ``None`` si no es interpretable."""
    if valor in (None, ""):
        return None
    if isinstance(valor, (int, float)):
        return float(valor)
    texto = str(valor).strip().replace("Z", "+00:00")
    for formato in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(texto, formato).replace(tzinfo=timezone.utc).timestamp()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(texto).timestamp()
    except ValueError:
        return None


@dataclass
class MetricasHistorico:
    """Contadores de la corrida (para logs y telemetria)."""

    avisos_nuevos: int = 0
    avisos_conocidos: int = 0
    observaciones_precio: int = 0
    bajadas_detectadas: int = 0
    subidas_detectadas: int = 0
    sin_precio: int = 0
    duplicados_lote: int = 0
    errores: int = 0

    def como_dict(self) -> Dict[str, int]:
        return dict(self.__dict__)


class HistoricoPrecios:
    """Serie temporal de precios por aviso (SQLite, WAL, thread-safe)."""

    def __init__(
        self,
        ruta: Optional[Path | str] = None,
        retencion_dias: float = 730.0,
        usar_fecha_publicacion: bool = True,
        logger: Optional[logging.Logger] = None,
    ):
        self._logger = logger or log
        self.retencion_dias = max(30.0, float(retencion_dias))
        self.usar_fecha_publicacion = bool(usar_fecha_publicacion)
        self.ruta = Path(ruta) if ruta else get_settings().portales.historico_ruta
        self.ruta.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.metricas = MetricasHistorico()
        self._conn = sqlite3.connect(str(self.ruta), timeout=30.0, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._crear_esquema()
        self._logger.info("Historico de precios listo en %s.", self.ruta)

    # ------------------------------------------------------------------ #
    #  Ciclo de vida
    # ------------------------------------------------------------------ #
    def __enter__(self) -> "HistoricoPrecios":
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

                -- Un registro por aviso (clave: id_aviso => sin duplicados).
                CREATE TABLE IF NOT EXISTS avisos (
                    id_aviso            TEXT PRIMARY KEY,
                    enlace              TEXT,
                    portal              TEXT,
                    tipo_inmueble       TEXT,
                    zona                TEXT,
                    distrito            TEXT,
                    provincia           TEXT,
                    m2                  REAL,
                    vendedor            TEXT,
                    ruc                 TEXT,
                    razon_social        TEXT,
                    moneda              TEXT,
                    precio_primero      REAL,
                    precio_ultimo       REAL,
                    precio_minimo       REAL,
                    fecha_publicacion   TEXT,
                    primera_ts          REAL NOT NULL,
                    primera_fecha       TEXT NOT NULL,
                    ultima_ts           REAL NOT NULL,
                    ultima_fecha        TEXT NOT NULL,
                    veces_visto         INTEGER NOT NULL DEFAULT 1,
                    bajadas_veces       INTEGER NOT NULL DEFAULT 0,
                    bajada_total_usd    REAL NOT NULL DEFAULT 0,
                    subidas_veces       INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS ix_avisos_ultima ON avisos (ultima_ts);
                CREATE INDEX IF NOT EXISTS ix_avisos_ruc ON avisos (ruc);

                -- Serie de observaciones de precio (una fila por cambio).
                CREATE TABLE IF NOT EXISTS precios (
                    id               INTEGER PRIMARY KEY AUTOINCREMENT,
                    id_aviso         TEXT NOT NULL,
                    fecha            TEXT NOT NULL,
                    ts               REAL NOT NULL,
                    precio_usd       REAL,
                    moneda_original  TEXT,
                    precio_original  REAL,
                    delta_usd        REAL
                );
                CREATE INDEX IF NOT EXISTS ix_precios_aviso ON precios (id_aviso, ts);
                """
            )
            version = self._conn.execute("SELECT valor FROM meta WHERE clave = 'version_esquema'").fetchone()
            if version is None:
                self._conn.execute(
                    "INSERT INTO meta (clave, valor) VALUES ('version_esquema', ?)", (str(ESQUEMA_VERSION),)
                )
            self._conn.commit()

    # ------------------------------------------------------------------ #
    #  Registro
    # ------------------------------------------------------------------ #
    def registrar(self, registros: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Persiste la corrida y devuelve los registros enriquecidos.

        Deduplica por ``id_aviso`` dentro del propio lote (se conserva la ultima
        aparicion, que suele ser la mas completa).
        """
        lote: Dict[str, Dict[str, Any]] = {}
        for registro in registros or []:
            if not isinstance(registro, dict):
                continue
            aviso_id = str(registro.get("id_aviso") or "").strip()
            if not aviso_id:
                self.metricas.errores += 1
                self._logger.debug("Aviso sin id_aviso: no se puede historizar.")
                continue
            if aviso_id in lote:
                self.metricas.duplicados_lote += 1
                # Se fusiona: lo nuevo no debe borrar lo ya capturado.
                previo = lote[aviso_id]
                lote[aviso_id] = {**previo, **{k: v for k, v in registro.items() if v not in (None, "")}}
            else:
                lote[aviso_id] = dict(registro)

        salida: List[Dict[str, Any]] = []
        for aviso_id, registro in lote.items():
            try:
                salida.append(self._registrar_uno(aviso_id, registro))
            except sqlite3.Error as error:
                self.metricas.errores += 1
                self._logger.error("No se pudo historizar %s: %s", aviso_id, error)
                salida.append(registro)
        return salida

    def _registrar_uno(self, aviso_id: str, registro: Dict[str, Any]) -> Dict[str, Any]:
        ahora = time.time()
        fecha_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
        precio = registro.get("precio_usd")
        precio = float(precio) if isinstance(precio, (int, float)) else None
        if precio is None:
            self.metricas.sin_precio += 1

        with self._lock:
            fila = self._conn.execute("SELECT * FROM avisos WHERE id_aviso = ?", (aviso_id,)).fetchone()

            if fila is None:
                # --- Primera vez que vemos el aviso ---
                primera_ts = ahora
                fecha_publicacion = registro.get("fecha_publicacion")
                if self.usar_fecha_publicacion and fecha_publicacion:
                    detectado = _a_timestamp(fecha_publicacion)
                    if detectado and detectado <= ahora:
                        primera_ts = detectado  # afinidad: la antiguedad real del aviso
                with self._conn:
                    self._conn.execute(
                        """
                        INSERT INTO avisos (
                            id_aviso, enlace, portal, tipo_inmueble, zona, distrito, provincia, m2, vendedor,
                            ruc, razon_social, moneda, precio_primero, precio_ultimo, precio_minimo,
                            fecha_publicacion, primera_ts, primera_fecha, ultima_ts, ultima_fecha, veces_visto
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)
                        """,
                        (
                            aviso_id,
                            registro.get("enlace_aviso"),
                            registro.get("portal"),
                            registro.get("tipo_inmueble"),
                            registro.get("zona"),
                            registro.get("distrito"),
                            registro.get("provincia"),
                            registro.get("m2"),
                            registro.get("vendedor"),
                            registro.get("ruc"),
                            registro.get("razon_social"),
                            registro.get("moneda_original"),
                            precio,
                            precio,
                            precio,
                            fecha_publicacion,
                            primera_ts,
                            datetime.fromtimestamp(primera_ts, timezone.utc).isoformat(timespec="seconds"),
                            ahora,
                            fecha_iso,
                        ),
                    )
                    self._insertar_precio(aviso_id, precio, registro, delta=None, ts=ahora, fecha=fecha_iso)
                self.metricas.avisos_nuevos += 1
                self.metricas.observaciones_precio += 1
                return self._salida(
                    aviso_id,
                    registro,
                    {
                        "precio_primero": precio,
                        "precio_ultimo": precio,
                        "precio_minimo": precio,
                        "primera_ts": primera_ts,
                        "veces_visto": 1,
                        "bajadas_veces": 0,
                        "bajada_total_usd": 0.0,
                        "ultima_bajada_ts": None,
                        "precio_anterior": None,
                    },
                )

            # --- Aviso ya conocido ---
            self.metricas.avisos_conocidos += 1
            precio_ultimo = fila["precio_ultimo"]
            precio_minimo = fila["precio_minimo"]
            bajadas = int(fila["bajadas_veces"] or 0)
            bajada_total = float(fila["bajada_total_usd"] or 0)
            subidas = int(fila["subidas_veces"] or 0)
            ultima_bajada_ts = self._ultima_bajada(aviso_id)
            delta: Optional[float] = None

            if precio is not None and precio_ultimo is not None and abs(precio - float(precio_ultimo)) > 0.5:
                delta = round(precio - float(precio_ultimo), 2)
                self._insertar_precio(aviso_id, precio, registro, delta=delta, ts=ahora, fecha=fecha_iso)
                self.metricas.observaciones_precio += 1
                if delta < 0:
                    bajadas += 1
                    bajada_total = round(bajada_total + abs(delta), 2)
                    precio_minimo = min(float(precio_minimo or precio), precio)
                    ultima_bajada_ts = ahora
                    self.metricas.bajadas_detectadas += 1
                    self._logger.info(
                        "BAJADA en %s: %.0f -> %.0f USD (%.0f USD menos).",
                        aviso_id,
                        float(precio_ultimo),
                        precio,
                        abs(delta),
                    )
                else:
                    subidas += 1
                    self.metricas.subidas_detectadas += 1
                    self._logger.info("Subida en %s: +%.0f USD.", aviso_id, delta)
            elif precio is not None and precio_ultimo is None:
                # Primera vez que conseguimos el precio de un aviso ya conocido.
                precio_minimo = precio
                self._insertar_precio(aviso_id, precio, registro, delta=None, ts=ahora, fecha=fecha_iso)
                self.metricas.observaciones_precio += 1

            with self._conn:
                self._conn.execute(
                    """
                    UPDATE avisos SET
                        ultima_ts = ?, ultima_fecha = ?, veces_visto = veces_visto + 1,
                        precio_ultimo = COALESCE(?, precio_ultimo),
                        precio_minimo = COALESCE(?, precio_minimo),
                        bajadas_veces = ?, bajada_total_usd = ?, subidas_veces = ?,
                        m2 = COALESCE(?, m2), zona = COALESCE(?, zona), vendedor = COALESCE(?, vendedor),
                        ruc = COALESCE(?, ruc), razon_social = COALESCE(?, razon_social),
                        distrito = COALESCE(?, distrito), provincia = COALESCE(?, provincia),
                        tipo_inmueble = COALESCE(?, tipo_inmueble)
                    WHERE id_aviso = ?
                    """,
                    (
                        ahora,
                        fecha_iso,
                        precio,
                        precio if precio is not None else precio_minimo,
                        bajadas,
                        bajada_total,
                        subidas,
                        registro.get("m2"),
                        registro.get("zona"),
                        registro.get("vendedor"),
                        registro.get("ruc"),
                        registro.get("razon_social"),
                        registro.get("distrito"),
                        registro.get("provincia"),
                        registro.get("tipo_inmueble"),
                        aviso_id,
                    ),
                )

            return self._salida(
                aviso_id,
                registro,
                {
                    "precio_primero": fila["precio_primero"],
                    "precio_ultimo": precio if precio is not None else precio_ultimo,
                    "precio_minimo": precio_minimo,
                    "primera_ts": fila["primera_ts"],
                    "veces_visto": int(fila["veces_visto"] or 1) + 1,
                    "bajadas_veces": bajadas,
                    "bajada_total_usd": bajada_total,
                    "ultima_bajada_ts": ultima_bajada_ts,
                    "precio_anterior": float(precio_ultimo) if precio_ultimo is not None else None,
                },
            )

    def _insertar_precio(
        self,
        aviso_id: str,
        precio: Optional[float],
        registro: Dict[str, Any],
        delta: Optional[float],
        ts: float,
        fecha: str,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO precios (id_aviso, fecha, ts, precio_usd, moneda_original, precio_original, delta_usd)
            VALUES (?,?,?,?,?,?,?)
            """,
            (
                aviso_id,
                fecha,
                ts,
                precio,
                registro.get("moneda_original"),
                registro.get("precio_original"),
                delta,
            ),
        )

    def _ultima_bajada(self, aviso_id: str) -> Optional[float]:
        fila = self._conn.execute(
            "SELECT MAX(ts) AS ts FROM precios WHERE id_aviso = ? AND delta_usd < 0", (aviso_id,)
        ).fetchone()
        return float(fila["ts"]) if fila and fila["ts"] else None

    @staticmethod
    def _salida(aviso_id: str, registro: Dict[str, Any], memoria: Dict[str, Any]) -> Dict[str, Any]:
        """Une el aviso con su memoria historica (GRUPO 3)."""
        precio_primero = memoria.get("precio_primero")
        precio_ultimo = memoria.get("precio_ultimo")
        bajada_total = round(float(memoria.get("bajada_total_usd") or 0), 2)
        bajada_pct = (
            round(100.0 * bajada_total / float(precio_primero), 2)
            if bajada_total and precio_primero
            else 0.0
        )
        salida = dict(registro)
        salida.update(
            {
                "id_aviso": aviso_id,
                "dias_publicado": dias_desde(memoria.get("primera_ts")),
                "bajadas_precio": bajada_total,               # USD acumulados (campo del contrato)
                "bajadas_veces": int(memoria.get("bajadas_veces") or 0),
                "bajada_pct": bajada_pct,
                "precio_primero": precio_primero,
                "precio_ultimo": precio_ultimo,
                "precio_minimo": memoria.get("precio_minimo"),
                "precio_anterior": memoria.get("precio_anterior"),
                "dias_ultima_bajada": dias_desde(memoria.get("ultima_bajada_ts")),
                "veces_visto": int(memoria.get("veces_visto") or 1),
                "primera_extraccion": datetime.fromtimestamp(
                    float(memoria["primera_ts"]), timezone.utc
                ).isoformat(timespec="seconds")
                if memoria.get("primera_ts")
                else None,
            }
        )
        return salida

    # ------------------------------------------------------------------ #
    #  Consultas y mantenimiento
    # ------------------------------------------------------------------ #
    def historial(self, aviso_id: str) -> Dict[str, Any]:
        """Ficha completa de un aviso con su serie de precios."""
        clave = str(aviso_id or "").strip()
        with self._lock:
            fila = self._conn.execute("SELECT * FROM avisos WHERE id_aviso = ?", (clave,)).fetchone()
            serie = self._conn.execute(
                "SELECT fecha, precio_usd, moneda_original, precio_original, delta_usd FROM precios "
                "WHERE id_aviso = ? ORDER BY ts",
                (clave,),
            ).fetchall()
        if fila is None:
            return {"id_aviso": clave, "existe": False}
        return {
            "id_aviso": clave,
            "existe": True,
            "aviso": dict(fila),
            "serie": [dict(punto) for punto in serie],
            "dias_publicado": dias_desde(fila["primera_ts"]),
            "bajadas_precio": round(float(fila["bajada_total_usd"] or 0), 2),
        }

    def resumen(self) -> Dict[str, Any]:
        """Metricas agregadas del historico (para telemetria y reportes)."""
        with self._lock:
            try:
                total = self._conn.execute("SELECT COUNT(*) AS n FROM avisos").fetchone()["n"]
                con_precio = self._conn.execute("SELECT COUNT(*) AS n FROM avisos WHERE precio_ultimo IS NOT NULL").fetchone()["n"]
                con_bajada = self._conn.execute("SELECT COUNT(*) AS n FROM avisos WHERE bajadas_veces > 0").fetchone()["n"]
                agregados = self._conn.execute(
                    """
                    SELECT COALESCE(SUM(bajada_total_usd), 0) AS bajado,
                           COALESCE(AVG(julianday('now') - julianday(primera_fecha)), 0) AS dias_promedio,
                           COALESCE(MAX(veces_visto), 0) AS max_veces
                    FROM avisos
                    """
                ).fetchone()
                observaciones = self._conn.execute("SELECT COUNT(*) AS n FROM precios").fetchone()["n"]
                top = self._conn.execute(
                    """
                    SELECT id_aviso, zona, precio_primero, precio_ultimo, bajada_total_usd
                    FROM avisos WHERE bajadas_veces > 0
                    ORDER BY bajada_total_usd DESC LIMIT 5
                    """
                ).fetchall()
            except sqlite3.Error as error:
                return {"error": str(error), "db": str(self.ruta)}
        return {
            "db": str(self.ruta),
            "avisos_total": total,
            "avisos_con_precio": con_precio,
            "avisos_con_bajada": con_bajada,
            "bajado_total_usd": round(float(agregados["bajado"] or 0), 2),
            "dias_publicado_promedio": round(float(agregados["dias_promedio"] or 0), 1),
            "observaciones_precio": observaciones,
            "max_veces_visto": int(agregados["max_veces"] or 0),
            "top_bajadas": [dict(fila) for fila in top],
            "metricas_proceso": self.metricas.como_dict(),
        }

    def purgar(self, dias: Optional[float] = None) -> Dict[str, int]:
        """Aplica retencion al historico (avisos sin verse en N dias)."""
        limite = float(dias if dias is not None else self.retencion_dias)
        corte = time.time() - limite * 86_400.0
        eliminados = {"avisos": 0, "precios": 0}
        with self._lock:
            try:
                with self._conn:
                    eliminados["precios"] = self._conn.execute(
                        "DELETE FROM precios WHERE id_aviso IN (SELECT id_aviso FROM avisos WHERE ultima_ts < ?)",
                        (corte,),
                    ).rowcount
                    eliminados["avisos"] = self._conn.execute(
                        "DELETE FROM avisos WHERE ultima_ts < ?", (corte,)
                    ).rowcount
            except sqlite3.Error as error:
                self._logger.error("No se pudo purgar el historico: %s", error)
        if any(eliminados.values()):
            self._logger.info("Retencion del historico (>%.0f dias): %s", limite, eliminados)
        return eliminados


# =========================================================================== #
#  Singleton y contrato del orquestador
# =========================================================================== #
_instancias: Dict[str, HistoricoPrecios] = {}
_lock_instancia = threading.Lock()


def get_historico(settings: Optional[Settings] = None, ruta: Optional[Path | str] = None) -> HistoricoPrecios:
    """Devuelve el historico para una ruta (una instancia por base).

    Se indexa por ruta para que una misma sesion pueda usar bases distintas
    (por ejemplo ``precios.db`` en produccion y ``precios_demo.db`` en --demo)
    sin que la primera cree el singleton de la segunda.
    """
    cfg = settings or get_settings()
    clave = str(ruta or cfg.portales.historico_ruta)
    with _lock_instancia:
        if clave not in _instancias:
            _instancias[clave] = HistoricoPrecios(
                ruta=clave,
                retencion_dias=cfg.portales.historico_retencion_dias,
                usar_fecha_publicacion=cfg.portales.usar_fecha_publicacion,
            )
        return _instancias[clave]


def cerrar_historico() -> None:
    """Cierra todas las bases de historico abiertas en el proceso."""
    with _lock_instancia:
        for instancia in _instancias.values():
            instancia.cerrar()
        _instancias.clear()


def procesar(
    opt: Optimizador,
    registros: Sequence[Dict[str, Any]],
    ruta: Optional[Path | str] = None,
) -> List[Dict[str, Any]]:
    """Punto de entrada que invoca ``main.py`` (nunca lanza excepcion).

    ``ruta`` permite usar una base distinta (p.ej. la de --demo) sin tocar la
    configuracion global.
    """
    try:
        historico = get_historico(opt.settings if opt else None, ruta=ruta)
        return historico.registrar(registros)
    except Exception as error:
        log.error("Procesador historico fallo: %s", error, exc_info=True)
        return [dict(registro) for registro in registros or [] if isinstance(registro, dict)]


# =========================================================================== #
#  Uso directo:  python procesador_historico.py --historial <id_aviso>
# =========================================================================== #
def main(argv: Optional[Sequence[str]] = None) -> int:
    import argparse
    import json

    from logger import configurar_logging

    parser = argparse.ArgumentParser(description="Historico de precios (GRUPO 3).")
    parser.add_argument("--historial", metavar="ID_AVISO", help="Serie de precios de un aviso.")
    parser.add_argument("--resumen", action="store_true", help="Metricas agregadas.")
    parser.add_argument("--purgar", nargs="?", const=-1.0, type=float, metavar="DIAS")
    argumentos = parser.parse_args(argv)

    settings = get_settings()
    configurar_logging(nivel=settings.logs.nivel, carpeta=settings.logs.carpeta, archivo="historico.log")
    historico = get_historico(settings)
    try:
        if argumentos.historial:
            print(json.dumps(historico.historial(argumentos.historial), ensure_ascii=False, indent=2, default=str))
        elif argumentos.purgar is not None:
            print(f"Retencion aplicada: {historico.purgar(None if argumentos.purgar < 0 else argumentos.purgar)}")
        else:
            print(json.dumps(historico.resumen(), ensure_ascii=False, indent=2, default=str))
    finally:
        cerrar_historico()
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
