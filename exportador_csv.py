"""
exportador_csv.py  --  Salida final para venta
==============================================

Convierte los prospectos A1 en el **archivo que se vende**: un CSV con el orden
de columnas ESTRICTO (``id_aviso`` primero, ``senal`` al final) y la columna
``senal`` construida con logica comercial.

Contrato del archivo (17 columnas)
----------------------------------
    id_aviso | GRUPO 1 (7) | GRUPO 2 (6) | GRUPO 3 (2) | senal

    GRUPO 1: ruc, razon_social, estado_sunat, actividad, distrito_fiscal,
             correo_corporativo, web
    GRUPO 2: tipo_inmueble, zona, m2, precio_usd, vendedor, enlace_aviso
    GRUPO 3: dias_publicado, bajadas_precio
    Comercial: senal

Con ``EXPORT_COLUMNAS_EXTRA=true`` se anaden al final columnas de auditoria
(``distrito``, ``provincia``, ``condicion``, ``ciiu``, ``precio_anterior``...)
sin alterar el orden de las 17.

La columna ``senal``
--------------------
Es el argumento de venta. Se compone con los datos auxiliares del historico y
del vendedor, en orden de impacto comercial:

    [OPORTUNIDAD: ] <bajada> (<veces>, -<pct>%, <recencia>),
    <precio en minimo historico>, <antiguedad>, <USD/m2>,
    <trato directo | empresa formal>

Ejemplos reales que produce:

    "OPORTUNIDAD: Bajo $25.6k (2 veces, -8%) hace 1 semana, publicado hace 1 mes,
     USD 1,400/m2, trato directo, sin comision"

    "Publicado hace 3 dias, USD 2,100/m2, empresa formal (INVERSIONES LIMA NORTE)"

    "OPORTUNIDAD: Bajo $15k hace 2 semanas, precio en su minimo historico,
     publicado hace 30 dias, USD 1,542/m2, trato directo"

Garantias
---------
* Nunca lanza: si falla la escritura, devuelve ``None`` y el pipeline continua.
* ``id_aviso`` se valida en cada fila (sin el, la venta no se puede registrar).
* Los nulos van como celda vacia (nunca ``None`` ni ``nan``).
* Los numeros se escriben en formato legible para Excel (sin notacion cientifica).
"""

from __future__ import annotations

import csv
import json
import logging
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from config import COLUMNAS_SALIDA, get_settings, columnas_salida
from logger import obtener_logger
from optimizador import Optimizador

__all__ = ["exportar", "generar_senal", "preparar_filas", "ExportadorCSV", "ultimas_metricas"]

log = obtener_logger("exportador_csv")

CAMPOS_GRUPO1 = ("ruc", "razon_social", "estado_sunat", "actividad", "distrito_fiscal",
                 "correo_corporativo", "web")
CAMPOS_GRUPO2 = ("tipo_inmueble", "zona", "m2", "precio_usd", "vendedor", "enlace_aviso")
CAMPOS_GRUPO3 = ("dias_publicado", "bajadas_precio")


# =========================================================================== #
#  Utilidades de formato
# =========================================================================== #
def _sin_acentos(valor: Any) -> str:
    plano = unicodedata.normalize("NFKD", str(valor or ""))
    return "".join(caracter for caracter in plano if not unicodedata.combining(caracter)).lower()


def abreviar_usd(valor: Optional[float]) -> str:
    """Abrevia una cifra en USD para el gancho comercial: 15000 -> ``$15k``."""
    if valor is None:
        return ""
    try:
        numero = float(valor)
    except (TypeError, ValueError):
        return ""
    if abs(numero) >= 1_000_000:
        return f"${numero / 1_000_000:.1f}M".replace(".0M", "M")
    if abs(numero) >= 1_000:
        texto = f"{numero / 1_000:.1f}"
        return "$" + texto.rstrip("0").rstrip(".") + "k"
    return f"${numero:.0f}"


def es_vendedor_directo(vendedor: Any) -> bool:
    """¿El aviso lo publica el dueño (sin intermediario)?"""
    plano = _sin_acentos(vendedor)
    if not plano:
        return False
    pistas = ("dueno directo", "propietario directo", "particular", "sin comision",
              "directo del propietario", "dueña directo", "propietaria directa")
    return any(pista in plano for pista in pistas)


# =========================================================================== #
#  Copywriting de la columna ``senal`` (gatillos mentales de inversion)
# =========================================================================== #
#  Titulares por prioridad: cuanto mas fuerte la senal, mas agresivo el gancho.
TITULARES: Dict[str, Tuple[str, str]] = {
    "inversion": ("🔥", "OPORTUNIDAD DE INVERSIÓN"),
    "negociable": ("📉", "NEGOCIABLE"),
    "tiempo": ("⏳", "OPORTUNIDAD POR TIEMPO"),
    "nuevo": ("🆕", "RECIÉN PUBLICADO"),
    "directo": ("🤝", "TRATO DIRECTO"),
    "formal": ("🏢", "EMPRESA FORMAL"),
}

CIERRES: Dict[str, str] = {
    "inversion": "Ideal para cierre rápido",
    "negociable": "Margen de negociación disponible",
    "tiempo": "Margen de negociación disponible",
    "nuevo": "Entrada temprana al mercado",
    "directo": "Sin comisión de intermediarios",
    "formal": "Documentación en regla",
}


def _antiguedad(dias: Optional[Any]) -> str:
    """Frase de urgencia: cuanto lleva la propiedad en el mercado."""
    if dias in (None, ""):
        return ""
    try:
        numero = int(float(dias))
    except (TypeError, ValueError):
        return ""
    if numero <= 1:
        return "recién publicado"
    if numero < 7:
        return f"{numero} días en el mercado"
    if numero < 45:
        semanas = max(1, numero // 7)
        return f"{semanas} {'semana' if semanas == 1 else 'semanas'} en el mercado"
    if numero < 365:
        meses = max(1, numero // 30)
        return f"{meses} {'mes' if meses == 1 else 'meses'} en el mercado ({numero} días)"
    anios = max(1, numero // 365)
    return f"{anios} {'año' if anios == 1 else 'años'} en el mercado ({numero} días)"


def _recencia(dias: Optional[Any]) -> str:
    """Cuando ocurrio la ultima bajada ('hace 3 días', 'hace 2 semanas')."""
    if dias in (None, ""):
        return ""
    try:
        numero = int(float(dias))
    except (TypeError, ValueError):
        return ""
    if numero <= 0:
        return "esta semana"
    if numero == 1:
        return "hace 1 día"
    if numero < 7:
        return f"hace {numero} días"
    if numero < 45:
        semanas = max(1, numero // 7)
        return f"hace {semanas} {'semana' if semanas == 1 else 'semanas'}"
    meses = max(1, numero // 30)
    return f"hace {meses} {'mes' if meses == 1 else 'meses'}"


def _precio_por_m2(registro: Dict[str, Any]) -> str:
    """USD por m2 (metrica que buscan los inversores)."""
    precio = registro.get("precio_usd")
    m2 = registro.get("m2")
    try:
        precio = float(precio)
        m2 = float(m2)
    except (TypeError, ValueError):
        return ""
    if precio <= 0 or m2 <= 0:
        return ""
    return f"USD {precio / m2:,.0f}/m²".replace(",", " ")


def generar_senal(
    registro: Dict[str, Any],
    minimo_bajada: float = 1000.0,
    emojis: bool = True,
    umbral_inversion_pct: float = 10.0,
    umbral_tiempo_dias: int = 45,
) -> str:
    """Redacta el argumento de venta B2B de la columna ``senal``.

    Estructura: ``{emoji} TITULAR: argumentos. CIERRE``

    Gatillos, por prioridad:
      1. ``🔥 OPORTUNIDAD DE INVERSIÓN``  rebaja >=10% (o >=20k USD)
      2. ``📉 NEGOCIABLE``                cualquier rebaja registrada
      3. ``⏳ OPORTUNIDAD POR TIEMPO``    mucho tiempo en el mercado
      4. ``🆕 RECIÉN PUBLICADO``          recien salido
      5. ``🤝 TRATO DIRECTO``             vendedor particular (sin comision)
      6. ``🏢 EMPRESA FORMAL``            empresa verificada en SUNAT

    Honestidad del dato: la rebaja se expresa **"por debajo de su precio de
    lista"** (lo que SI sabemos por nuestra serie historica). No se afirma
    "por debajo del mercado" porque no tenemos comparables verificados.
    """
    if not isinstance(registro, dict):
        return ""
    try:
        bajada = float(registro.get("bajadas_precio") or 0)
    except (TypeError, ValueError):
        bajada = 0.0
    try:
        veces = int(float(registro.get("bajadas_veces") or 0))
    except (TypeError, ValueError):
        veces = 0
    try:
        porcentaje = float(registro.get("bajada_pct") or 0)
    except (TypeError, ValueError):
        porcentaje = 0.0
    try:
        dias = int(float(registro.get("dias_publicado"))) if registro.get("dias_publicado") not in (None, "") else None
    except (TypeError, ValueError):
        dias = None

    precio = registro.get("precio_usd")
    minimo = registro.get("precio_minimo")
    vendedor = registro.get("vendedor")
    razon_social = str(registro.get("razon_social") or "").strip()
    estado = str(registro.get("estado_sunat") or "").strip().upper()
    directo = es_vendedor_directo(vendedor)
    empresa_formal = bool(razon_social and estado == "ACTIVO")
    rebaja = bajada >= max(1.0, minimo_bajada) and veces > 0

    # ----------------------------- Titular ---------------------------- #
    titular = ""
    if rebaja and (porcentaje >= umbral_inversion_pct or bajada >= 20_000):
        titular = "inversion"
    elif rebaja:
        titular = "negociable"
    elif dias is not None and dias >= umbral_tiempo_dias:
        titular = "tiempo"
    elif dias is not None and dias <= 7:
        titular = "nuevo"
    elif directo:
        titular = "directo"
    elif empresa_formal:
        titular = "formal"

    # --------------------------- Argumentos --------------------------- #
    partes: List[str] = []
    if rebaja:
        recencia = _recencia(registro.get("dias_ultima_bajada"))
        if porcentaje >= 3:
            detalle = f"Precio un {porcentaje:.0f}% por debajo de su precio de lista"
            detalle += f" (Bajó {abreviar_usd(bajada)}"
            detalle += f" en {veces} rebajas)" if veces > 1 else ")"
        else:
            detalle = f"Bajó {abreviar_usd(bajada)}"
            if veces > 1:
                detalle += f" ({veces} rebajas)"
        if recencia:
            detalle += f" {recencia}"
        partes.append(detalle)

    try:
        if precio is not None and minimo is not None and float(precio) <= float(minimo) + 1 and veces:
            partes.append("En su precio más bajo histórico")
    except (TypeError, ValueError):
        pass

    antiguedad = _antiguedad(dias)
    if antiguedad:
        partes.append(antiguedad[0].upper() + antiguedad[1:])

    por_m2 = _precio_por_m2(registro)
    if por_m2:
        partes.append(por_m2)

    if directo:
        partes.append("Dueño directo, sin comisión")
    elif empresa_formal:
        partes.append(f"Empresa formal verificada en SUNAT ({razon_social[:30]})")
    elif razon_social:
        partes.append(f"Vendedor: {razon_social[:30]}")

    if not partes:
        return ""

    # ----------------------------- Cierre ----------------------------- #
    cierre = CIERRES.get(titular, "")
    emoji, etiqueta = TITULARES.get(titular, ("", ""))
    prefijo = f"{emoji} " if (emojis and emoji) else ""
    encabezado = f"{prefijo}{etiqueta}: " if etiqueta else ""
    tope = 280

    def _armar(piezas: List[str]) -> str:
        texto = f"{encabezado}{'. '.join(piezas)}"
        if cierre:
            texto += f". {cierre}"
        return texto

    # Si no cabe, se sacrifican los datos accesorios (m2 y luego antiguedad)
    # ANTES que el cierre comercial: el gancho de venta es lo que vende.
    accesorios = [pieza for pieza in (por_m2, antiguedad, por_m2) if pieza]
    while len(_armar(partes)) > tope and accesorios:
        quitado = accesorios.pop(0)
        if quitado in partes:
            partes.remove(quitado)
    texto = _armar(partes)
    if len(texto) > tope:  # ultimo recurso: recorte limpio
        texto = texto[: tope - 1].rstrip(" .,;") + "."
    return texto.strip()


# =========================================================================== #
#  Preparacion de filas
# =========================================================================== #
def _celda(valor: Any, truncar: int = 300) -> Any:
    """Normaliza un valor para el CSV: nulos -> celda vacia, texto acotado."""
    if valor is None:
        return ""
    if isinstance(valor, bool):
        return "si" if valor else "no"
    if isinstance(valor, (int, float)):
        return valor
    if isinstance(valor, (list, tuple, set)):
        return ", ".join(str(elemento) for elemento in valor)[:truncar]
    if isinstance(valor, dict):
        return json.dumps(valor, ensure_ascii=False)[:truncar]
    texto = re.sub(r"[\r\n\t]+", " ", str(valor)).strip()
    return texto[:truncar]


def _numero(valor: Any, decimales: int = 2) -> Any:
    """Numeros legibles para Excel (sin notacion cientifica ni .0 sobrante)."""
    if valor in (None, ""):
        return ""
    try:
        numero = float(valor)
    except (TypeError, ValueError):
        return _celda(valor)
    if numero == int(numero):
        return int(numero)
    return round(numero, decimales)


def preparar_filas(
    registros: Sequence[Dict[str, Any]],
    columnas: Sequence[str] = COLUMNAS_SALIDA,
    minimo_bajada: float = 1000.0,
    truncar: int = 300,
    emojis: bool = True,
    umbral_inversion_pct: float = 10.0,
    umbral_tiempo_dias: int = 45,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Construye las filas en el orden ESTRICTO del contrato.

    Devuelve ``(filas, metricas)``. Las metricas alimentan el resumen comercial.
    """
    filas: List[Dict[str, Any]] = []
    metricas = {
        "filas": 0,
        "sin_id": 0,
        "con_oportunidad": 0,
        "con_bajada": 0,
        "dueno_directo": 0,
        "empresa_formal": 0,
        "con_correo": 0,
        "sin_precio": 0,
        "titulares": {},
        "valor_cartera_usd": 0.0,
        "m2_totales": 0.0,
        "antiguedad_total": 0,
        "con_antiguedad": 0,
        "distritos": {},
        "columnas": list(columnas),
    }

    for registro in registros or []:
        if not isinstance(registro, dict):
            continue
        senal = ""
        fila: Dict[str, Any] = {}
        for columna in columnas:
            if columna == "senal":
                senal = generar_senal(
                    registro,
                    minimo_bajada=minimo_bajada,
                    emojis=emojis,
                    umbral_inversion_pct=umbral_inversion_pct,
                    umbral_tiempo_dias=umbral_tiempo_dias,
                )
                fila[columna] = senal
            elif columna in {"m2", "precio_usd", "bajadas_precio", "precio_anterior", "precio_minimo"}:
                fila[columna] = _numero(registro.get(columna))
            elif columna in {"dias_publicado", "veces_visto", "bajadas_veces"}:
                fila[columna] = _numero(registro.get(columna), decimales=0)
            elif columna == "empresa_vinculada":
                fila[columna] = "si" if registro.get("empresa_vinculada") else "no"
            else:
                fila[columna] = _celda(registro.get(columna), truncar)
        filas.append(fila)

        # --- Metricas comerciales ---
        metricas["filas"] += 1
        if not fila.get("id_aviso"):
            metricas["sin_id"] += 1
        # Se cuenta por el titular de la senal (gatillo mental), no por texto suelto.
        titular = ""
        for clave, (_emoji, etiqueta) in TITULARES.items():
            if etiqueta and etiqueta in senal:
                titular = clave
                break
        if titular:
            metricas["titulares"][titular] = metricas["titulares"].get(titular, 0) + 1
        if titular in {"inversion", "negociable", "tiempo"}:
            metricas["con_oportunidad"] += 1
        if (registro.get("bajadas_veces") or 0) and (registro.get("bajadas_precio") or 0) > 0:
            metricas["con_bajada"] += 1
        if es_vendedor_directo(registro.get("vendedor")):
            metricas["dueno_directo"] += 1
        elif str(registro.get("estado_sunat") or "").upper() == "ACTIVO":
            metricas["empresa_formal"] += 1
        if registro.get("correo_corporativo"):
            metricas["con_correo"] += 1
        try:
            metricas["valor_cartera_usd"] += float(registro.get("precio_usd") or 0)
        except (TypeError, ValueError):
            metricas["sin_precio"] += 1
        try:
            metricas["m2_totales"] += float(registro.get("m2") or 0)
        except (TypeError, ValueError):
            pass
        dias = registro.get("dias_publicado")
        if dias not in (None, ""):
            try:
                metricas["antiguedad_total"] += int(float(dias))
                metricas["con_antiguedad"] += 1
            except (TypeError, ValueError):
                pass
        distrito = str(registro.get("distrito") or registro.get("distrito_fiscal") or "").strip()
        if distrito:
            metricas["distritos"][distrito] = metricas["distritos"].get(distrito, 0) + 1

    metricas["valor_cartera_usd"] = round(metricas["valor_cartera_usd"], 2)
    metricas["m2_totales"] = round(metricas["m2_totales"], 2)
    metricas["antiguedad_promedio"] = (
        round(metricas["antiguedad_total"] / metricas["con_antiguedad"], 1)
        if metricas["con_antiguedad"]
        else 0.0
    )
    metricas["ticket_promedio_usd"] = (
        round(metricas["valor_cartera_usd"] / metricas["filas"], 2) if metricas["filas"] else 0.0
    )
    metricas["tasa_oportunidad_pct"] = (
        round(100.0 * metricas["con_oportunidad"] / metricas["filas"], 1) if metricas["filas"] else 0.0
    )
    metricas["top_distritos"] = sorted(
        metricas["distritos"].items(), key=lambda par: par[1], reverse=True
    )[:5]
    return filas, metricas


# =========================================================================== #
#  Exportador
# =========================================================================== #
@dataclass
class MetricasExportacion:
    """Resultado de la exportacion (alimenta el resumen comercial del dashboard)."""

    archivo: Optional[str] = None
    filas: int = 0
    columnas: int = 0
    con_oportunidad: int = 0
    con_bajada: int = 0
    dueno_directo: int = 0
    empresa_formal: int = 0
    con_correo: int = 0
    sin_id: int = 0
    valor_cartera_usd: float = 0.0
    ticket_promedio_usd: float = 0.0
    m2_totales: float = 0.0
    antiguedad_promedio: float = 0.0
    tasa_oportunidad_pct: float = 0.0
    top_distritos: List[Tuple[str, int]] = field(default_factory=list)
    bytes_escritos: int = 0
    reporte: Optional[str] = None

    def como_dict(self) -> Dict[str, Any]:
        datos = dict(self.__dict__)
        datos["top_distritos"] = [{"distrito": d, "avisos": n} for d, n in self.top_distritos]
        return datos


_METRICAS: MetricasExportacion = MetricasExportacion()


def ultimas_metricas() -> Dict[str, Any]:
    """Metricas de la ultima exportacion (para el resumen del orquestador)."""
    return _METRICAS.como_dict()


class ExportadorCSV:
    """Escribe el CSV final respetando el contrato de columnas."""

    def __init__(self, settings: Any = None, logger: Optional[logging.Logger] = None):
        self.settings = settings or get_settings()
        self.config = self.settings.exportacion
        self._logger = logger or log

    # ------------------------------------------------------------------ #
    def ruta_destino(self, ruta: Optional[Path | str] = None, demo: bool = False) -> Path:
        """Ruta del archivo final (los demo llevan sufijo para no venderse).

        Si ``EXPORT_NOMBRE_ARCHIVO`` esta definido se usa ese nombre fijo (sin
        marca de tiempo): es lo que produce la ``Muestra_Premium_Surco.csv``.
        """
        if ruta:
            return Path(ruta)
        sufijo = "_demo" if demo else ""
        if self.config.nombre_archivo:
            # Se admite que el nombre venga con extension (no se duplica el .csv).
            base = self.config.nombre_archivo.strip()
            if base.lower().endswith(".csv"):
                base = base[:-4]
            return self.config.carpeta / f"{base}{sufijo}.csv"
        momento = datetime.now().strftime("%Y%m%d_%H%M%S")
        return self.config.carpeta / f"{self.config.prefijo}{sufijo}_{momento}.csv"

    def exportar(
        self,
        registros: Sequence[Dict[str, Any]],
        ruta: Optional[Path | str] = None,
        demo: bool = False,
    ) -> Optional[Path]:
        """Escribe el CSV final. Devuelve la ruta o ``None`` si no se pudo."""
        global _METRICAS
        columnas = columnas_salida(self.config.incluir_extras)
        if not columnas or columnas[0] != "id_aviso":
            self._logger.error("Contrato invalido: 'id_aviso' debe ser la primera columna.")
            return None

        filas, metricas = preparar_filas(
            registros,
            columnas=columnas,
            minimo_bajada=self.config.minimo_senal_bajada,
            truncar=self.config.truncar_texto,
            emojis=self.config.senal_emojis,
            umbral_inversion_pct=self.config.senal_umbral_inversion_pct,
            umbral_tiempo_dias=self.config.senal_umbral_tiempo_dias,
        )
        if not filas:
            self._logger.warning("Sin prospectos que exportar: no se genera archivo.")
            return None

        destino = self.ruta_destino(ruta, demo=demo)
        delimitador = self.config.delimitador or ";"
        encoding = self.config.encoding or "utf-8-sig"

        try:
            destino.parent.mkdir(parents=True, exist_ok=True)
            with destino.open("w", encoding=encoding, newline="") as archivo:
                escritor = csv.DictWriter(
                    archivo, fieldnames=list(columnas), delimiter=delimitador, extrasaction="ignore"
                )
                escritor.writeheader()
                escritor.writerows(filas)
        except OSError as error:
            self._logger.error("No se pudo escribir el CSV (%s): %s", destino, error)
            return None

        _METRICAS = MetricasExportacion(
            archivo=str(destino),
            filas=metricas["filas"],
            columnas=len(columnas),
            con_oportunidad=metricas["con_oportunidad"],
            con_bajada=metricas["con_bajada"],
            dueno_directo=metricas["dueno_directo"],
            empresa_formal=metricas["empresa_formal"],
            con_correo=metricas["con_correo"],
            sin_id=metricas["sin_id"],
            valor_cartera_usd=metricas["valor_cartera_usd"],
            ticket_promedio_usd=metricas["ticket_promedio_usd"],
            m2_totales=metricas["m2_totales"],
            antiguedad_promedio=metricas["antiguedad_promedio"],
            tasa_oportunidad_pct=metricas["tasa_oportunidad_pct"],
            top_distritos=list(metricas["top_distritos"]),
            bytes_escritos=destino.stat().st_size if destino.exists() else 0,
        )

        if self.config.guardar_reporte:
            _METRICAS.reporte = self._guardar_reporte(destino, metricas)

        self._logger.info(
            "CSV final: %s | %d prospectos | %d columnas | %d oportunidades | cartera %.0f USD",
            destino,
            _METRICAS.filas,
            _METRICAS.columnas,
            _METRICAS.con_oportunidad,
            _METRICAS.valor_cartera_usd,
        )
        if _METRICAS.sin_id:
            self._logger.error(
                "ATENCION: %d filas sin id_aviso (no se podra registrar su venta).", _METRICAS.sin_id
            )
        return destino

    def _guardar_reporte(self, destino: Path, metricas: Dict[str, Any]) -> Optional[str]:
        """Sidecar JSON con las metricas de la corrida (para el area comercial)."""
        ruta = destino.with_suffix(".reporte.json")
        paquete = {
            "generado": datetime.now().isoformat(timespec="seconds"),
            "archivo_csv": destino.name,
            "columnas": list(metricas["columnas"]),
            "metricas": {
                clave: valor for clave, valor in metricas.items()
                if clave not in {"distritos", "columnas"}
            },
            "distritos": dict(metricas.get("distritos") or {}),
        }
        try:
            ruta.write_text(json.dumps(paquete, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
            return str(ruta)
        except OSError as error:
            self._logger.debug("No se pudo escribir el reporte: %s", error)
            return None


# =========================================================================== #
#  Contrato del orquestador
# =========================================================================== #
def exportar(
    opt: Optimizador,
    registros: Sequence[Dict[str, Any]],
    ruta: Optional[Path | str] = None,
    demo: bool = False,
) -> Optional[Path]:
    """Punto de entrada que invoca ``main.py`` (nunca lanza excepcion)."""
    try:
        exportador = ExportadorCSV(getattr(opt, "settings", None) if opt else None)
        return exportador.exportar(registros, ruta=ruta, demo=demo)
    except Exception as error:
        log.error("Exportador CSV fallo: %s", error, exc_info=True)
        return None


# =========================================================================== #
#  Uso directo / autotest:  python exportador_csv.py
# =========================================================================== #
def _demo_registros() -> List[Dict[str, Any]]:
    """Registros sinteticos que ejercitan todos los titulares de la senal."""
    return [
        {   # rebaja fuerte -> 🔥 OPORTUNIDAD DE INVERSION
            "id_aviso": "url:c6bed01fba89bb63", "ruc": "20512345671",
            "razon_social": "INVERSIONES LIMA NORTE S.A.C.", "estado_sunat": "ACTIVO",
            "actividad": "ACTIVIDADES INMOBILIARIAS", "distrito_fiscal": "San Isidro",
            "correo_corporativo": "contacto@limanorte.pe", "web": "https://www.limanorte.pe",
            "tipo_inmueble": "departamento", "zona": "San Isidro, Lima", "m2": 120.0,
            "precio_usd": 162_800.0, "vendedor": "INVERSIONES LIMA NORTE S.A.C.",
            "enlace_aviso": "https://portal-ejemplo.pe/aviso/123456",
            "dias_publicado": 30, "bajadas_precio": 22_200.0, "bajadas_veces": 1,
            "bajada_pct": 12.0, "precio_minimo": 162_800.0, "precio_anterior": 185_000.0,
            "dias_ultima_bajada": 14, "distrito": "San Isidro", "provincia": "LIMA",
            "empresa_vinculada": True, "portal": "urbania.pe", "veces_visto": 2,
        },
        {   # dueño directo recien publicado -> 🆕 RECIEN PUBLICADO
            "id_aviso": "url:9876543210abcdef", "ruc": None, "razon_social": None,
            "estado_sunat": None, "actividad": None, "distrito_fiscal": "Miraflores",
            "correo_corporativo": None, "web": None, "tipo_inmueble": "casa",
            "zona": "Miraflores, Lima", "m2": 210.5, "precio_usd": 320_000.0,
            "vendedor": "dueño directo", "enlace_aviso": "https://portal-ejemplo.pe/aviso/987654",
            "dias_publicado": 3, "bajadas_precio": 0, "bajadas_veces": 0,
            "precio_minimo": 320_000.0, "dias_ultima_bajada": None, "distrito": "Miraflores",
            "provincia": "LIMA", "empresa_vinculada": False, "portal": "adondevivir.com",
            "veces_visto": 1,
        },
        {   # 95 dias + rebaja acumulada -> 📉 NEGOCIABLE (o inversion por monto)
            "id_aviso": "url:555111aaaabbbbcc", "ruc": "20487654320",
            "razon_social": "CONSTRUCTORA ANDINA E.I.R.L.", "estado_sunat": "ACTIVO",
            "actividad": "CONSTRUCCION DE EDIFICIOS", "distrito_fiscal": "Callao",
            "correo_corporativo": "proyectos@andina-construye.com", "web": "https://andina-construye.com",
            "tipo_inmueble": "terreno", "zona": "Cieneguilla, Lima", "m2": 1_000.0,
            "precio_usd": 85_500.0, "vendedor": "CONSTRUCTORA ANDINA E.I.R.L.",
            "enlace_aviso": "https://portal-ejemplo.pe/aviso/555111",
            "dias_publicado": 95, "bajadas_precio": 25_600.0, "bajadas_veces": 3,
            "bajada_pct": 23.0, "precio_minimo": 85_500.0, "precio_anterior": 95_000.0,
            "dias_ultima_bajada": 40, "distrito": "Cieneguilla", "provincia": "LIMA",
            "empresa_vinculada": True, "portal": "urbania.pe", "veces_visto": 4,
        },
        {   # mucho tiempo sin rebaja -> ⏳ OPORTUNIDAD POR TIEMPO
            "id_aviso": "url:aaaabbbbccccdddd", "ruc": "20601234565",
            "razon_social": "GRUPO MIRAFLORES PROPIEDADES S.A.C.", "estado_sunat": "ACTIVO",
            "actividad": "ACTIVIDADES INMOBILIARIAS", "distrito_fiscal": "Miraflores",
            "correo_corporativo": "ventas@grupomiraflores.pe", "web": "https://www.grupomiraflores.pe",
            "tipo_inmueble": "departamento", "zona": "Miraflores, Lima", "m2": 145.0,
            "precio_usd": 268_000.0, "vendedor": "GRUPO MIRAFLORES PROPIEDADES S.A.C.",
            "enlace_aviso": "https://portal-ejemplo.pe/aviso/666777",
            "dias_publicado": 120, "bajadas_precio": 0, "bajadas_veces": 0,
            "precio_minimo": 268_000.0, "dias_ultima_bajada": None, "distrito": "Miraflores",
            "provincia": "LIMA", "empresa_vinculada": True, "portal": "urbania.pe",
            "veces_visto": 5,
        },
        {   # rebaja moderada -> 📉 NEGOCIABLE
            "id_aviso": "url:eeeeffff00001111", "ruc": "20609876540",
            "razon_social": "DESARROLLOS URBANOS DEL SUR S.A.C.", "estado_sunat": "ACTIVO",
            "actividad": "CONSTRUCCION DE EDIFICIOS COMPLETOS", "distrito_fiscal": "Santiago de Surco",
            "correo_corporativo": "informes@urbanosdelsur.com", "web": "https://www.urbanosdelsur.com",
            "tipo_inmueble": "departamento", "zona": "Santiago de Surco, Lima", "m2": 132.0,
            "precio_usd": 246_500.0, "vendedor": "DESARROLLOS URBANOS DEL SUR S.A.C.",
            "enlace_aviso": "https://portal-ejemplo.pe/aviso/888999",
            "dias_publicado": 22, "bajadas_precio": 13_500.0, "bajadas_veces": 1,
            "bajada_pct": 5.0, "precio_minimo": 246_500.0, "precio_anterior": 260_000.0,
            "dias_ultima_bajada": 6, "distrito": "Santiago de Surco", "provincia": "LIMA",
            "empresa_vinculada": True, "portal": "urbania.pe", "veces_visto": 2,
        },
        {   # dueño directo, sin rebaja y en rango medio -> 🤝 TRATO DIRECTO
            "id_aviso": "url:1122334455667788", "ruc": None, "razon_social": None,
            "estado_sunat": None, "actividad": None, "distrito_fiscal": "San Miguel",
            "correo_corporativo": None, "web": None, "tipo_inmueble": "casa",
            "zona": "San Miguel, Lima", "m2": 160.0, "precio_usd": 195_000.0,
            "vendedor": "dueño directo", "enlace_aviso": "https://portal-ejemplo.pe/aviso/222333",
            "dias_publicado": 20, "bajadas_precio": 0, "bajadas_veces": 0,
            "precio_minimo": 195_000.0, "dias_ultima_bajada": None, "distrito": "San Miguel",
            "provincia": "LIMA", "empresa_vinculada": False, "portal": "adondevivir.com",
            "veces_visto": 2,
        },
    ]


def main(argv: Optional[Sequence[str]] = None) -> int:
    import argparse

    from logger import configurar_logging

    parser = argparse.ArgumentParser(description="Exportador CSV final (prospectos A1 listos para venta)")
    parser.add_argument("--salida", default=None, help="Ruta del CSV (por defecto, la configurada).")
    parser.add_argument("--extras", action="store_true", help="Incluye las columnas de auditoria.")
    parser.add_argument("--senal", action="store_true", help="Muestra solo las frases de senal.")
    argumentos = parser.parse_args(argv)

    settings = get_settings()
    configurar_logging(nivel="INFO", carpeta=settings.logs.carpeta, archivo="exportador.log")
    if argumentos.extras:
        object.__setattr__(settings.exportacion, "incluir_extras", True)

    registros = _demo_registros()

    if argumentos.senal:
        print("\n=== FRASES DE SENAL (demo) ===")
        for registro in registros:
            print(f"\n  {registro['tipo_inmueble'].upper()} en {registro['zona']} "
                  f"({registro['precio_usd']:,.0f} USD)")
            print(f"  -> {generar_senal(registro, settings.exportacion.minimo_senal_bajada)}")
        return 0

    exportador = ExportadorCSV(settings)
    ruta = exportador.exportar(registros, ruta=argumentos.salida, demo=True)
    if not ruta:
        print("No se pudo exportar.")
        return 1

    print(f"\n=== CSV GENERADO: {ruta} ===")
    print(f"  columnas ({ultimas_metricas()['columnas']}):")
    print(f"    {list(columnas_salida(settings.exportacion.incluir_extras))}")
    print("\n  primeras lineas:")
    for numero, linea in enumerate(ruta.read_text(encoding=settings.exportacion.encoding).splitlines()[:4], 1):
        print(f"    {numero}: {linea[:190]}")
    print(f"\n  metricas: {json.dumps(ultimas_metricas(), ensure_ascii=False, indent=2)[:900]}")
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
