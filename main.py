"""
main.py  --  Orquestador / CLI del Pipeline B2B Inmobiliario Peru  (v2.0)
========================================================================

Punto de entrada unico con **menu interactivo**, aislamiento de fallos por
etapa y las dos reglas de negocio del producto:

  1. ALCANCE GEOGRAFICO ESTRICTO: solo Lima Metropolitana + Callao.
  2. EXCLUSIVIDAD COMERCIAL: un aviso vendido NUNCA se revende; un aviso
     entregado no se repite dentro de la ventana (30 dias por defecto).
  3. META ESTRICTA: el pipeline se detiene al alcanzar N prospectos de calidad
     A1 (10 por defecto) para control manual y ahorro de costos.

Uso
---
    python main.py                       # MENU INTERACTIVO
    python main.py --modo local --demo   # Modo Local (solo cache/BD, costo $0)
    python main.py --modo live --demo    # Modo OSINT/Live (Serper + Scrapling)
    python main.py --telemetria          # Dashboard de estado del sistema
    python main.py --demo --meta 2       # Prueba la meta estricta de 2
    python main.py --modo sunat --limite 50

------------------------------------------------------------------------------
CONTRATO DE MODULOS  (todos devuelven ``list[dict]`` y NUNCA lanzan excepcion)
------------------------------------------------------------------------------
    extractor_sunat.extraer(opt, limite=None)          -> list[dict]   # GRUPO 1
    extractor_portales.extraer(opt, limite=None)       -> list[dict]   # GRUPO 2
    procesador_historico.procesar(opt, registros)      -> list[dict]   # + GRUPO 3
    exportador_csv.exportar(opt, registros, ruta=None) -> str | None   # salida

Los extractores deben:
  * anclar toda busqueda con ``opt.settings.alcance.consulta(termino)``;
  * respetar ``opt.solo_local`` (en Modo Local solo leen cache/BD, sin red);
  * devolver como maximo ``limite`` registros (control de costos).
------------------------------------------------------------------------------

FLUJO DE 5 ETAPAS
-----------------
 1. GRUPO 1 (SUNAT)      -> empresas y contacto
 2. GRUPO 2 (portales)   -> avisos inmobiliarios
 3. GRUPO 3 (historico)  -> dias_publicado, bajadas_precio (PostgreSQL)
 4. CONTROL DE CALIDAD   -> alcance Lima/Callao + exclusividad + META estricta
 5. EXPORTACION          -> CSV final (id_aviso 1a columna) + confirmacion
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from config import COLUMNAS_CRITICAS, COLUMNAS_SALIDA, get_settings, mascarar

try:
    from extractor_portales import fusionar_empresa, vincular_empresa
except ModuleNotFoundError:
    from extractor_demo import fusionar_empresa, vincular_empresa
from logger import configurar_logging, obtener_logger
from optimizador import Optimizador, cerrar_optimizador, get_optimizador
from procesador_historico import HistoricoPrecios, cerrar_historico
from registro_ventas import RegistroVentas, ResultadoFiltro, generar_aviso_id

MODULOS: Dict[str, str] = {
    "sunat": "extractor_demo_sunat",
    "portales": "extractor_demo",
    "historico": "procesador_historico",
    "exportador": "exportador_csv",
}

# Columnas que deben existir ANTES de exportar ("senal" la crea el exportador).
COLUMNAS_CRITICAS_PRE_EXPORT: Tuple[str, ...] = tuple(c for c in COLUMNAS_CRITICAS if c != "senal")

log = obtener_logger("main")


# =========================================================================== #
#  Presentacion en terminal
#  ---------------------------------------------------------------------------
#  Con `rich` instalado se usan paneles, tablas y barras; si no esta, todo
#  degrada a texto plano sin perder funcionalidad (mismo contenido, sin adornos).
# =========================================================================== #
try:  # interfaz premium opcional
    from rich import box as _rich_box
    from rich.console import Group as _RichGroup
    from rich.panel import Panel as _RichPanel
    from rich.table import Table as _RichTable
    from rich.text import Text as _RichText

    from logger import obtener_consola_rich

    _CONSOLA = obtener_consola_rich()
    RICH_DISPONIBLE = _CONSOLA is not None
except Exception:  # pragma: no cover - sin rich se sigue funcionando
    _CONSOLA = None
    RICH_DISPONIBLE = False


def _color_activo() -> bool:
    if os.getenv("NO_COLOR"):
        return False
    try:
        return bool(sys.stdout.isatty())
    except Exception:
        return False


_COLORES = {"verde": "32", "amarillo": "33", "rojo": "31", "cian": "36", "gris": "90", "negrita": "1"}

# Etiquetas de rich que se eliminan al renderizar en texto plano.
RE_MARKUP = re.compile(
    r"\[/\s*\]|"
    r"\[/?(?:bold|dim|italic|underline|reverse|blink|black|white|green|red|yellow|blue|magenta|cyan|"
    r"grey\d*|bright_\w+)(?:\s+[a-z ]+)?\]"
)


def c(texto: Any, color: str = "", negrita: bool = False) -> str:
    """Colorea texto (solo en la ruta sin rich y si hay terminal interactiva)."""
    etiqueta = str(texto)
    if RICH_DISPONIBLE or not _color_activo():
        return etiqueta
    codigos = ([_COLORES["negrita"]] if negrita else []) + ([_COLORES[color]] if color in _COLORES else [])
    return f"\033[{';'.join(codigos)}m{etiqueta}\033[0m" if codigos else etiqueta


def barra(pct: float, ancho: int = 26) -> str:
    """Barra de progreso ASCII (respaldo de rich)."""
    pct = max(0.0, min(100.0, float(pct)))
    llenos = int(round(ancho * pct / 100.0))
    return "[" + "#" * llenos + "-" * (ancho - llenos) + "]"


def _estilo_por_valor(pct: float) -> str:
    if pct >= 60:
        return "green"
    if pct >= 25:
        return "yellow"
    return "red"


def _soporta_unicode() -> bool:
    """¿La salida admite caracteres de bloque (evita UnicodeEncodeError)?."""
    try:
        codificacion = (getattr(sys.stdout, "encoding", "") or "").lower()
    except Exception:
        return False
    return "utf" in codificacion


def barra_visual(pct: float, ancho: int = 28, invertir: bool = False) -> str:
    """Barra de progreso como markup rich (o ASCII si rich no esta).

    Se construye a mano en vez de usar ``rich.progress_bar.ProgressBar``: asi
    se renderiza igual en terminal, en pipe y dentro de una tabla.
    """
    pct = max(0.0, min(100.0, float(pct)))
    if invertir:
        pct = 100.0 - pct
    if not RICH_DISPONIBLE or not _soporta_unicode():
        return barra(pct, ancho)
    llenos = int(round(ancho * pct / 100.0))
    color = _estilo_por_valor(pct)
    return f"[{color}]{'█' * llenos}[/][grey37]{'░' * (ancho - llenos)}[/]"


def panel_cartera(metricas: Dict[str, Any], entregados: int = 0) -> None:
    """Panel final: la cartera A1 lista para vender (metricas comerciales)."""
    if not metricas or not int(metricas.get("filas") or 0):
        return

    filas_exportadas = int(metricas.get("filas") or 0)
    columnas = int(metricas.get("columnas") or 0)
    cartera = float(metricas.get("valor_cartera_usd") or 0)
    ticket = float(metricas.get("ticket_promedio_usd") or 0)
    oportunidades = int(metricas.get("con_oportunidad") or 0)
    tasa = float(metricas.get("tasa_oportunidad_pct") or 0)
    directos = int(metricas.get("dueno_directo") or 0)
    formales = int(metricas.get("empresa_formal") or 0)
    con_correo = int(metricas.get("con_correo") or 0)
    antiguedad = float(metricas.get("antiguedad_promedio") or 0)
    peso_kb = float(metricas.get("bytes_escritos") or 0) / 1024.0
    distritos = metricas.get("top_distritos") or []
    archivo = str(metricas.get("archivo") or "-")
    titular = "CARTERA A1 LISTA PARA VENTA"
    distritos_texto = " · ".join(f"{d.get('distrito')} ({d.get('avisos')})" for d in distritos[:3]) or "-"
    nombre_archivo = Path(str(metricas.get("archivo") or "-")).name

    lineas = [
        ("Prospectos A1", f"[bold green]{filas_exportadas}[/]  ({columnas} columnas · {peso_kb:,.1f} KB)"),
        ("Con oportunidad", f"[bold yellow]{oportunidades}[/]  ({tasa:.1f}% bajó de precio)"),
        ("Valor de la cartera", f"[bold]USD {cartera:,.0f}[/]"),
        ("Ticket promedio", f"USD {ticket:,.0f}"),
        ("Antigüedad promedio", f"{antiguedad:.0f} días publicado"),
        ("Vendedores", f"{directos} dueño directo · {formales} empresa(s) formal(es) · {con_correo} con correo"),
        ("Top distritos", distritos_texto),
        ("Archivo", nombre_archivo),
        ("Exclusividad", f"{entregados} avisos registrados (no se repiten)"),
    ]

    if RICH_DISPONIBLE:
        cuerpo = _RichTable(box=None, show_header=False, padding=(0, 2), expand=False)
        cuerpo.add_column(style="cyan", no_wrap=True)
        cuerpo.add_column(style="white")
        for etiqueta, valor in lineas:
            cuerpo.add_row(etiqueta, _RichText.from_markup(valor))
        cta = _RichText()
        cta.append("\n  Registrar una venta: ", style="dim")
        cta.append("python registro_ventas.py --vender <id_aviso> --cliente \"<cliente>\"", style="bold green")
        _CONSOLA.print()
        _CONSOLA.print(
            _RichPanel(
                _RichGroup(
                    _RichText(f"{titular}", style="bold green", justify="center"),
                    _RichText(),
                    cuerpo,
                    cta,
                ),
                border_style="green",
                box=_rich_box.DOUBLE,
                padding=(1, 3),
            )
        )
        return

    ancho = 78
    print("\n" + "+" + "-" * (ancho - 2) + "+")
    print("|" + titular.center(ancho - 2) + "|")
    print("+" + "-" * (ancho - 2) + "+")
    for etiqueta, valor in lineas:
        print(f"  {etiqueta:24}: {_limpiar_markup(valor)}")
    print(f"\n  Registrar una venta: python registro_ventas.py --vender <id_aviso> --cliente \"<cliente>\"")
    print("+" + "-" * (ancho - 2) + "+\n")


class TableroEtapas:
    """Panel de progreso por etapas: rich Live o lineas de texto.

    La consola solo muestra el avance de alto nivel (etapa, resultado, estado),
    mientras el detalle tecnico se escribe en ``logs/pipeline.log``.
    """

    def __init__(self, titulo: str, etapas: Sequence[str]):
        self.titulo = titulo
        self.etapas = list(etapas)
        self.estados: List[str] = ["pendiente"] * len(self.etapas)
        self.detalles: List[str] = [""] * len(self.etapas)
        self._live = None
        self._tabla = None

    # ------------------------------------------------------------------ #
    def __enter__(self) -> "TableroEtapas":
        if RICH_DISPONIBLE and _color_activo():
            try:
                self._tabla = _RichTable(
                    title=self.titulo, title_justify="left", box=_rich_box.ROUNDED, expand=False
                )
                self._tabla.add_column("#", justify="right", style="cyan", width=2)
                self._tabla.add_column("Etapa", style="bold", no_wrap=True)
                self._tabla.add_column("Resultado", style="white")
                self._tabla.add_column("Estado", justify="left", width=14)
                self._live = _CONSOLA.live(self._tabla, refresh_per_second=8, transient=False)
                self._live.__enter__()
                self._refrescar()
            except Exception:
                self._live = None
        if self._live is None:
            encabezado(self.titulo)
        return self

    def __exit__(self, *_excepcion: Any) -> bool:
        if self._live is not None:
            try:
                self._refrescar()
                self._live.__exit__(None, None, None)
            except Exception:
                pass
        return False

    # ------------------------------------------------------------------ #
    def en_curso(self, indice: int) -> None:
        self.estados[indice] = "en curso"
        self._refrescar()

    def completar(self, indice: int, detalle: str, estado: str = "ok") -> None:
        self.estados[indice] = estado
        self.detalles[indice] = detalle or ""
        self._refrescar()
        if self._live is None:
            print(f"  {indice + 1}. {self.etapas[indice]:46} {self.detalles[indice]}  [{estado}]")

    # ------------------------------------------------------------------ #
    def _etiqueta_estado(self, estado: str) -> Any:
        if estado == "ok":
            return "[green]listo[/]"
        if estado == "en curso":
            try:
                from rich.spinner import Spinner

                return Spinner("dots", style="cyan")
            except Exception:
                return "[cyan]...[/]"
        if estado == "demo":
            return "[cyan]demo[/]"
        if estado in {"aviso", "sin_datos", "omitida"}:
            return "[yellow]parcial[/]"
        if estado == "error":
            return "[red]error[/]"
        return "[dim]pendiente[/]"

    def _refrescar(self) -> None:
        if self._live is None or self._tabla is None:
            return
        try:
            self._tabla.rows.clear()
            for indice, etapa in enumerate(self.etapas):
                self._tabla.add_row(
                    str(indice + 1),
                    etapa,
                    self.detalles[indice] or "[dim]-[/]",
                    self._etiqueta_estado(self.estados[indice]),
                )
            self._live.refresh()
        except Exception:
            pass


def encabezado(titulo: str, subtitulo: str = "") -> None:
    """Titulo de seccion (panel rich o linea ASCII)."""
    if RICH_DISPONIBLE:
        _CONSOLA.print()
        _CONSOLA.rule(f"[bold cyan]{titulo}[/]", style="cyan", align="left")
        if subtitulo:
            _CONSOLA.print(f"  [dim]{subtitulo}[/]")
        return
    print("\n" + "=" * 76)
    print(f"  {titulo}")
    if subtitulo:
        print(f"  {subtitulo}")
    print("=" * 76)


def _limpiar_markup(texto: Any) -> str:
    """Quita etiquetas de rich cuando se renderiza en texto plano."""
    return RE_MARKUP.sub("", str(texto))


def tabla(titulo: str, columnas: Sequence[str], filas: Sequence[Sequence[Any]], alineaciones: Optional[Sequence[str]] = None) -> None:
    """Tabla de datos (rich o texto plano alineado)."""
    if not filas:
        return
    if RICH_DISPONIBLE:
        tabla_rich = _RichTable(title=titulo or None, box=_rich_box.SIMPLE_HEAVY, title_justify="left", expand=False)
        for indice, columna in enumerate(columnas):
            justificacion = (alineaciones[indice] if alineaciones and indice < len(alineaciones) else "left")
            tabla_rich.add_column(str(columna), justify=justificacion, no_wrap=False)
        for fila in filas:
            tabla_rich.add_row(*[celda if not isinstance(celda, str) else _RichText.from_markup(celda) for celda in fila])
        _CONSOLA.print(tabla_rich)
        return
    filas_planas = [[_limpiar_markup(celda) for celda in fila] for fila in filas]
    if titulo:
        print(f"\n  {_limpiar_markup(titulo)}")
    anchos = [max(len(str(fila[i])) for fila in [columnas, *filas_planas]) for i in range(len(columnas))]
    print("  " + "  ".join(str(col).ljust(anchos[i]) for i, col in enumerate(columnas)))
    print("  " + "  ".join("-" * anchos[i] for i in range(len(columnas))))
    for fila in filas_planas:
        print("  " + "  ".join(str(celda).ljust(anchos[i]) for i, celda in enumerate(fila)))


def aviso(mensaje: str, nivel: str = "info") -> None:
    """Mensaje destacado (color rich o prefijo ASCII)."""
    iconos = {"info": "[cyan]i[/]", "ok": "[green]OK[/]", "alerta": "[yellow]![/]", "error": "[red]X[/]"}
    if RICH_DISPONIBLE:
        _CONSOLA.print(f"  {iconos.get(nivel, '[cyan]i[/]')} {mensaje}")
        return
    prefijos = {"info": " [i]", "ok": " [OK]", "alerta": " [!]", "error": " [X]"}
    print(f"{prefijos.get(nivel, ' [i]')} {_limpiar_markup(mensaje)}")


class estado_tarea:
    """Spinner de progreso durante una etapa (no-op si no hay rich/terminal)."""

    def __init__(self, mensaje: str):
        self.mensaje = mensaje
        self._contexto = None

    def __enter__(self):
        if RICH_DISPONIBLE and _color_activo():
            try:
                self._contexto = _CONSOLA.status(f"[cyan]{self.mensaje}[/]", spinner="dots")
                self._contexto.__enter__()
            except Exception:
                self._contexto = None
        return self

    def __exit__(self, *_excepcion: Any) -> bool:
        if self._contexto is not None:
            try:
                self._contexto.__exit__(None, None, None)
            except Exception:
                pass
        return False


# =========================================================================== #
#  Utilidades del orquestador
# =========================================================================== #
def cargar_etapa(nombre: str) -> Optional[Any]:
    """Importa una etapa de forma tolerante a fallos (``None`` si no existe)."""
    modulo = MODULOS.get(nombre)
    if not modulo:
        return None
    try:
        return importlib.import_module(modulo)
    except ModuleNotFoundError as error:
        log.warning("Etapa '%s' no disponible (%s). Se omite por ahora.", modulo, error)
        return None
    except Exception as error:
        log.error("Etapa '%s' no se pudo importar: %s", modulo, error, exc_info=True)
        return None


def invocar(funcion: Any, *args: Any, **kwargs: Any) -> Any:
    """Llama una funcion de etapa capturando cualquier excepcion."""
    try:
        return funcion(*args, **kwargs)
    except Exception as error:
        log.error("Fallo en %s: %s", getattr(funcion, "__name__", funcion), error, exc_info=True)
        return None


# =========================================================================== #
#  Resultado de la ejecucion
# =========================================================================== #
@dataclass
class ResultadoPipeline:
    """Estado y metricas de una corrida completa."""

    modo: str = "completo"
    meta_prospectos: int = 10
    inicio: float = field(default_factory=time.time)
    etapas: Dict[str, str] = field(default_factory=dict)
    registros_grupo1: List[Dict[str, Any]] = field(default_factory=list)
    registros_grupo2: List[Dict[str, Any]] = field(default_factory=list)
    registros_finales: List[Dict[str, Any]] = field(default_factory=list)
    omitidos_exclusividad: List[Dict[str, Any]] = field(default_factory=list)
    fuera_de_alcance: List[Dict[str, Any]] = field(default_factory=list)
    sin_empresa: List[Dict[str, Any]] = field(default_factory=list)
    ids_entregables: List[str] = field(default_factory=list)
    entregados: int = 0
    excedente_descartado: int = 0
    meta_alcanzada: bool = False
    vinculados: int = 0
    historico: Dict[str, Any] = field(default_factory=dict)
    exportacion: Dict[str, Any] = field(default_factory=dict)
    archivo_salida: Optional[str] = None
    filtro_exclusividad: Dict[str, Any] = field(default_factory=dict)

    def marcar(self, etapa: str, estado: str) -> None:
        self.etapas[etapa] = estado

    @property
    def duracion_segundos(self) -> float:
        return round(time.time() - self.inicio, 2)

    def resumen(self) -> Dict[str, Any]:
        return {
            "modo": self.modo,
            "meta_prospectos": self.meta_prospectos,
            "meta_alcanzada": self.meta_alcanzada,
            "duracion_segundos": self.duracion_segundos,
            "etapas": dict(self.etapas),
            "grupo1_empresas": len(self.registros_grupo1),
            "grupo2_avisos": len(self.registros_grupo2),
            "avisos_vinculados_empresa": self.vinculados,
            "avisos_sin_empresa": len(self.sin_empresa),
            "fuera_de_alcance": len(self.fuera_de_alcance),
            "omitidos_por_exclusividad": len(self.omitidos_exclusividad),
            "prospectos_finales": len(self.registros_finales),
            "excedente_descartado": self.excedente_descartado,
            "entregas_registradas": self.entregados,
            "archivo_salida": self.archivo_salida,
        }


# =========================================================================== #
#  Orquestador
# =========================================================================== #
class Pipeline:
    """Ejecuta el flujo: extraccion -> historico -> calidad -> exportacion."""

    def __init__(
        self,
        modo: str = "completo",
        limite: Optional[int] = None,
        meta: Optional[int] = None,
        demo: bool = False,
        solo_local: bool = False,
        guardar_json: bool = False,
        usar_cache: bool = True,
        sin_registro: bool = False,
        confirmar_forzado: bool = False,
        incluir_fuera_de_lima: bool = False,
        ventana_dias: Optional[float] = None,
        permitir_sin_empresa: bool = False,
        demo_bajada: float = 0.0,
        resumen_completo: bool = True,
    ):
        self.modo = modo
        self.limite = limite
        self.demo = demo
        self.solo_local = solo_local
        self.guardar_json = guardar_json
        self.usar_cache = usar_cache
        self.sin_registro = sin_registro
        self.confirmar_forzado = confirmar_forzado
        self.incluir_fuera_de_lima = incluir_fuera_de_lima
        self.permitir_sin_empresa = permitir_sin_empresa
        self.demo_bajada = float(demo_bajada or 0.0)
        # En modo automatico la consola es minimalista: solo RESUMEN + cartera.
        self.resumen_completo = bool(resumen_completo)

        self.settings = get_settings()
        self.opt = get_optimizador(self.settings)
        self.opt.establecer_modo(solo_local)
        self.meta = max(1, int(meta or self.settings.meta.limite_prospectos))
        self.resultado = ResultadoPipeline(modo=modo, meta_prospectos=self.meta)

        ruta_ventas = self.settings.ventas.ruta
        if demo and self.settings.ventas.separar_db_demo:
            ruta_ventas = ruta_ventas.with_name(f"{ruta_ventas.stem}_demo{ruta_ventas.suffix}")
        self.registro = RegistroVentas(
            ruta=ruta_ventas,
            ventana_dias=ventana_dias or self.settings.ventas.ventana_dias,
            retencion_dias=self.settings.ventas.retencion_dias,
            politica_error=self.settings.ventas.politica_error,
            bloqueo_venta_permanente=self.settings.ventas.bloqueo_venta_permanente,
        )

        # El historico de precios tampoco se contamina con datos de prueba.
        ruta_historico = self.settings.portales.historico_ruta
        if demo and self.settings.ventas.separar_db_demo:
            ruta_historico = ruta_historico.with_name(f"{ruta_historico.stem}_demo{ruta_historico.suffix}")
        self.ruta_historico = ruta_historico
        self.historico = HistoricoPrecios(
            ruta=ruta_historico,
            retencion_dias=self.settings.portales.historico_retencion_dias,
            usar_fecha_publicacion=self.settings.portales.usar_fecha_publicacion,
        )

    # ------------------------------ Ejecucion ---------------------------- #
    def ejecutar(self) -> ResultadoPipeline:
        log.info("=" * 78)
        log.info(
            "PIPELINE B2B INMOBILIARIO v2.0 | modo=%s | %s | demo=%s | meta=%d",
            self.modo,
            self.opt.pronunciar_modo(),
            self.demo,
            self.meta,
        )
        log.info("Alcance: %s | Exclusividad: %s", self.settings.alcance.etiqueta, self._politica_exclusividad())
        log.info("=" * 78)

        etapas: List[Tuple[str, str, Any]] = []
        if self.modo in {"completo", "local", "live", "sunat"}:
            etapas.append(("grupo1_sunat", "GRUPO 1 · Empresas y contacto (SUNAT)", self.etapa_grupo1))
        if self.modo in {"completo", "local", "live", "portales"}:
            if self._meta_alcanzada():
                log.warning("META ya alcanzada: se omite la extraccion de portales.")
            else:
                etapas.append(("grupo2_portales", "GRUPO 2 · Avisos de portales", self.etapa_grupo2))
        if self.modo in {"completo", "local", "live", "sunat", "portales"}:
            etapas.append(
                ("grupo3_historico", "GRUPO 3 · Cruce con empresa + histórico de precios", self._etapa_grupo3_con_control)
            )
            etapas.append(
                ("control_calidad", "Control de calidad A1 (alcance + exclusividad + meta)", self.etapa_control_calidad)
            )
        if self.modo in {"completo", "local", "live", "sunat", "portales", "exportar"}:
            etapas.append(("exportacion", "Exportación del CSV final para venta", self.etapa_exportar))

        tablero = TableroEtapas(
            f"PIPELINE {self.opt.pronunciar_modo()} · meta {self.meta} prospectos A1 · {self.settings.alcance.etiqueta}",
            [descripcion for _, descripcion, _ in etapas],
        )
        try:
            with tablero:
                for indice, (clave, _descripcion, funcion) in enumerate(etapas):
                    tablero.en_curso(indice)
                    funcion()
                    tablero.completar(
                        indice, self._detalle_etapa(clave), self._estado_etapa(clave)
                    )
        finally:
            self._volcar_json()
            self._imprimir_resumen()
            self.registro.cerrar()
            self.historico.cerrar()
            cerrar_optimizador()

        return self.resultado

    # ------------------------------------------------------------------ #
    #  Detalle y estado de cada etapa (alimenta el tablero de progreso)
    # ------------------------------------------------------------------ #
    def _detalle_etapa(self, clave: str) -> str:
        if clave == "grupo1_sunat":
            return f"{len(self.resultado.registros_grupo1)} empresas"
        if clave == "grupo2_portales":
            portales = sorted({str(r.get("portal") or "?") for r in self.resultado.registros_grupo2})
            base = f"{len(self.resultado.registros_grupo2)} avisos"
            return f"{base} de {', '.join(portales)}" if portales else base
        if clave == "grupo3_historico":
            con_bajada = sum(1 for r in self.resultado.registros_finales if (r.get("bajadas_precio") or 0) > 0)
            return f"{self.resultado.vinculados} vinculados a empresa · {con_bajada} con bajada de precio"
        if clave == "control_calidad":
            return (
                f"{len(self.resultado.registros_finales)}/{self.meta} prospectos A1"
                + (f" (excedente {self.resultado.excedente_descartado})" if self.resultado.excedente_descartado else "")
            )
        if clave == "exportacion":
            if self.resultado.archivo_salida:
                return Path(self.resultado.archivo_salida).name
            return "sin archivo (nada que exportar)"
        return ""

    def _estado_etapa(self, clave: str) -> str:
        estado = str(self.resultado.etapas.get(clave) or "")
        if estado == "demo":
            return "demo"
        if estado.startswith("ok"):
            return "ok"
        if estado in {"sin_datos", "omitida"}:
            return "aviso"
        if estado == "error":
            return "error"
        return "aviso"

    def _etapa_grupo3_con_control(self) -> None:
        """GRUPO 3: enriquece y deja el resultado en ``registros_finales``."""
        self.resultado.registros_finales = self.etapa_grupo3()

    # ------------------------------------------------------------------ #
    #  Modo automatico: un solo comando, sin intervencion
    # ------------------------------------------------------------------ #
    def ejecutar_auto(self) -> ResultadoPipeline:
        """Alias explicito del flujo completo (lo usa ``--auto`` y el menu)."""
        return self.ejecutar()

    def _politica_exclusividad(self) -> str:
        if self.settings.ventas.bloqueo_venta_permanente:
            return f"venta = bloqueo permanente | entregas = {self.settings.ventas.ventana_dias:.0f} dias"
        return f"todo bloquea {self.settings.ventas.ventana_dias:.0f} dias (incluidas las ventas)"

    # ------------------------ Control de la META ------------------------- #
    def _meta_alcanzada(self) -> bool:
        alcanzada = len(self.resultado.registros_finales) >= self.meta
        self.resultado.meta_alcanzada = alcanzada
        return alcanzada

    def _slots_restantes(self) -> int:
        return max(0, self.meta - len(self.resultado.registros_finales))

    def _limite_extraccion(self) -> int:
        """Tope de extraccion: meta + sobremuestreo, acotado por --limite."""
        slots = self._slots_restantes()
        if slots <= 0:
            return 0
        objetivo = slots + max(0, self.settings.meta.sobremuestreo)
        if self.limite:
            objetivo = min(objetivo, self.limite)
        return max(1, objetivo)

    def _recortar_a_la_meta(self, registros: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Detiene el pipeline en la meta exacta de prospectos A1."""
        if len(registros) <= self.meta:
            return registros
        recortados = registros[: self.meta]
        self.resultado.excedente_descartado = len(registros) - self.meta
        self.resultado.meta_alcanzada = True
        log.warning(
            "META ESTRICTA: se conservan %d prospectos A1 y se descartan %d (excedente).",
            self.meta,
            self.resultado.excedente_descartado,
        )
        return recortados

    # --------------------------- Etapa GRUPO 1 --------------------------- #
    def etapa_grupo1(self) -> List[Dict[str, Any]]:
        """RUC, razon social, estado SUNAT, actividad, distrito, correo, web."""
        log.info("-" * 78)
        log.info("ETAPA 1/5 - Extractor SUNAT (GRUPO 1) | anclaje: %s", self.settings.alcance.sufijo_busqueda)
        if self.demo:
            self.resultado.registros_grupo1 = self._datos_demo_grupo1()
            self.resultado.marcar("grupo1_sunat", "demo")
            log.info("GRUPO 1 (demo): %d empresas.", len(self.resultado.registros_grupo1))
            return self.resultado.registros_grupo1

        etapa = cargar_etapa("sunat")
        if etapa is None or not hasattr(etapa, "extraer"):
            self.resultado.marcar("grupo1_sunat", "omitida")
            return []

        datos = invocar(etapa.extraer, self.opt, limite=self.limite)
        self.resultado.registros_grupo1 = [d for d in (datos or []) if isinstance(d, dict)]
        self.resultado.marcar(
            "grupo1_sunat", f"ok:{len(self.resultado.registros_grupo1)}" if datos is not None else "error"
        )
        log.info("GRUPO 1: %d empresas extraidas.", len(self.resultado.registros_grupo1))
        return self.resultado.registros_grupo1

    # --------------------------- Etapa GRUPO 2 --------------------------- #
    def etapa_grupo2(self) -> List[Dict[str, Any]]:
        """Tipo de inmueble, zona, m2, precio USD, vendedor, enlace del aviso."""
        limite = self._limite_extraccion()
        log.info("-" * 78)
        log.info(
            "ETAPA 2/5 - Extractor Portales (GRUPO 2) | anclaje: %s | cupo: %d",
            self.settings.alcance.sufijo_busqueda,
            limite,
        )
        if self.demo:
            self.resultado.registros_grupo2 = self._datos_demo_grupo2()
            self.resultado.marcar("grupo2_portales", "demo")
            log.info("GRUPO 2 (demo): %d avisos.", len(self.resultado.registros_grupo2))
            return self.resultado.registros_grupo2

        etapa = cargar_etapa("portales")
        if etapa is None or not hasattr(etapa, "extraer"):
            self.resultado.marcar("grupo2_portales", "omitida")
            return []

        datos = invocar(etapa.extraer, self.opt, limite=limite)
        self.resultado.registros_grupo2 = [d for d in (datos or []) if isinstance(d, dict)]
        self.resultado.marcar(
            "grupo2_portales", f"ok:{len(self.resultado.registros_grupo2)}" if datos is not None else "error"
        )
        log.info("GRUPO 2: %d avisos extraidos (cupo %d).", len(self.resultado.registros_grupo2), limite)
        return self.resultado.registros_grupo2

    # --------------------------- Etapa GRUPO 3 --------------------------- #
    def etapa_grupo3(self) -> List[Dict[str, Any]]:
        """Cruza avisos con empresas y enriquece con el historico de precios.

        Aqui ocurren las dos cosas que dan valor al producto:
          1. **Vinculacion**: cada aviso se asocia a su empresa (SUNAT).
          2. **Historico (GRUPO 3)**: dias_publicado y bajadas_precio.
        """
        log.info("-" * 78)
        log.info("ETAPA 3/5 - Cruce con empresas + procesador historico (GRUPO 3)")

        cruzados = self._cruzar(self.resultado.registros_grupo1, self.resultado.registros_grupo2)
        if not cruzados:
            self.resultado.marcar("grupo3_historico", "sin_datos")
            return []

        etapa = cargar_etapa("historico")
        if etapa is not None and hasattr(etapa, "procesar"):
            datos = invocar(etapa.procesar, self.opt, cruzados, ruta=self.ruta_historico)
        else:
            # Respaldo: se usa la instancia del propio orquestador.
            datos = invocar(self.historico.registrar, cruzados)

        if datos is None:
            self.resultado.marcar("grupo3_historico", "error")
            return cruzados

        finales = [d for d in datos if isinstance(d, dict)]
        self.resultado.historico = self.historico.resumen()
        con_bajada = sum(1 for d in finales if (d.get("bajadas_precio") or 0) > 0)
        self.resultado.marcar("grupo3_historico", f"ok:{len(finales)}")
        log.info(
            "GRUPO 3: %d avisos historizados | %d con bajada de precio | %d dias promedio publicado.",
            len(finales),
            con_bajada,
            self.resultado.historico.get("dias_publicado_promedio", 0),
        )
        return finales

    # --------------------- Etapa 4: control de calidad -------------------- #
    def etapa_control_calidad(self) -> None:
        """ALCANCE (Lima/Callao) + EXCLUSIVIDAD + META ESTRICTA.

        No escribe nada en la base comercial: la confirmacion de entrega ocurre
        en la etapa 5, solo si el CSV se genero realmente.
        """
        log.info("-" * 78)
        log.info("ETAPA 4/5 - Control de calidad comercial (alcance + exclusividad + meta)")

        # 4.1 Filtro geografico estricto.
        dentro, fuera = self._filtrar_alcance(self.resultado.registros_finales)
        self.resultado.fuera_de_alcance = fuera
        if fuera:
            log.warning(
                "ALCANCE: %d registros descartados por estar fuera de %s.",
                len(fuera),
                self.settings.alcance.etiqueta,
            )
            for registro in fuera[:5]:
                log.warning("  - descartado: %s", registro.get("_fuera_de_alcance", {}).get("motivo"))
        log.info("ALCANCE: %d registros dentro de %s.", len(dentro), self.settings.alcance.etiqueta)

        # 4.1.b META A1: solo avisos vinculados a una empresa confirmada.
        if not self.permitir_sin_empresa and self.resultado.registros_grupo2:
            con_empresa = [registro for registro in dentro if registro.get("empresa_vinculada")]
            huerfanos = [registro for registro in dentro if not registro.get("empresa_vinculada")]
            if huerfanos:
                log.warning(
                    "META A1: %d avisos sin empresa vinculada se descartan "
                    "(use --permitir-sin-empresa para conservarlos).",
                    len(huerfanos),
                )
            dentro = con_empresa
        log.info("META A1: %d avisos con empresa confirmada.", len(dentro))

        # 4.2 Exclusividad comercial (fase 1: solo consulta).
        lote = f"run_{time.strftime('%Y%m%d_%H%M%S')}"
        filtro: ResultadoFiltro = self.registro.filtrar(dentro, lote=lote, politica_sin_id="conservar")
        self.resultado.filtro_exclusividad = filtro.resumen()
        self.resultado.omitidos_exclusividad = filtro.omitidos
        log.info("EXCLUSIVIDAD: %s", filtro.resumen())
        if filtro.sin_id:
            log.warning(
                "EXCLUSIVIDAD: %d avisos sin identificador (no se puede garantizar exclusividad).",
                len(filtro.sin_id),
            )

        # 4.3 META ESTRICTA: el pipeline se detiene en N prospectos A1.
        disponibles = self._recortar_a_la_meta(filtro.disponibles)
        self.resultado.registros_finales = disponibles
        self.resultado.ids_entregables = [
            str(registro.get("id_aviso")) for registro in disponibles if registro.get("id_aviso")
        ]
        self.resultado.marcar("control_calidad", f"ok:{len(disponibles)}/{len(dentro)}")
        log.info(
            "META: %d/%d prospectos A1 listos (cupo %d).",
            len(disponibles),
            self.meta,
            self._slots_restantes(),
        )

    # --------------------------- Etapa 5: export ------------------------- #
    def etapa_exportar(self) -> Optional[str]:
        """Genera la columna ``senal`` y exporta el CSV final para venta."""
        log.info("-" * 78)
        log.info("ETAPA 5/5 - Exportador CSV")
        registros = self.resultado.registros_finales

        if not registros:
            log.warning("No hay registros aptos para exportar tras el control de calidad.")
            self.resultado.marcar("exportacion", "sin_datos")
            return None

        self._validar_contrato_salida(registros)

        etapa = cargar_etapa("exportador")
        if etapa is None or not hasattr(etapa, "exportar"):
            self.resultado.marcar("exportacion", "omitida (sin exportador_csv.py)")
            if self.confirmar_forzado:
                log.warning("Sin exportador: confirmacion FORZADA (--confirmar-entrega).")
            else:
                log.warning(
                    "Sin exportador: la entrega NO se registra (%d avisos siguen disponibles).",
                    len(self.resultado.ids_entregables),
                )
            self._confirmar_entrega(lote="sin_exportador")
            return None

        ruta = invocar(etapa.exportar, self.opt, registros, demo=self.demo)
        self.resultado.archivo_salida = str(ruta) if ruta else None
        self.resultado.marcar("exportacion", "ok" if ruta else "sin_datos")

        try:
            from exportador_csv import ultimas_metricas

            self.resultado.exportacion = ultimas_metricas()
        except Exception as error:
            log.debug("Sin metricas de exportacion: %s", error)

        if ruta:
            log.info("Archivo final: %s (%d prospectos)", ruta, len(registros))
        else:
            log.warning("El exportador no genero archivo: la entrega no se registra.")
        self._confirmar_entrega(lote=Path(str(ruta)).stem if ruta else "sin_archivo")
        return self.resultado.archivo_salida

    def _validar_contrato_salida(self, registros: Sequence[Dict[str, Any]]) -> None:
        """Verifica el contrato del CSV antes de exportar (calidad A1)."""
        faltantes = {
            columna: sum(1 for registro in registros if not registro.get(columna))
            for columna in COLUMNAS_CRITICAS_PRE_EXPORT
            if any(not registro.get(columna) for registro in registros)
        }
        if faltantes:
            log.error(
                "CONTRATO CSV INCOMPLETO: columnas con valores vacios %s (revise los extractores).",
                faltantes,
            )
        else:
            log.info(
                "Contrato CSV OK: %d columnas, '%s' en primera posicion.",
                len(COLUMNAS_SALIDA),
                COLUMNAS_SALIDA[0],
            )

    def _confirmar_entrega(self, lote: Optional[str]) -> int:
        """FASE 2 de la exclusividad: bloquea los avisos realmente entregados.

        Nunca se registra una entrega que no ocurrio: si el CSV no se genero (o
        no hay modulo exportador), los avisos quedan disponibles.
        """
        if self.sin_registro:
            log.warning(
                "Confirmacion OMITIDA (--sin-registro): %d avisos siguen disponibles.",
                len(self.resultado.ids_entregables),
            )
            return 0

        if not self.resultado.archivo_salida and not self.confirmar_forzado:
            log.warning(
                "No se genero CSV: no se registra la entrega (%d avisos siguen disponibles).",
                len(self.resultado.ids_entregables),
            )
            return 0

        if not self.settings.ventas.registrar_entrega_auto and not self.confirmar_forzado:
            log.info("VENTAS_REGISTRAR_ENTREGA_AUTO=false: entrega no registrada por configuracion.")
            return 0

        urls: Dict[str, str] = {}
        for registro in self.resultado.registros_finales:
            aviso_id = str(registro.get("id_aviso") or "")
            enlace = registro.get("enlace_aviso")
            if aviso_id and enlace:
                urls[aviso_id] = str(enlace)

        registrados = self.registro.confirmar_entrega(
            self.resultado.ids_entregables, lote=lote or "sin_lote", urls=urls
        )
        self.resultado.entregados = registrados
        self.resultado.marcar("entrega_registrada", f"ok:{registrados}")
        return registrados

    # ------------------------------ Utilidades --------------------------- #
    def _filtrar_alcance(
        self, registros: Sequence[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Aplica el filtro geografico estricto (Lima + Callao) y enriquece."""
        alcance = self.settings.alcance
        dentro: List[Dict[str, Any]] = []
        fuera: List[Dict[str, Any]] = []
        for registro in registros or []:
            if not isinstance(registro, dict):
                continue
            ubicacion = alcance.ubicacion_de(registro)
            if ubicacion["pertenece"]:
                copia = dict(registro)
                if ubicacion["distrito"] and not copia.get("distrito"):
                    copia["distrito"] = ubicacion["distrito"]
                if ubicacion["provincia"] and not copia.get("provincia"):
                    copia["provincia"] = ubicacion["provincia"]
                dentro.append(copia)
            else:
                fuera.append(
                    dict(registro, _fuera_de_alcance={"motivo": ubicacion["motivo"], "alcance": alcance.etiqueta})
                )
        if self.incluir_fuera_de_lima:  # modo debug/comparacion
            log.warning("--incluir-fuera-de-lima activo: %d registros se reincorporan.", len(fuera))
            dentro.extend(fuera)
            fuera = []
        return dentro, fuera

    def _cruzar(
        self, grupo1: Sequence[Dict[str, Any]], grupo2: Sequence[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Une los avisos (GRUPO 2) con su empresa (GRUPO 1).

        La vinculacion usa, en orden: RUC explicito, dominio web del vendedor y
        nombre comercial. Un aviso sin empresa NO es un prospecto A1 (no se puede
        entregar con datos fiscales confirmados).
        """
        if not grupo2:
            return [dict(registro) for registro in grupo1]
        if not grupo1:
            self.resultado.vinculados = 0
            self.resultado.sin_empresa = [dict(registro) for registro in grupo2]
            log.warning("CRUCE: no hay empresas del GRUPO 1: %d avisos quedan sin vincular.", len(grupo2))
            return [dict(registro) for registro in grupo2]

        empresas = [registro for registro in grupo1 if isinstance(registro, dict)]
        salida: List[Dict[str, Any]] = []
        vinculados = 0
        sin_empresa: List[Dict[str, Any]] = []
        for aviso in grupo2:
            base = dict(aviso)
            # Identidad canonica garantizada ANTES de historizar: sin id_aviso no
            # hay serie de precios ni control de exclusividad.
            if not base.get("id_aviso"):
                base["id_aviso"] = generar_aviso_id(base)
            empresa = vincular_empresa(base, empresas)
            if empresa:
                base = fusionar_empresa(base, empresa)
                vinculados += 1
            else:
                sin_empresa.append(base)
            salida.append(base)

        self.resultado.vinculados = vinculados
        self.resultado.sin_empresa = sin_empresa
        log.info(
            "CRUCE: %d/%d avisos vinculados a una empresa | %d sin empresa.",
            vinculados,
            len(grupo2),
            len(sin_empresa),
        )
        if sin_empresa:
            ejemplo = sin_empresa[0].get("vendedor") or sin_empresa[0].get("enlace_aviso")
            log.info("  primer aviso sin empresa: %s", str(ejemplo)[:90])
        return salida

    def _volcar_json(self) -> None:
        """Respaldo crudo de la corrida (auditoria y re-proceso sin re-scrapear)."""
        if not self.guardar_json:
            return
        ruta = self.settings.exportacion.carpeta / f"raw_{time.strftime('%Y%m%d_%H%M%S')}.json"
        paquete = {
            "resumen": self.resultado.resumen(),
            "grupo1": self.resultado.registros_grupo1,
            "grupo2": self.resultado.registros_grupo2,
            "finales": self.resultado.registros_finales,
            "fuera_de_alcance": self.resultado.fuera_de_alcance,
            "omitidos_exclusividad": self.resultado.omitidos_exclusividad,
        }
        try:
            ruta.parent.mkdir(parents=True, exist_ok=True)
            with ruta.open("w", encoding="utf-8") as archivo:
                json.dump(paquete, archivo, ensure_ascii=False, indent=2, default=str)
            log.info("Respaldo JSON: %s", ruta)
        except OSError as error:
            log.warning("No se pudo escribir el respaldo JSON: %s", error)

    def _imprimir_resumen(self) -> None:
        """Resumen premium de la corrida (tablas + barra de meta)."""
        resumen = self.resultado.resumen()
        ahorro = self.opt.resumen_ahorro()
        ventas = self.registro.resumen()

        log.info("=" * 78)
        log.info("RESUMEN DE LA CORRIDA")

        filas_corrida = [
            ["Modo", f"{self.opt.pronunciar_modo()}  |  {'demo' if self.demo else 'datos reales'}"],
            ["Duracion", f"{resumen['duracion_segundos']}s"],
            ["GRUPO 1 (empresas)", str(resumen["grupo1_empresas"])],
            ["GRUPO 2 (avisos)", str(resumen["grupo2_avisos"])],
            ["Avisos vinculados a empresa", f"{resumen['avisos_vinculados_empresa']} / {resumen['grupo2_avisos']}"],
            ["Avisos sin empresa", str(resumen["avisos_sin_empresa"])],
            ["Fuera de alcance (Lima/Callao)", str(resumen["fuera_de_alcance"])],
            ["Omitidos por exclusividad", str(resumen["omitidos_por_exclusividad"])],
            [
                "PROSPECTOS A1",
                f"[bold]{resumen['prospectos_finales']}[/] / {resumen['meta_prospectos']}  "
                f"{barra_visual(100.0 * resumen['prospectos_finales'] / max(1, resumen['meta_prospectos']), 24)}"
                + ("  [yellow](meta alcanzada)[/]" if resumen["meta_alcanzada"] else ""),
            ],
            ["Excedente descartado", str(resumen["excedente_descartado"])],
            ["Entregas registradas", str(resumen["entregas_registradas"])],
            ["Archivo final", str(resumen["archivo_salida"] or "-")],
        ]
        tabla("RESUMEN DE LA CORRIDA", ["Metrica", "Valor"], filas_corrida)

        if not self.resumen_completo:
            # Consola minimalista (--auto): solo el resumen y la cartera final.
            # El detalle (historico, ahorro, registro comercial) esta en el log
            # y en `python main.py --telemetria`.
            log.info(
                "Detalle completo (historico, ahorro y registro comercial) en el log; "
                "dashboard: python main.py --telemetria"
            )
            log.info("=" * 78)
            panel_cartera(self.resultado.exportacion, entregados=self.resultado.entregados)
            return

        historico = self.resultado.historico or {}
        if historico:
            tabla(
                f"HISTORICO DE PRECIOS (GRUPO 3) - {historico.get('db', '-')}",
                ["Metrica", "Valor"],
                [
                    ["Avisos historizados", f"{historico.get('avisos_total', 0):,} (con precio: {historico.get('avisos_con_precio', 0):,})"],
                    ["Con bajada de precio", f"{historico.get('avisos_con_bajada', 0):,}  {barra_visual(100.0 * historico.get('avisos_con_bajada', 0) / max(1, historico.get('avisos_total', 1)), 20)}"],
                    ["Bajado acumulado", f"{historico.get('bajado_total_usd', 0):,.0f} USD"],
                    ["Dias publicado (promedio)", f"{historico.get('dias_publicado_promedio', 0)}"],
                    ["Observaciones de precio", f"{historico.get('observaciones_precio', 0):,}"],
                ],
            )

        tabla(
            "AHORRO (cache / proxies)",
            ["Metrica", "Valor"],
            [
                ["Consultas totales", str(ahorro["consultas_totales"])],
                ["Consultas de red (pagadas)", str(ahorro["consultas_red"])],
                ["Ahorradas por cache", str(ahorro["ahorradas_por_cache"])],
                ["Tasa de ahorro", f"{ahorro['tasa_ahorro_pct']}%  {barra_visual(ahorro['tasa_ahorro_pct'], 20)}"],
                ["Creditos gastados", str(ahorro["creditos_gastados"])],
                ["Proxies disponibles", str(ahorro["proxies_disponibles"])],
            ],
        )

        tabla(
            f"REGISTRO COMERCIAL ({ventas.get('db', '-')})",
            ["Metrica", "Valor"],
            [
                ["Avisos entregados", f"{ventas.get('avisos_extraidos_vigentes', 0):,} vigentes | {ventas.get('avisos_extraidos_total', 0):,} historicos"],
                ["Ventas permanentes", f"{ventas.get('ventas_activas', 0):,}"],
                ["Clientes unicos", f"{ventas.get('clientes_unicos', 0):,}"],
                ["Monto activo", f"{ventas.get('monto_activo_usd', 0):,.0f} USD"],
            ],
        )
        log.info("=" * 78)

        # Cierre: la cartera A1 lista para vender.
        panel_cartera(self.resultado.exportacion, entregados=self.resultado.entregados)

    # --------------------------- Datos sinteticos ------------------------ #
    @staticmethod
    def _datos_demo_grupo1() -> List[Dict[str, Any]]:
        return [
            {
                "ruc": "20512345671",
                "razon_social": "INVERSIONES LIMA NORTE S.A.C.",
                "estado_sunat": "ACTIVO",
                "actividad": "ACTIVIDADES INMOBILIARIAS",
                "distrito_fiscal": "SAN ISIDRO",
                "correo_corporativo": "contacto@limanorte.pe",
                "web": "https://www.limanorte.pe",
            },
            {
                "ruc": "20487654320",
                "razon_social": "CONSTRUCTORA ANDINA E.I.R.L.",
                "estado_sunat": "ACTIVO",
                "actividad": "CONSTRUCCION DE EDIFICIOS",
                "distrito_fiscal": "CALLAO",
                "correo_corporativo": None,
                "web": "https://andina-construye.com",
            },
        ]

    def _datos_demo_grupo2(self) -> List[Dict[str, Any]]:
        """Avisos sinteticos que reproducen el caso real completo.

        Incluye a proposito: duplicado con parametros de campaña, un aviso sin
        empresa (dueño directo), un aviso fuera de Lima y precios que pueden
        bajar entre corridas (``--demo-bajada``) para probar el GRUPO 3.
        """
        from datetime import datetime, timedelta, timezone

        def hace(dias: int) -> str:
            return (datetime.now(timezone.utc) - timedelta(days=dias)).isoformat(timespec="seconds")

        limanorte = "INVERSIONES LIMA NORTE S.A.C."
        andina = "CONSTRUCTORA ANDINA E.I.R.L."
        avisos = [
            {"tipo_inmueble": "Departamento", "zona": "San Isidro, Lima", "m2": 120.0, "precio_usd": 185_000.0,
             "vendedor": limanorte, "enlace_aviso": "https://portal-ejemplo.pe/aviso/123456",
             "ruc": "20512345671", "fecha_publicacion": hace(25), "dormitorios": 3, "portal": "demo"},
            {   # MISMO aviso republicado con parametros de campaña -> duplicado
                "tipo_inmueble": "Departamento", "zona": "San Isidro, Lima", "m2": 120.0, "precio_usd": 185_000.0,
                "vendedor": limanorte,
                "enlace_aviso": "https://portal-ejemplo.pe/aviso/123456?utm_source=facebook",
                "ruc": "20512345671", "fecha_publicacion": hace(25), "portal": "demo"},
            {"tipo_inmueble": "Casa", "zona": "Miraflores, Lima", "m2": 210.5, "precio_usd": 320_000.0,
             "vendedor": limanorte, "enlace_aviso": "https://portal-ejemplo.pe/aviso/987654",
             "ruc": "20512345671", "fecha_publicacion": hace(12), "dormitorios": 4, "portal": "demo"},
            {"tipo_inmueble": "Departamento", "zona": "La Molina, Lima", "m2": 150.0, "precio_usd": 240_000.0,
             "vendedor": limanorte, "enlace_aviso": "https://portal-ejemplo.pe/aviso/222111",
             "ruc": "20512345671", "fecha_publicacion": hace(5), "portal": "demo"},
            {"tipo_inmueble": "Departamento", "zona": "Bellavista, Callao", "m2": 95.0, "precio_usd": 128_000.0,
             "vendedor": andina, "enlace_aviso": "https://portal-ejemplo.pe/aviso/444333",
             "ruc": "20487654320", "fecha_publicacion": hace(18), "portal": "demo"},
            {"tipo_inmueble": "Terreno", "zona": "Cieneguilla, Lima", "m2": 1_000.0, "precio_usd": 95_000.0,
             "vendedor": andina, "enlace_aviso": "https://portal-ejemplo.pe/aviso/555111",
             "ruc": "20487654320", "fecha_publicacion": hace(40), "portal": "demo"},
            {"tipo_inmueble": "Local comercial", "zona": "Surquillo, Lima", "m2": 80.0, "precio_usd": 210_000.0,
             "vendedor": andina, "enlace_aviso": "https://portal-ejemplo.pe/aviso/111000",
             "ruc": "20487654320", "fecha_publicacion": hace(3), "portal": "demo"},
            {   # SIN EMPRESA: debe descartarlo la meta A1
                "tipo_inmueble": "Departamento", "zona": "San Miguel, Lima", "m2": 88.0, "precio_usd": 155_000.0,
                "vendedor": "dueño directo", "enlace_aviso": "https://portal-ejemplo.pe/aviso/333222",
                "ruc": None, "fecha_publicacion": hace(9), "portal": "demo"},
            {   # FUERA DE ALCANCE
                "tipo_inmueble": "Departamento", "zona": "Trujillo, La Libertad", "m2": 95.0, "precio_usd": 78_000.0,
                "vendedor": "Inmobiliaria del Norte", "enlace_aviso": "https://portal-ejemplo.pe/aviso/777222",
                "ruc": None, "portal": "demo"},
        ]

        # Simula una corrida posterior donde el mercado bajo sus precios.
        if self.demo_bajada:
            factor = max(0.05, 1.0 - (self.demo_bajada / 100.0))
            for aviso in avisos:
                if aviso.get("precio_usd") and aviso.get("vendedor") == limanorte:
                    aviso["precio_usd"] = round(aviso["precio_usd"] * factor, 2)
        return avisos


# =========================================================================== #
#  Dashboard de telemetria (opcion 3)
# =========================================================================== #
def mostrar_telemetria(opt: Optional[Optimizador] = None, demo: bool = False) -> Dict[str, Any]:
    """Dashboard visual: Serper, proxies y volumen de SQLite."""
    settings = get_settings()
    optimizador = opt or get_optimizador(settings)
    datos = optimizador.telemetria()

    ruta_ventas = settings.ventas.ruta
    if demo and settings.ventas.separar_db_demo:
        ruta_ventas = ruta_ventas.with_name(f"{ruta_ventas.stem}_demo{ruta_ventas.suffix}")
    registro = RegistroVentas(
        ruta=ruta_ventas,
        ventana_dias=settings.ventas.ventana_dias,
        retencion_dias=settings.ventas.retencion_dias,
        politica_error=settings.ventas.politica_error,
        bloqueo_venta_permanente=settings.ventas.bloqueo_venta_permanente,
    )
    try:
        ventas = registro.resumen()
        integridad = registro.validar_integridad()
    finally:
        registro.cerrar()

    serper = datos["serper"]
    proxies = datos["proxies"]
    cache = datos["cache"]

    encabezado(
        "TELEMETRIA Y ESTADO DEL SISTEMA",
        f"entorno={datos['entorno']}  |  {datos['modo']}  |  meta A1: {datos['meta_prospectos']} prospectos",
    )

    # ---------------------------- Contexto ---------------------------- #
    tabla(
        "",
        ["Parametro", "Valor"],
        [
            ["Alcance geografico", datos["alcance"]],
            ["Anclaje de busqueda", datos["sufijo_busqueda"]],
            ["Exclusividad", (
                f"venta PERMANENTE | entregas {ventas.get('ventana_dias', 0):.0f} dias"
                if ventas.get("venta_permanente")
                else f"todo bloquea {ventas.get('ventana_dias', 0):.0f} dias"
            )],
            ["Base comercial", str(ventas.get("db", "-"))],
        ],
    )

    # ---------------------------- SERPER ------------------------------ #
    plan = int(serper["plan_creditos"])
    gastados = int(serper["creditos_gastados"])
    restantes = int(serper["creditos_restantes"])
    restante_pct = 100.0 - float(serper["consumo_pct"]) if plan else 0.0
    operativo = serper["operativo"]
    tabla(
        "SERPER API (Google Search)",
        ["Metrica", "Valor"],
        [
            ["Estado", f"[{'green' if operativo else 'yellow'}]{'OPERATIVO' if operativo else 'INACTIVO'}[/]  (key: {serper['api_key']})"],
            ["Plan contratado", f"{plan:,} creditos"],
            ["Creditos consumidos", f"{gastados:,}"],
            ["Creditos restantes (est.)", f"{restantes:,}  {barra_visual(restante_pct, 24)}  {restante_pct:.1f}%"],
            ["Llamadas de red / cache", f"{serper['llamadas_red']} / {serper['aciertos_cache']}"],
            ["Ahorro por cache", f"{serper['tasa_ahorro_pct']}%"],
            ["Reintentos / errores", f"{serper['reintentos']} / {serper['errores']}"],
            ["Ultimo uso", str(serper["ultimo_uso"] or "nunca")],
            ["Circuit breaker", "[red]ABIERTO[/]" if serper["circuito_abierto"] else "[green]cerrado[/]"],
        ],
    )

    # ---------------------------- PROXIES ----------------------------- #
    if proxies["total"] == 0:
        tabla("PROXIES (rotacion y cuarentena)", ["Metrica", "Valor"],
              [["Pool", "[yellow]sin proxies configurados[/]: conexion directa (IP local expuesta)"]])
    else:
        filas_proxy = []
        for detalle in proxies["detalle"][:10]:
            vivo = detalle["disponible"]
            filas_proxy.append([
                f"[{'green' if vivo else 'red'}]{'VIVO' if vivo else 'BANEADO'}[/]",
                detalle["proxy"],
                str(detalle["usos"]),
                str(detalle["exitos"]),
                str(detalle["fallos"]),
            ])
        tabla(
            f"PROXIES ({proxies['estrategia']}) - vivos: {proxies['vivos']} | baneados: {proxies['baneados']}",
            ["Estado", "Proxy", "Usos", "Exitos", "Fallos"],
            filas_proxy,
            alineaciones=["left", "left", "right", "right", "right"],
        )

    # ------------------------- SQLITE / CACHE ------------------------- #
    ttl_map = cache.get("ttl_segundos", {})
    filas_cache = []
    for nombre, valores in sorted(cache.get("por_namespace", {}).items()):
        ttl = ttl_map.get(nombre)
        filas_cache.append([
            nombre,
            f"{valores['vigentes']:,}",
            f"{valores['entradas']:,}",
            f"{valores['aciertos']:,}",
            "sin expiracion" if ttl is None else f"{ttl / 86400:.0f} dias",
        ])
    tabla(
        f"CACHE SQLITE ({cache.get('backend', '-')}) - {cache.get('vigentes', 0):,} registros vigentes",
        ["Namespace", "Vigentes", "Total", "Aciertos", "TTL"],
        filas_cache or [["(vacia)", "0", "0", "0", "-"]],
        alineaciones=["left", "right", "right", "right", "right"],
    )

    tabla(
        "REGISTRO COMERCIAL (ventas.db)",
        ["Metrica", "Valor"],
        [
            ["Avisos entregados", f"{ventas.get('avisos_extraidos_vigentes', 0):,} vigentes (ventana) | {ventas.get('avisos_extraidos_total', 0):,} historicos"],
            ["Avisos vendidos", f"[cyan]{ventas.get('ventas_activas', 0):,}[/] permanentes | clientes: {ventas.get('clientes_unicos', 0)} | {ventas.get('monto_activo_usd', 0):,.0f} USD"],
            ["Ventas anuladas", f"{ventas.get('ventas_anuladas', 0):,}"],
            ["Integridad comercial", "[green]OK[/]" if integridad["ok"] else f"[red]{len(integridad['alertas'])} ALERTAS[/]"],
        ],
    )
    for alerta in integridad["alertas"][:5]:
        aviso(f"[red][{alerta['tipo']}][/] {alerta['aviso_id'] or '-'}: {alerta['detalle']}", "error")

    # ------------------------- MOTORES POR DOMINIO -------------------- #
    enrutador = datos.get("enrutador") or {}
    if enrutador.get("politicas"):
        filas_motor = []
        for dominio, cadena in enrutador["politicas"].items():
            aprendido = (enrutador.get("motores_aprendidos") or {}).get(dominio)
            filas_motor.append([
                dominio,
                " > ".join(cadena),
                (f"[green]{aprendido}[/]" if aprendido else "[dim]sin probar[/]"),
                "ligero (HTTP)" if cadena[0] == "fetcher" else "navegador (JS)",
            ])
        tabla(
            "ENRUTADOR DE MOTORES POR DOMINIO",
            ["Dominio", "Cadena", "Motor aprendido", "Tipo"],
            filas_motor,
        )

    # ------------------------ HISTORICO DE PRECIOS -------------------- #
    try:
        ruta_hist = settings.portales.historico_ruta
        if demo and settings.ventas.separar_db_demo:
            ruta_hist = ruta_hist.with_name(f"{ruta_hist.stem}_demo{ruta_hist.suffix}")
        from procesador_historico import get_historico

        historico = get_historico(settings, ruta=ruta_hist)
        resumen_hist = historico.resumen()
    except Exception as error:
        resumen_hist = {"error": str(error)}

    if resumen_hist and "error" not in resumen_hist:
        tabla(
            "HISTORICO DE PRECIOS (GRUPO 3)",
            ["Metrica", "Valor"],
            [
                ["Avisos historizados", f"{resumen_hist.get('avisos_total', 0):,} (con precio: {resumen_hist.get('avisos_con_precio', 0):,})"],
                ["Con bajada de precio", f"{resumen_hist.get('avisos_con_bajada', 0):,}"],
                ["Bajado acumulado", f"{resumen_hist.get('bajado_total_usd', 0):,.0f} USD"],
                ["Dias publicado (promedio)", f"{resumen_hist.get('dias_publicado_promedio', 0)}"],
                ["Observaciones de precio", f"{resumen_hist.get('observaciones_precio', 0):,}"],
                ["Base", str(resumen_hist.get("db", "-"))],
            ],
        )
        for fila in resumen_hist.get("top_bajadas", [])[:5]:
            aviso(
                f"{fila.get('id_aviso')} | {fila.get('zona') or '-'}: "
                f"{fila.get('precio_primero'):,.0f} -> {fila.get('precio_ultimo'):,.0f} USD "
                f"(-{fila.get('bajada_total_usd'):,.0f})",
                "ok",
            )

    # -------------------------- ADVERTENCIAS -------------------------- #
    if datos["advertencias"]:
        tabla("CONFIGURACION A REVISAR", ["Aviso"], [[f"[yellow]{texto}[/]"] for texto in datos["advertencias"]])

    aviso(
        "Los creditos restantes se ESTIMAN con SERPER_PLAN_CREDITOS menos el consumo acumulado "
        "en la base local. Verifique el saldo real en serper.dev.",
        "info",
    )

    return {"telemetria": datos, "ventas": ventas, "integridad": integridad}


# =========================================================================== #
#  Menu interactivo
# =========================================================================== #
OPCIONES_MENU = (
    ("1", "Modo Local", "Solo cache y BD SQLite - Costo $0"),
    ("2", "Modo OSINT/Live", "Scrapling + Serper API + Proxies"),
    ("3", "Ver Telemetria", "Estado del sistema, creditos, proxies y SQLite"),
    ("4", "MODO AUTOMATICO", "SUNAT -> Portales -> Historico -> CSV, sin intervencion"),
    ("0", "Salir", "Cierra el pipeline"),
)


def _banner_auto(pipeline: "Pipeline") -> None:
    """Plan de la corrida automatica (una linea, sin tablas)."""
    settings = pipeline.settings
    aviso(
        "Plan: SUNAT (empresas) -> Portales (avisos) -> Cruce + Historico -> CSV final",
        "ok",
    )
    print(
        f"      meta {pipeline.meta} prospectos A1  |  {pipeline.opt.pronunciar_modo()}  |  "
        f"{settings.alcance.etiqueta}  |  portales: {', '.join(settings.portales.portales)}"
    )
    if pipeline.resumen_completo:
        tabla(
            "PLAN DE LA CORRIDA AUTOMATICA",
            ["Parametro", "Valor"],
            [
                ["Modo", pipeline.opt.pronunciar_modo()],
                ["Alcance", settings.alcance.etiqueta],
                ["Meta", f"{pipeline.meta} prospectos A1"],
                ["Portales", ", ".join(settings.portales.portales)],
                ["Consultas SUNAT", ", ".join(settings.sunat.consultas)],
                ["Exclusividad", f"venta permanente | entregas {settings.ventas.ventana_dias:.0f} dias"],
                ["CSV", str(settings.exportacion.carpeta)],
            ],
        )


def ejecutar_automatico(
    demo: bool = False,
    solo_local: bool = False,
    meta: Optional[int] = None,
    modo: str = "completo",
    resumen_completo: bool = False,
    **opciones: Any,
) -> int:
    """Comando unico: extrae, cruza, historiza y exporta sin pedir nada.

    Todas las opciones del CLI se reenvian al pipeline (``--sin-registro``,
    ``--limite``, ``--ventana-dias``, ``--permitir-sin-empresa``...).
    Por defecto la consola queda **minimalista**: solo la tabla RESUMEN DE LA
    CORRIDA y la caja CARTERA A1 LISTA PARA VENTA.
    """
    settings = get_settings()
    pipeline = Pipeline(
        modo=modo,
        meta=meta,
        demo=demo,
        solo_local=solo_local,
        resumen_completo=resumen_completo,
        **opciones,
    )
    _banner_auto(pipeline)
    pipeline.ejecutar_auto()
    return 0


def _preguntar(prompt: str) -> str:
    try:
        return input(prompt).strip()
    except (EOFError, KeyboardInterrupt):
        print()
        return "0"


def _preguntar_meta(por_defecto: int) -> int:
    respuesta = _preguntar(f"  Meta de prospectos exitosos A1 [{por_defecto}]: ")
    if not respuesta:
        return por_defecto
    try:
        return max(1, int(respuesta))
    except ValueError:
        aviso(f"Valor no valido: se usara {por_defecto}", "alerta")
        return por_defecto


def _dibujar_menu(settings: Any, optimizador: Optimizador) -> None:
    """Cabecera de estado + opciones (panel y tabla con rich)."""
    datos = optimizador.telemetria()
    serper = datos["serper"]
    encabezado("PIPELINE B2B INMOBILIARIO PERU v2.0", f"entorno: {settings.entorno}")

    tabla(
        "ESTADO DEL SISTEMA",
        ["Parametro", "Valor"],
        [
            ["Alcance", datos["alcance"]],
            ["Exclusividad", (
                f"venta PERMANENTE | entregas {settings.ventas.ventana_dias:.0f} dias"
                if settings.ventas.bloqueo_venta_permanente
                else f"bloqueo {settings.ventas.ventana_dias:.0f} dias"
            )],
            ["Meta de prospectos (A1)", str(datos["meta_prospectos"])],
            ["Serper", (
                f"[green]OPERATIVO[/]  creditos restantes: {serper['creditos_restantes']:,}"
                if serper["operativo"]
                else "[yellow]INACTIVO[/]  (sin API key o sin red)"
            )],
            ["Proxies", (
                f"{datos['proxies']['vivos']} vivos / {datos['proxies']['baneados']} baneados"
                if datos["proxies"]["total"]
                else "[yellow]sin configurar[/] (conexion directa)"
            )],
            ["Base comercial", str(settings.ventas.ruta)],
        ],
    )

    tabla(
        "OPCIONES",
        ["#", "Accion", "Descripcion"],
        [
            [f"[bold cyan]{clave}[/]", f"[bold]{nombre}[/]", descripcion]
            for clave, nombre, descripcion in OPCIONES_MENU
        ],
        alineaciones=["center", "left", "left"],
    )


def _ejecutar_desde_menu(modo: str, demo: bool, solo_local: bool, meta: int) -> None:
    if not solo_local:
        settings = get_settings()
        if not settings.serper.configurado:
            aviso("SERPER_API_KEY no configurada: las busquedas web fallaran.", "alerta")
        confirmacion = _preguntar("  El Modo LIVE usa Serper y proxies (costo real). Continuar? [s/N]: ").lower()
        if confirmacion not in {"s", "si", "sí", "y", "yes"}:
            aviso("Cancelado. No se consumio nada.", "info")
            return

    pipeline = Pipeline(modo=modo, meta=meta, demo=demo, solo_local=solo_local)
    with estado_tarea(f"Ejecutando pipeline en modo {pipeline.opt.pronunciar_modo()}..."):
        pipeline.ejecutar()
    aviso("Corrida finalizada. Volviendo al menu...", "ok")


def menu_interactivo() -> int:
    """Menu principal en bucle hasta que el usuario sale."""
    settings = get_settings()
    configurar_logging(
        nivel=settings.logs.nivel,
        carpeta=settings.logs.carpeta,
        archivo=settings.logs.archivo,
        consola=settings.logs.consola,
        consola_nivel=settings.logs.consola_nivel,
        archivo_nivel=settings.logs.archivo_nivel,
        rotacion_mb=settings.logs.rotacion_mb,
        respaldos=settings.logs.respaldos,
        forzar=True,
    )
    optimizador = get_optimizador(settings)

    if not sys.stdin.isatty():
        encabezado("PIPELINE B2B INMOBILIARIO v2.0")
        for clave, nombre, descripcion in OPCIONES_MENU:
            print(f"  [{clave}] {nombre}  -  {descripcion}")
        aviso("Sin terminal interactiva: use los argumentos CLI, por ejemplo", "alerta")
        print("      python main.py --modo local --demo")
        print("      python main.py --modo live")
        print("      python main.py --telemetria")
        return 0

    while True:
        _dibujar_menu(settings, optimizador)
        opcion = _preguntar("\n  Seleccione una opcion: ")

        if opcion == "0":
            aviso("Hasta luego.", "ok")
            cerrar_optimizador()
            return 0
        if opcion in {"1", "2"}:
            solo_local = opcion == "1"
            meta = _preguntar_meta(settings.meta.limite_prospectos)
            demo = _preguntar("  Usar datos de prueba (demo)? [s/N]: ").lower() in {"s", "si", "sí", "y"}
            _ejecutar_desde_menu("local" if solo_local else "live", demo, solo_local, meta)
        elif opcion == "3":
            mostrar_telemetria(optimizador, demo=False)
        elif opcion == "4":
            # Comando unico: sin preguntas, con la meta configurada en el .env.
            demo = _preguntar("  Usar datos de prueba (demo)? [s/N]: ").lower() in {"s", "si", "sí", "y"}
            ejecutar_automatico(demo=demo, solo_local=False, meta=settings.meta.limite_prospectos)
            aviso("Corrida automatica finalizada.", "ok")
        else:
            aviso("Opcion no valida. Intente de nuevo.", "error")


# =========================================================================== #
#  CLI
# =========================================================================== #
def construir_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Pipeline B2B inmobiliario Peru v2.0 (recoleccion + procesamiento)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--modo",
        choices=["completo", "local", "live", "sunat", "portales", "exportar", "diagnostico", "telemetria"],
        default="completo",
        help="Etapas a ejecutar. 'local' = sin red (costo $0); 'live' = OSINT completo.",
    )
    parser.add_argument("--menu", action="store_true", help="Abre el menu interactivo.")
    parser.add_argument(
        "--auto",
        action="store_true",
        help="COMANDO UNICO: ejecuta todo el pipeline en secuencia (SUNAT -> portales -> historico -> CSV) sin intervencion.",
    )
    parser.add_argument(
        "--resumen-completo",
        action="store_true",
        help="En --auto, muestra tambien las tablas de historico, ahorro y registro comercial.",
    )
    parser.add_argument("--telemetria", action="store_true", help="Muestra el dashboard de estado.")
    parser.add_argument("--limite", type=int, default=None, help="Tope de registros por etapa.")
    parser.add_argument("--meta", type=int, default=None, help="Meta estricta de prospectos A1 (default 10).")
    parser.add_argument("--demo", action="store_true", help="Ejecuta con datos sinteticos (sin red).")
    parser.add_argument("--guardar-json", action="store_true", help="Respalda la corrida cruda en JSON.")
    parser.add_argument("--sin-cache", action="store_true", help="Ignora la cache (modo debug).")
    parser.add_argument(
        "--ventana-dias", type=float, default=None, help="Dias de exclusividad de las entregas."
    )
    parser.add_argument("--sin-registro", action="store_true", help="Dry-run: no confirma la entrega.")
    parser.add_argument(
        "--confirmar-entrega", action="store_true", help="Confirma la entrega aunque no se genere CSV."
    )
    parser.add_argument(
        "--incluir-fuera-de-lima",
        action="store_true",
        help="DEBUG: reincorpora registros fuera del alcance (no usar en produccion).",
    )
    parser.add_argument(
        "--permitir-sin-empresa",
        action="store_true",
        help="Acepta avisos sin empresa vinculada (baja la calidad A1 del prospecto).",
    )
    parser.add_argument(
        "--demo-bajada",
        type=float,
        default=0.0,
        metavar="PCT",
        help="DEMO: baja los precios de los avisos demo en PCT%% (prueba el GRUPO 3).",
    )
    parser.add_argument("--log-level", default=None, help="DEBUG | INFO | WARNING | ERROR")
    parser.add_argument("--version", action="version", version="pipeline-inmobiliario 2.0.0")
    return parser


def _modos_a_ejecutar(argumentos: argparse.Namespace) -> Tuple[str, bool, bool]:
    """Traduce la opcion CLI a ``(modo, solo_local, demo)``."""
    modo = argumentos.modo
    solo_local = modo == "local"
    if modo in {"local", "live"}:
        modo = "completo"
    return modo, solo_local, bool(argumentos.demo)


def _ejecutar(argumentos: argparse.Namespace) -> int:
    settings = get_settings()
    configurar_logging(
        nivel=argumentos.log_level or settings.logs.nivel,
        carpeta=settings.logs.carpeta,
        archivo=settings.logs.archivo,
        consola=settings.logs.consola,
        consola_nivel=settings.logs.consola_nivel,
        archivo_nivel=settings.logs.archivo_nivel,
        rotacion_mb=settings.logs.rotacion_mb,
        respaldos=settings.logs.respaldos,
        forzar=True,
    )

    if argumentos.telemetria or argumentos.modo == "telemetria":
        mostrar_telemetria(get_optimizador(settings), demo=bool(argumentos.demo))
        cerrar_optimizador()
        return 0

    modo, solo_local, demo = _modos_a_ejecutar(argumentos)

    # --- COMANDO UNICO (--auto): todo en secuencia, sin intervencion ---
    if argumentos.auto:
        log.info("Comando unico --auto | modo=%s | local=%s | demo=%s", modo, solo_local, demo)
        return ejecutar_automatico(
            demo=demo,
            solo_local=solo_local,
            meta=argumentos.meta,
            modo=modo,
            resumen_completo=argumentos.resumen_completo,
            limite=argumentos.limite,
            guardar_json=argumentos.guardar_json,
            usar_cache=not argumentos.sin_cache,
            sin_registro=argumentos.sin_registro,
            confirmar_forzado=argumentos.confirmar_entrega,
            incluir_fuera_de_lima=argumentos.incluir_fuera_de_lima,
            ventana_dias=argumentos.ventana_dias,
            permitir_sin_empresa=argumentos.permitir_sin_empresa,
            demo_bajada=argumentos.demo_bajada,
        )

    if argumentos.modo == "diagnostico":
        optimizador = get_optimizador(settings)
        optimizador.establecer_modo(solo_local)
        informe = optimizador.diagnostico(verificar_proxies=False)
        informe["telemetria"] = optimizador.telemetria()
        print(json.dumps(informe, indent=2, ensure_ascii=False, default=str))
        cerrar_optimizador()
        return 0

    log.info(
        "Inicio | entorno=%s | %s | alcance=%s",
        settings.entorno,
        "LOCAL" if solo_local else "LIVE",
        settings.alcance.etiqueta,
    )

    pipeline = Pipeline(
        modo=modo,
        limite=argumentos.limite,
        meta=argumentos.meta,
        demo=demo,
        solo_local=solo_local,
        guardar_json=argumentos.guardar_json,
        usar_cache=not argumentos.sin_cache,
        sin_registro=argumentos.sin_registro,
        confirmar_forzado=argumentos.confirmar_entrega,
        incluir_fuera_de_lima=argumentos.incluir_fuera_de_lima,
        ventana_dias=argumentos.ventana_dias,
        permitir_sin_empresa=argumentos.permitir_sin_empresa,
        demo_bajada=argumentos.demo_bajada,
    )
    pipeline.ejecutar()
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    argumentos_crudos = list(argv) if argv is not None else sys.argv[1:]

    try:
        if not argumentos_crudos:
            return menu_interactivo()

        argumentos = construir_parser().parse_args(argumentos_crudos)
        if argumentos.menu:
            return menu_interactivo()
        return _ejecutar(argumentos)

    except KeyboardInterrupt:
        log.warning("Ejecucion interrumpida por el usuario.")
        return 130
    except Exception as error:
        log.critical("Fallo fatal no controlado: %s", error, exc_info=True)
        return 1
    finally:
        cerrar_optimizador()


if __name__ == "__main__":
    sys.exit(main())
