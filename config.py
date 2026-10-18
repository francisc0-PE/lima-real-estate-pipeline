"""
config.py
=========
Fuente unica de verdad de la configuracion del pipeline (patron 12-factor).

Reglas de diseno
----------------
* Todo valor sensible o configurable vive en el archivo ``.env``.
* **Nunca** se rompe la ejecucion por una variable faltante o mal tipada:
  se usa el valor por defecto y se reporta en :meth:`Settings.advertencias`.
* Inmutable (``frozen``) y cacheado: :func:`get_settings` devuelve siempre la
  misma instancia, evitando re-leer el disco en cada import.

Uso
---
    from config import get_settings
    cfg = get_settings()
    print(cfg.serper.endpoint, cfg.proxies.estrategia)
"""

from __future__ import annotations

import os
import re
import unicodedata
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote_plus

try:  # dependencia opcional: si no esta, se leen solo las variables del sistema
    from dotenv import load_dotenv
except ModuleNotFoundError:  # pragma: no cover

    def load_dotenv(*_args: Any, **_kwargs: Any) -> bool:  # type: ignore[no-redef]
        return False


RAIZ_PROYECTO: Path = Path(__file__).resolve().parent
ARCHIVO_ENV: Path = RAIZ_PROYECTO / ".env"

_VERDADEROS = {"1", "true", "yes", "y", "si", "sí", "on", "verdadero"}
_FALSOS = {"0", "false", "no", "n", "off", "falso"}


# --------------------------------------------------------------------------- #
#  Lectores tolerantes a fallos
# --------------------------------------------------------------------------- #
def _texto(clave: str, por_defecto: str = "") -> str:
    """Lee un string del entorno, limpiando comillas y espacios."""
    valor = os.getenv(clave)
    if valor is None:
        return por_defecto
    valor = valor.strip().strip('"').strip("'").strip()
    return valor or por_defecto


def _entero(
    clave: str,
    por_defecto: int,
    minimo: Optional[int] = None,
    maximo: Optional[int] = None,
) -> int:
    crudo = _texto(clave)
    if not crudo:
        return por_defecto
    try:
        valor = int(float(crudo))
    except (TypeError, ValueError):
        return por_defecto
    if minimo is not None:
        valor = max(minimo, valor)
    if maximo is not None:
        valor = min(maximo, valor)
    return valor


def _flotante(
    clave: str,
    por_defecto: float,
    minimo: Optional[float] = None,
    maximo: Optional[float] = None,
) -> float:
    crudo = _texto(clave)
    if not crudo:
        return por_defecto
    try:
        valor = float(crudo)
    except (TypeError, ValueError):
        return por_defecto
    if minimo is not None:
        valor = max(minimo, valor)
    if maximo is not None:
        valor = min(maximo, valor)
    return valor


def _booleano(clave: str, por_defecto: bool = False) -> bool:
    crudo = _texto(clave).lower()
    if not crudo:
        return por_defecto
    if crudo in _VERDADEROS:
        return True
    if crudo in _FALSOS:
        return False
    return por_defecto


_MOTORES_VALIDOS = {"fetcher", "dynamic"}


def _motores_por_dominio(clave: str) -> Dict[str, Tuple[str, ...]]:
    """Lee el enrutamiento de motores: ``dominio|motor1,motor2;dominio2|motor3``.

    ```text
    SCRAPER_MOTORES=urbania.pe|fetcher;adondevivir.com|dynamic
    ```
    """
    crudo = _texto(clave)
    if not crudo:
        return {}
    politicas: Dict[str, Tuple[str, ...]] = {}
    for entrada in re.split(r"[;\n]+", crudo):
        if "|" not in entrada:
            continue
        dominio, _, motores = entrada.partition("|")
        dominio = dominio.strip().lower().lstrip(".")
        if dominio.startswith("www."):
            dominio = dominio[4:]
        cadena = tuple(
            motor.strip().lower() for motor in re.split(r"[,>]+", motores) if motor.strip().lower() in _MOTORES_VALIDOS
        )
        if dominio and cadena:
            politicas[dominio] = cadena
    return politicas


def _lista(clave: str, separador: str = ",") -> Tuple[str, ...]:
    """Lee una lista separada por comas, punto y coma o saltos de linea."""
    crudo = _texto(clave)
    if not crudo:
        return ()
    normalizado = crudo.replace("\r", separador).replace("\n", separador).replace(";", separador)
    return tuple(item.strip() for item in normalizado.split(separador) if item.strip())


def _ruta(clave: str, por_defecto: str) -> Path:
    """Lee una ruta y la resuelve contra la raiz del proyecto si es relativa."""
    ruta = Path(_texto(clave, por_defecto)).expanduser()
    return ruta if ruta.is_absolute() else (RAIZ_PROYECTO / ruta)


def mascarar(valor: Optional[str], visibles: int = 4) -> str:
    """Ofusca un secreto para poder imprimirlo en logs sin filtrarlo."""
    if not valor:
        return "(vacio)"
    if len(valor) <= visibles:
        return "*" * len(valor)
    return f"{valor[:visibles]}{'*' * (len(valor) - visibles)}"


# --------------------------------------------------------------------------- #
#  Catalogo geografico: Provincia de Lima (43 distritos) + Callao
# --------------------------------------------------------------------------- #
DISTRITOS_LIMA: Tuple[str, ...] = (
    "Ancón",
    "Ate",
    "Barranco",
    "Breña",
    "Carabayllo",
    "Chaclacayo",
    "Chorrillos",
    "Cieneguilla",
    "Comas",
    "El Agustino",
    "Independencia",
    "Jesús María",
    "La Molina",
    "La Victoria",
    "Lima",
    "Lince",
    "Los Olivos",
    "Lurigancho-Chosica",
    "Lurín",
    "Magdalena del Mar",
    "Miraflores",
    "Pachacámac",
    "Pucusana",
    "Pueblo Libre",
    "Puente Piedra",
    "Punta Hermosa",
    "Punta Negra",
    "Rímac",
    "San Bartolo",
    "San Borja",
    "San Isidro",
    "San Juan de Lurigancho",
    "San Juan de Miraflores",
    "San Luis",
    "San Martín de Porres",
    "San Miguel",
    "Santa Anita",
    "Santa María del Mar",
    "Santa Rosa",
    "Santiago de Surco",
    "Surquillo",
    "Villa El Salvador",
    "Villa María del Triunfo",
)

# Variantes reales que usan SUNAT y los portales (claves ya normalizadas).
ALIAS_LIMA: Dict[str, str] = {
    "ate vitarte": "Ate",
    "vitarte": "Ate",
    "surco": "Santiago de Surco",
    "santiago de surco": "Santiago de Surco",
    "monterrico": "Santiago de Surco",
    "chosica": "Lurigancho-Chosica",
    "lurigancho chosica": "Lurigancho-Chosica",
    "cercado de lima": "Lima",
    "lima cercado": "Lima",
    "lima metropolitana": "Lima",
    "lima prov": "Lima",
    "provincia de lima": "Lima",
    "lima province": "Lima",
    "magdalena": "Magdalena del Mar",
    "agustino": "El Agustino",
    "el rimac": "Rímac",
    "ves": "Villa El Salvador",
    "vmt": "Villa María del Triunfo",
    "sjl": "San Juan de Lurigancho",
    "sjm": "San Juan de Miraflores",
    "santa maria": "Santa María del Mar",
    "maranga": "San Miguel",
    "pachacamac": "Pachacámac",
    "brena": "Breña",
    "jesus maria": "Jesús María",
    "san martin": "San Martín de Porres",
}

# Distritos de la Provincia Constitucional del Callao (fuera de Lima por defecto).
DISTRITOS_CALLAO: Tuple[str, ...] = (
    "Bellavista",
    "Callao",
    "Carmen de la Legua Reynoso",
    "La Perla",
    "La Punta",
    "Mi Perú",
    "Ventanilla",
)

# Regiones/provincias que NO son la ciudad de Lima.
# Ojo: incluye las provincias de la region Lima que quedan fuera de Lima
# Metropolitana (Barranca, Huaral, Canete...), muy comunes en avisos de portales.
REGIONES_FUERA_DE_LIMA: Tuple[str, ...] = (
    # Departamentos del Peru
    "amazonas", "ancash", "apurimac", "arequipa", "ayacucho", "cajamarca",
    "cusco", "huancavelica", "huanuco", "ica", "junin", "la libertad",
    "lambayeque", "loreto", "madre de dios", "moquegua", "pasco", "piura",
    "puno", "san martin", "tacna", "tumbes", "ucayali",
    # Ciudades frecuentes en avisos inmobiliarios
    "trujillo", "chiclayo", "piura", "arequipa", "cusco", "huancayo", "ica",
    "tacna", "pucallpa", "iquitos", "chimbote", "huaraz", "cajamarca",
    "juliaca", "puno", "tarapoto", "chincha", "pisco", "sullana", "talara",
    "moquegua", "abancay", "huanuco", "cerro de pasco", "puerto maldonado",
    "tumbes", "mollendo", "chachapoyas",
    # Provincias de la region Lima, fuera de Lima Metropolitana
    "barranca", "paramonga", "pativilca", "huacho", "huaral", "chancay",
    "canta", "cajatambo", "oyon", "yauyos", "canete", "lunahuana",
    "san vicente de canete", "imperial", "nuevo imperial", "cerro azul",
)

# Provincias de la REGION Lima que NO pertenecen a Lima Metropolitana.
# A diferencia de REGIONES_FUERA_DE_LIMA, estos nombres se buscan en TODO el
# texto porque los portales etiquetan la region simplemente como "Lima"
# (ej: "Huacho, Lima" es provincia de Huaral, no Lima Metropolitana).
# Criterio A1: ante la duda se descarta el aviso (fail-closed).
PROVINCIAS_REGION_LIMA_EXTERNAS: Tuple[str, ...] = (
    "barranca", "paramonga", "pativilca", "supe", "huacho", "huaral",
    "chancay", "canta", "cajatambo", "oyon", "yauyos", "canete",
    "san vicente de canete", "lunahuana", "nuevo imperial", "cerro azul",
)


def _normalizar_frase(valor: Any) -> str:
    """Normaliza texto para comparar ubicaciones *conservando* los espacios.

    A diferencia de :func:`normalizar_texto`, aqui si importa la separacion de
    palabras ("San Juan de Lurigancho" no debe confundirse con "San Juan").
    """
    if valor is None:
        return ""
    texto = unicodedata.normalize("NFKD", str(valor))
    texto = "".join(caracter for caracter in texto if not unicodedata.combining(caracter))
    texto = texto.lower()
    for separador in ("-", "/", ",", ".", "(", ")", "|", "#"):
        texto = texto.replace(separador, " ")
    texto = re.sub(r"[^a-z0-9 ]+", " ", texto)
    return re.sub(r"\s+", " ", texto).strip()


def _contiene_frase(frase: str, subcadena: str) -> bool:
    """Busca una frase respetando limites de palabra (evita "ate" en "o late")."""
    if not frase or not subcadena:
        return False
    patron = r"(?<![a-z0-9])" + re.escape(subcadena) + r"(?![a-z0-9])"
    return re.search(patron, frase) is not None


@dataclass(frozen=True)
class AlcanceGeografico:
    """Filtro geografico estricto del producto comercial.

    Regla de negocio A1: por defecto **solo la Provincia de Lima (Peru)**.
    Toda logica de extraccion debe validar sus registros con :meth:`evaluar`
    antes de darlos por buenos.
    """

    pais: str = "PE"
    departamento: str = "LIMA"
    provincia: str = "LIMA"
    distritos: Tuple[str, ...] = ()          # vacio = los 43 distritos de Lima
    incluir_callo: bool = True               # Callao = area metropolitana
    gl: str = "pe"
    hl: str = "es"

    # ------------------------------ Derivados ---------------------------- #
    @property
    def catalogo(self) -> Tuple[str, ...]:
        """Distritos validos de la provincia de Lima."""
        return self.distritos or DISTRITOS_LIMA

    @property
    def catalogo_efectivo(self) -> Tuple[str, ...]:
        """Catalogo util para detectar distritos (incluye Callao si aplica)."""
        return self.catalogo + DISTRITOS_CALLAO if self.incluir_callo else self.catalogo

    @property
    def etiqueta(self) -> str:
        partes = [f"{self.provincia.title()}, {self.departamento.title()} ({len(self.catalogo)} distritos)"]
        if self.incluir_callo:
            partes.append(f"Callao ({len(DISTRITOS_CALLAO)} distritos)")
        return " + ".join(partes)

    @property
    def sufijo_busqueda(self) -> str:
        """Sufijo obligatorio a anexar a toda consulta (Serper y portales)."""
        if self.distritos:
            base = ", ".join(distrito.title() for distrito in self.distritos) + f", {self.provincia.title()}"
        elif self.provincia.title() == self.departamento.title():
            base = self.provincia.title()
        else:
            base = f"{self.provincia.title()}, {self.departamento.title()}"
        if self.incluir_callo and "callao" not in base.lower():
            base = f"{base} y Callao"
        return base

    @property
    def gl_hl(self) -> Tuple[str, str]:
        """Region e idioma para la Serper API."""
        return self.gl, self.hl

    def consulta(self, termino: str) -> str:
        """Compone una consulta geograficamente anclada a Lima.

        >>> get_settings().alcance.consulta("constructora")
        'constructora Lima, LIMA'
        """
        termino = (termino or "").strip()
        return f"{termino} {self.sufijo_busqueda}".strip()

    # --------------------------- Deteccion ------------------------------- #
    def distrito_de(self, texto: Any) -> Optional[str]:
        """Devuelve el distrito del alcance detectado en un texto libre, o ``None``."""
        frase = _normalizar_frase(texto)
        if not frase:
            return None
        # 1) Variantes conocidas primero: son mas especificas que el catalogo
        #    (si no, "Surco, Lima" devolveria "Lima" en vez de "Santiago de Surco").
        for alias, distrito in sorted(ALIAS_LIMA.items(), key=lambda par: len(par[0]), reverse=True):
            if distrito in self.catalogo_efectivo and _contiene_frase(frase, alias):
                return distrito
        # 2) Distritos del catalogo (los mas largos primero para no cortar frases).
        for distrito in sorted(self.catalogo_efectivo, key=len, reverse=True):
            if _contiene_frase(frase, _normalizar_frase(distrito)):
                return distrito
        return None

    def provincia_de(self, distrito: Any) -> Optional[str]:
        """Provincia a la que pertenece un distrito del alcance."""
        if not distrito:
            return None
        clave = _normalizar_frase(distrito)
        for candidato in DISTRITOS_CALLAO:
            if clave == _normalizar_frase(candidato):
                return "CALLAO"
        for candidato in DISTRITOS_LIMA:
            if clave == _normalizar_frase(candidato):
                return self.provincia
        return None

    def es_callo(self, texto: Any) -> bool:
        frase = _normalizar_frase(texto)
        if not frase:
            return False
        return any(_contiene_frase(frase, _normalizar_frase(d)) for d in DISTRITOS_CALLAO)

    @staticmethod
    def region_final(texto: Any) -> str:
        """Ultimo segmento de una ubicacion al estilo portal: "Miraflores, Lima"."""
        if not texto:
            return ""
        return _normalizar_frase(str(texto).replace("|", ",").split(",")[-1])

    def es_region_foranea(self, texto: Any) -> Optional[str]:
        """Devuelve la region detectada si esta fuera de Lima, o ``None``."""
        region = self.region_final(texto)
        if not region:
            return None
        for candidata in REGIONES_FUERA_DE_LIMA:
            if _contiene_frase(region, candidata):
                # El propio nombre del alcance siempre tiene prioridad.
                if _contiene_frase(region, _normalizar_frase(self.provincia)):
                    return None
                return candidata
        return None

    def es_provincia_externa(self, texto: Any) -> Optional[str]:
        """Detecta provincias de la region Lima ajenas a Lima Metropolitana.

        Se escanea TODO el texto (no solo el final) porque el portal suele
        reportar la ciudad como "Lima" aunque el inmueble este en Huacho,
        Huaral, Canete, etc.
        """
        frase = _normalizar_frase(texto)
        if not frase:
            return None
        for candidata in sorted(PROVINCIAS_REGION_LIMA_EXTERNAS, key=len, reverse=True):
            if _contiene_frase(frase, candidata):
                return candidata
        return None

    def evaluar(
        self,
        zona: Optional[str] = None,
        distrito: Optional[str] = None,
        provincia: Optional[str] = None,
        departamento: Optional[str] = None,
    ) -> Tuple[bool, Optional[str], str]:
        """Valida si un registro pertenece al alcance.

        Orden de autoridad: distrito > departamento > provincia > texto libre.
        Los campos explicitos tienen prioridad, de modo que un dato
        contradictorio (p.ej. distrito "Trujillo") se rechaza aunque la zona
        mencione "Lima".

        Devuelve ``(pertenece, distrito_detectado, motivo)``.
        """
        # 1) Distrito explicito: manda sobre todo lo demas.
        if distrito:
            detectado = self.distrito_de(distrito)
            if detectado:
                provincia = self.provincia_de(detectado) or self.provincia
                return True, detectado, f"distrito de {provincia.title()}: {detectado}"
            if self.incluir_callo and self.es_callo(distrito):
                return True, distrito, f"Callao incluido por configuracion: {distrito}"
            return False, None, f"distrito fuera de Lima: {distrito}"

        # 2) Departamento explicito.
        if departamento:
            frase = _normalizar_frase(departamento)
            if _contiene_frase(frase, "lima"):
                detectado = self.distrito_de(zona)
                return True, detectado, f"departamento de Lima: {departamento}"
            if self.incluir_callo and self.es_callo(departamento):
                return True, self.distrito_de(departamento), "Callao incluido por configuracion"
            return False, None, f"departamento fuera de Lima: {departamento}"

        # 3) Provincia explicita.
        if provincia:
            frase = _normalizar_frase(provincia)
            if _contiene_frase(frase, "lima"):
                return True, self.distrito_de(zona), f"provincia de Lima: {provincia}"
            if self.incluir_callo and self.es_callo(provincia):
                return True, self.distrito_de(zona), "Provincia Constitucional del Callao"
            return False, None, f"provincia fuera de Lima: {provincia}"

        # 4) Ultimo recurso: evidencia textual en la zona.
        libre = (zona or "").strip()
        if not libre:
            return False, None, "sin dato geografico"
        # 4.a) Veto por region final ("Av. Lima 123, Trujillo" -> Trujillo).
        foranea = self.es_region_foranea(libre)
        if foranea:
            return False, None, f"region fuera de Lima: {foranea}"
        # 4.b) Veto por provincia externa de la region Lima ("Huacho, Lima").
        provincia_externa = self.es_provincia_externa(libre)
        if provincia_externa:
            return False, None, f"provincia de la region Lima fuera de Lima Metropolitana: {provincia_externa}"
        detectado = self.distrito_de(libre)
        if detectado:
            provincia = self.provincia_de(detectado) or self.provincia
            return True, detectado, f"distrito de {provincia.title()} detectado en el texto: {detectado}"
        if self.incluir_callo and self.es_callo(libre):
            return True, None, "Callao detectado en el texto (incluido)"
        return False, None, "sin evidencia de Lima en el texto"

    def es_lima(self, **campos: Optional[str]) -> bool:
        """Atajo booleano de :meth:`evaluar`."""
        return self.evaluar(**campos)[0]

    def normalizar_registro(self, registro: Dict[str, Any]) -> Tuple[bool, str, Optional[str]]:
        """Valida un registro proveniente de un extractor.

        Reconoce los nombres de campo de ambos grupos (``zona``, ``distrito_fiscal``,
        ``distrito``, ``provincia``, ``departamento``) y devuelve
        ``(pertenece, motivo, distrito)``.
        """
        if not isinstance(registro, dict):
            return False, "registro invalido", None
        pertenece, distrito_detectado, motivo = self.evaluar(
            zona=registro.get("zona") or registro.get("ubicacion") or registro.get("direccion"),
            distrito=registro.get("distrito") or registro.get("distrito_fiscal"),
            provincia=registro.get("provincia"),
            departamento=registro.get("departamento") or registro.get("region"),
        )
        return pertenece, motivo, distrito_detectado

    def ubicacion_de(self, registro: Dict[str, Any]) -> Dict[str, Any]:
        """Ubicacion normalizada de un registro, lista para enriquecerlo.

        Devuelve ``{"pertenece", "motivo", "distrito", "provincia"}``; los
        extractores y el orquestador la usan para poblar las columnas
        ``distrito`` y ``provincia`` del CSV final.
        """
        if not isinstance(registro, dict):
            return {"pertenece": False, "motivo": "registro invalido", "distrito": None, "provincia": None}
        pertenece, motivo, distrito = self.normalizar_registro(registro)
        return {
            "pertenece": pertenece,
            "motivo": motivo,
            "distrito": distrito,
            "provincia": self.provincia_de(distrito),
        }


@dataclass(frozen=True)
class VentasSettings:
    """Reglas comerciales de exclusividad y control de duplicados."""

    ruta: Path = field(default_factory=lambda: RAIZ_PROYECTO / "data/ventas/ventas.db")
    ventana_dias: float = 30.0          # exclusividad de las ENTREGAS
    retencion_dias: float = 730.0       # historico de extracciones a conservar
    registrar_entrega_auto: bool = True  # confirmar entrega al cerrar el CSV
    separar_db_demo: bool = True        # --demo no contamina la base comercial
    politica_error: str = "cerrado"     # cerrado = ante duda, NO entregar el aviso
    bloqueo_venta_permanente: bool = True  # vendido a un cliente = nunca a otro


@dataclass(frozen=True)
class MetaSettings:
    """Limite estricto de la operacion (control de calidad y de costos)."""

    limite_prospectos: int = 10         # el pipeline se detiene al alcanzarlo
    sobremuestreo: int = 3              # avisos extra por si el filtro descarta


@dataclass(frozen=True)
class PortalesSettings:
    """Configuracion del extractor del GRUPO 2 (avisos) y del historico (GRUPO 3)."""

    # Dominios de portales a consultar (se usan en busquedas "site:").
    portales: Tuple[str, ...] = ("urbania.pe", "adondevivir.com", "laencontre.pe")
    # Terminos de busqueda (SIEMPRE se anclan al alcance: "Lima y Callao").
    consultas: Tuple[str, ...] = (
        "departamento en venta",
        "casa en venta",
        "terreno en venta",
        "local comercial en venta",
    )
    max_por_consulta: int = 10          # resultados organicos por busqueda
    paginas_por_aviso: int = 1          # paginas a visitar por aviso (la ficha)
    # Cupo de fichas por portal en el descubrimiento (0 = automatico:
    # objetivo / nº de portales). Garantiza que TODOS los portales aporten
    # y que ninguno se "coma" la meta del resto.
    cupo_por_portal: int = 0
    # Exclusiones de Google (-inurl:) para que no devuelva paginas de categoria.
    exclusiones: Tuple[str, ...] = (
        "-inurl:buscar", "-inurl:blog", "-inurl:noticias", "-inurl:empresas",
        "-inurl:inmobiliarias", "-inurl:guia", "-inurl:ayuda", "-inurl:calculadora",
    )
    # Como descubrir las fichas:
    #   crawl  -> se rastrea la propia pagina de busqueda del portal (costo $0 en Serper)
    #   serper -> busquedas "site:" en Google (1 credito por portal y termino)
    #   ambos  -> serper y, si falta material, crawl
    descubrir_por: str = "crawl"
    urls_busqueda: Tuple[str, ...] = ()   # "dominio|url" (opcional, sobreescribe los defaults)
    paginas_busqueda: int = 1             # paginas de categoria a recorrer por portal
    # Motor de descarga para los portales: "" = usa SCRAPER_FETCHER global.
    # Poner "dynamic" si algun portal requiere renderizado JS en navegador.
    fetcher: str = ""
    exigir_ubicacion: bool = True       # descartar avisos fuera de Lima/Callao
    exigir_precio: bool = True          # un aviso sin precio no es vendible (A1)
    fx_pen_usd: float = 3.75            # tipo de cambio para convertir soles a USD
    deduplicar_por: str = "url"         # url | huella (identidad del aviso)
    perfiles_json: Optional[Path] = None  # selectores CSS por portal (ajuste fino sin codigo)
    # --- GRUPO 3: historico de precios ---
    historico_ruta: Path = field(default_factory=lambda: RAIZ_PROYECTO / "data/historico/precios.db")
    historico_retencion_dias: float = 730.0
    usar_fecha_publicacion: bool = True  # si el portal publica la fecha, afina dias_publicado


@dataclass(frozen=True)
class SunatSettings:
    """Configuracion del extractor del GRUPO 1 (empresas y contacto)."""

    consultas: Tuple[str, ...] = (
        "inmobiliaria",
        "constructora",
        "desarrolladora inmobiliaria",
        "agente inmobiliario",
        "administradora de inmuebles",
    )
    exigir_ubicacion: bool = True       # descartar empresas sin evidencia Lima/Callao
    urls_consulta: Tuple[str, ...] = () # plantillas de padron con {ruc} (opcional)
    enriquecer_web: bool = True         # visitar la web corporativa buscando correo
    max_por_consulta: int = 10          # resultados organicos por busqueda Serper
    max_busquedas_empresa: int = 1      # consultas extra por empresa (control de costo)
    paginas_por_empresa: int = 2        # paginas web a visitar por empresa
    completitud_minima: int = 60        # % minimo para cachear los 90 dias completos
    ttl_reintento_dias: int = 7         # si esta incompleta, se reintenta en N dias
    rubro_estricto: bool = True         # exigir el rubro en el titulo o el dominio
    # --- Padron de Confianza OFFLINE (respaldo sin red) ---
    padron_local: Path = field(default_factory=lambda: RAIZ_PROYECTO / "padron_local.json")
    padron_local_activo: bool = True    # usarlo si la consulta oficial no esta disponible
    # --- Consulta OFICIAL en el portal de SUNAT (navegador real) ---
    usar_sunat_oficial: bool = True     # dato autoritativo de RUC/estado/actividad
    oficial_timeout_ms: int = 120_000   # margen para cargar el formulario web completo
    oficial_espera_ms: int = 1_000      # espera tras pulsar Buscar (el flujo ya espera selectores)
    oficial_max_intentos: int = 2       # 1 = sin reintentos
    oficial_pausa_segundos: float = 3.0 # cortesia entre consultas (es un servicio publico)
    oficial_enfriamiento_segundos: float = 90.0  # pausa global tras fallos no definitivos


# --------------------------------------------------------------------------- #
#  Contrato del archivo final (una sola fuente de verdad)
# --------------------------------------------------------------------------- #
COLUMNAS_SALIDA: Tuple[str, ...] = (
    "id_aviso",
    # --- GRUPO 1: empresa ---
    "ruc",
    "razon_social",
    "estado_sunat",
    "actividad",
    "distrito_fiscal",
    "correo_corporativo",
    "web",
    # --- GRUPO 2: inmueble ---
    "tipo_inmueble",
    "zona",
    "m2",
    "precio_usd",
    "vendedor",
    "enlace_aviso",
    # --- GRUPO 3: historico ---
    "dias_publicado",
    "bajadas_precio",
    # --- Comercial ---
    "senal",
)

COLUMNAS_EXTRA: Tuple[str, ...] = (
    "distrito",
    "provincia",
    "condicion",
    "ciiu",
    "precio_anterior",
    "precio_minimo",
    "veces_visto",
    "bajadas_veces",
    "empresa_vinculada",
    "portal",
)

COLUMNAS_CRITICAS: Tuple[str, ...] = ("id_aviso", "enlace_aviso", "senal")


def columnas_salida(incluir_extras: bool = False) -> Tuple[str, ...]:
    """Columnas efectivas del CSV (17 por defecto; + extras si se piden)."""
    return COLUMNAS_SALIDA + (COLUMNAS_EXTRA if incluir_extras else ())


# --------------------------------------------------------------------------- #
#  Grupos de configuracion
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class LogSettings:
    nivel: str = "INFO"
    carpeta: Path = field(default_factory=lambda: RAIZ_PROYECTO / "logs")
    archivo: str = "pipeline.log"
    consola: bool = True
    consola_nivel: str = "ERROR"
    archivo_nivel: str = "DEBUG"
    rotacion_mb: int = 10
    respaldos: int = 5


@dataclass(frozen=True)
class SerperSettings:
    api_key: str = ""
    endpoint: str = "https://google.serper.dev"
    rate_limit_rps: float = 1.0
    max_reintentos: int = 3
    timeout: float = 15.0
    retry_base: float = 1.5
    retry_max: float = 30.0
    breaker_umbral: int = 5
    breaker_espera: float = 60.0
    plan_creditos: int = 2500        # creditos contratados (telemetria de gasto)

    @property
    def configurado(self) -> bool:
        """¿Hay una API key usable? (si no, el cliente opera en modo seguro)."""
        return bool(self.api_key and self.api_key.lower() not in {"tu_api_key_aqui", "none"})


@dataclass(frozen=True)
class ProxiesSettings:
    urls: Tuple[str, ...] = ()
    estrategia: str = "round_robin"
    max_fallos: int = 3
    cooldown: float = 300.0
    healthcheck: bool = False
    healthcheck_url: str = "https://api.ipify.org?format=json"
    healthcheck_timeout: float = 8.0


@dataclass(frozen=True)
class CacheSettings:
    habilitado: bool = True
    backend: str = "sqlite"
    ruta: Path = field(default_factory=lambda: RAIZ_PROYECTO / "data/cache/pipeline_cache.db")
    ttl_dias: float = 30.0
    ttl_por_namespace: Dict[str, float] = field(default_factory=dict)
    max_entradas: int = 200_000


# Enrutamiento de motores por dominio:
#   fetcher -> cliente HTTP directo (rapido y ligero)
#   dynamic -> navegador headless para sitios que renderizan con JavaScript
MOTORES_POR_DOMINIO_POR_DEFECTO: Dict[str, Tuple[str, ...]] = {
    "urbania.pe": ("fetcher",),
    "adondevivir.com": ("dynamic",),
    "laencontre.pe": ("dynamic",),
    "properati.com.pe": ("fetcher", "dynamic"),
    "infocasas.pe": ("fetcher", "dynamic"),
}


@dataclass(frozen=True)
class ScraperSettings:
    fetcher: str = "fetcher"
    timeout_ms: int = 30_000
    reintentos: int = 3
    delay_segundos: float = 1.5
    delay_jitter: float = 1.0
    headless: bool = True
    bloquear_recursos: bool = True
    # --- Enrutamiento inteligente de motores por dominio ---
    # Politica: dominio -> cadena de motores a intentar, en orden.
    motores_por_dominio: Dict[str, Tuple[str, ...]] = field(
        default_factory=lambda: dict(MOTORES_POR_DOMINIO_POR_DEFECTO)
    )
    autoescalar: bool = True          # si el motor no sirve, prueba el siguiente de la cadena
    validar_contenido: bool = True    # detecta respuestas incompletas (HTTP 200 con cuerpo vacio)
    min_bytes: int = 120              # contenido minimo para considerar valida la descarga
    espera_dinamica_ms: int = 1_500   # espera extra en navegadores (deja montar el JS)
    network_idle: bool = True         # con navegador: espera a que la red se calme


@dataclass(frozen=True)
class PostgresSettings:
    host: str = "localhost"
    puerto: int = 5432
    base_datos: str = "pipeline_inmobiliario"
    usuario: str = "postgres"
    password: str = "postgres"
    esquema: str = "public"
    timeout: int = 10

    @property
    def dsn(self) -> str:
        """Cadena de conexion lista para psycopg, con credenciales escapadas."""
        return (
            f"postgresql://{quote_plus(self.usuario)}:{quote_plus(self.password)}"
            f"@{self.host}:{self.puerto}/{self.base_datos}"
        )


@dataclass(frozen=True)
class ExportSettings:
    carpeta: Path = field(default_factory=lambda: RAIZ_PROYECTO / "data/salida")
    prefijo: str = "inmobiliaria_b2b"
    nombre_archivo: str = ""
    encoding: str = "utf-8-sig"
    delimitador: str = ";"
    incluir_bom: bool = True
    incluir_extras: bool = False      # anade COLUMNAS_EXTRA al final del CSV
    guardar_reporte: bool = True      # escribe un .json con las metricas de la corrida
    truncar_texto: int = 300          # longitud maxima por celda (Excel-friendly)
    minimo_senal_bajada: float = 1000.0  # bajada minima (USD) para gritar OPORTUNIDAD
    senal_emojis: bool = True         # emojis en la columna senal (🔥 📉 ⏳ 🆕 🤝 🏢)
    senal_umbral_inversion_pct: float = 10.0  # % de rebaja para "OPORTUNIDAD DE INVERSIÓN"
    senal_umbral_tiempo_dias: int = 45        # dias en el mercado para "OPORTUNIDAD POR TIEMPO"


@dataclass(frozen=True)
class Settings:
    """Configuracion completa y validada del pipeline."""

    entorno: str
    raiz: Path
    logs: LogSettings
    serper: SerperSettings
    proxies: ProxiesSettings
    cache: CacheSettings
    scraper: ScraperSettings
    postgres: PostgresSettings
    exportacion: ExportSettings
    alcance: AlcanceGeografico
    ventas: VentasSettings
    meta: MetaSettings
    sunat: SunatSettings
    portales: PortalesSettings

    # ------------------------------------------------------------------ #
    #  Construccion
    # ------------------------------------------------------------------ #
    @classmethod
    def from_env(cls, archivo_env: Optional[Path] = None) -> "Settings":
        """Carga el ``.env`` (una sola vez) y construye la configuracion."""
        ruta_env = Path(archivo_env) if archivo_env else ARCHIVO_ENV
        try:
            load_dotenv(dotenv_path=ruta_env, override=False)
        except Exception:  # el .env nunca debe tumbar el pipeline
            pass

        ttl_ns: Dict[str, float] = {
            "serper": _flotante("CACHE_TTL_DAYS", 30.0, 0.0),
            "empresa": _flotante("CACHE_TTL_DAYS", 30.0, 0.0),
            "sunat": _flotante("CACHE_TTL_SUNAT_DAYS", 90.0, 0.0),
            "portales": _flotante("CACHE_TTL_PORTALES_DAYS", 2.0, 0.0),
            "web": _flotante("CACHE_TTL_WEB_DAYS", 7.0, 0.0),
        }

        return cls(
            entorno=_texto("ENV_NAME", "dev"),
            raiz=RAIZ_PROYECTO,
            logs=LogSettings(
                nivel=_texto("LOG_LEVEL", "INFO").upper(),
                carpeta=_ruta("LOG_DIR", "logs"),
                archivo=_texto("LOG_FILE", "pipeline.log"),
                consola=_booleano("LOG_CONSOLE", True),
                consola_nivel=_texto("LOG_CONSOLE_LEVEL", "ERROR").upper(),
                archivo_nivel=_texto("LOG_FILE_LEVEL", "DEBUG").upper(),
                rotacion_mb=_entero("LOG_ROTATE_MB", 10, 1),
                respaldos=_entero("LOG_BACKUPS", 5, 0),
            ),
            serper=SerperSettings(
                api_key=_texto("SERPER_API_KEY", ""),
                endpoint=_texto("SERPER_ENDPOINT", "https://google.serper.dev").rstrip("/"),
                rate_limit_rps=_flotante("SERPER_RATE_LIMIT_RPS", 1.0, 0.0, 100.0),
                max_reintentos=_entero("SERPER_MAX_RETRIES", 3, 1, 10),
                timeout=_flotante("SERPER_TIMEOUT", 15.0, 1.0, 300.0),
                retry_base=_flotante("SERPER_RETRY_BASE_SECONDS", 1.5, 0.1, 60.0),
                retry_max=_flotante("SERPER_RETRY_MAX_SECONDS", 30.0, 0.1, 600.0),
                breaker_umbral=_entero("SERPER_BREAKER_THRESHOLD", 5, 1, 100),
                breaker_espera=_flotante("SERPER_BREAKER_COOLDOWN", 60.0, 1.0, 3600.0),
            ),
            proxies=ProxiesSettings(
                urls=_lista("PROXY_LIST"),
                estrategia=_texto("PROXY_STRATEGY", "round_robin").lower(),
                max_fallos=_entero("PROXY_MAX_FAILURES", 3, 1, 100),
                cooldown=_flotante("PROXY_COOLDOWN_SECONDS", 300.0, 0.0),
                healthcheck=_booleano("PROXY_HEALTHCHECK_ENABLED", False),
                healthcheck_url=_texto("PROXY_HEALTHCHECK_URL", "https://api.ipify.org?format=json"),
                healthcheck_timeout=_flotante("PROXY_HEALTHCHECK_TIMEOUT", 8.0, 1.0, 120.0),
            ),
            cache=CacheSettings(
                habilitado=_booleano("CACHE_ENABLED", True),
                backend=_texto("CACHE_BACKEND", "sqlite").lower(),
                ruta=_ruta("CACHE_PATH", "./data/cache/pipeline_cache.db"),
                ttl_dias=_flotante("CACHE_TTL_DAYS", 30.0, 0.0),
                ttl_por_namespace=ttl_ns,
                max_entradas=_entero("CACHE_MAX_ENTRIES", 200_000, 0),
            ),
            scraper=ScraperSettings(
                fetcher=_texto("SCRAPER_FETCHER", "fetcher").lower(),
                timeout_ms=_entero("SCRAPER_TIMEOUT_MS", 30_000, 1_000),
                reintentos=_entero("SCRAPER_RETRIES", 3, 0, 10),
                delay_segundos=_flotante("SCRAPER_DELAY_SECONDS", 1.5, 0.0, 600.0),
                delay_jitter=_flotante("SCRAPER_DELAY_JITTER", 1.0, 0.0, 600.0),
                headless=_booleano("SCRAPER_HEADLESS", True),
                bloquear_recursos=_booleano("SCRAPER_DISABLE_RESOURCES", True),
                motores_por_dominio=_motores_por_dominio("SCRAPER_MOTORES") or dict(MOTORES_POR_DOMINIO_POR_DEFECTO),
                autoescalar=_booleano("SCRAPER_AUTOESCALAR", True),
                validar_contenido=_booleano("SCRAPER_VALIDAR_CONTENIDO", True),
                min_bytes=_entero("SCRAPER_MIN_BYTES", 120, 0, 1_000_000),
                espera_dinamica_ms=_entero("SCRAPER_ESPERA_DINAMICA_MS", 1_500, 0, 60_000),
                network_idle=_booleano("SCRAPER_NETWORK_IDLE", True),
            ),
            postgres=PostgresSettings(
                host=_texto("PG_HOST", "localhost"),
                puerto=_entero("PG_PORT", 5432, 1, 65535),
                base_datos=_texto("PG_DATABASE", "pipeline_inmobiliario"),
                usuario=_texto("PG_USER", "postgres"),
                password=_texto("PG_PASSWORD", "postgres"),
                esquema=_texto("PG_SCHEMA", "public"),
                timeout=_entero("PG_CONNECT_TIMEOUT", 10, 1, 120),
            ),
            exportacion=ExportSettings(
                carpeta=_ruta("EXPORT_DIR", "./data/salida"),
                prefijo=_texto("EXPORT_PREFIX", "inmobiliaria_b2b"),
                nombre_archivo=_texto("EXPORT_NOMBRE_ARCHIVO", ""),
                encoding=_texto("EXPORT_ENCODING", "utf-8-sig"),
                delimitador=_texto("EXPORT_DELIMITER", ";"),
                incluir_bom=_booleano("EXPORT_INCLUDE_BOM", True),
                incluir_extras=_booleano("EXPORT_COLUMNAS_EXTRA", False),
                guardar_reporte=_booleano("EXPORT_GUARDAR_REPORTE", True),
                truncar_texto=_entero("EXPORT_TRUNCAR_TEXTO", 300, 40, 5000),
                minimo_senal_bajada=_flotante("EXPORT_MINIMO_BAJADA_SENAL", 1000.0, 0.0, 1_000_000.0),
                senal_emojis=_booleano("EXPORT_SENAL_EMOJIS", True),
                senal_umbral_inversion_pct=_flotante("EXPORT_SENAL_UMBRAL_INVERSION_PCT", 10.0, 0.0, 100.0),
                senal_umbral_tiempo_dias=_entero("EXPORT_SENAL_UMBRAL_TIEMPO_DIAS", 45, 1, 3650),
            ),
            alcance=AlcanceGeografico(
                pais=_texto("GEO_PAIS", "PE").upper(),
                departamento=_texto("GEO_DEPARTAMENTO", "LIMA").upper(),
                provincia=_texto("GEO_PROVINCIA", "LIMA").upper(),
                distritos=_lista("GEO_DISTRITOS"),
                incluir_callo=_booleano("GEO_INCLUIR_CALLAO", True),
                gl=_texto("GEO_SERPER_GL", "pe").lower(),
                hl=_texto("GEO_SERPER_HL", "es").lower(),
            ),
            ventas=VentasSettings(
                ruta=_ruta("VENTAS_DB_PATH", "./data/ventas/ventas.db"),
                ventana_dias=_flotante("EXCLUSIVIDAD_VENTANA_DIAS", 30.0, 1.0, 3650.0),
                retencion_dias=_flotante("VENTAS_RETENCION_DIAS", 730.0, 30.0, 36500.0),
                registrar_entrega_auto=_booleano("VENTAS_REGISTRAR_ENTREGA_AUTO", True),
                separar_db_demo=_booleano("VENTAS_SEPARAR_DB_DEMO", True),
                politica_error=_texto("VENTAS_POLITICA_ERROR", "cerrado").lower(),
                bloqueo_venta_permanente=_booleano("VENTAS_BLOQUEO_VENTA_PERMANENTE", True),
            ),
            meta=MetaSettings(
                limite_prospectos=_entero("META_LIMITE_PROSPECTOS", 10, 1, 100_000),
                sobremuestreo=_entero("META_SOBREMUESTREO", 3, 0, 100),
            ),
            sunat=SunatSettings(
                consultas=_lista("SUNAT_QUERIES") or SunatSettings.consultas,
                exigir_ubicacion=_booleano("SUNAT_EXIGIR_UBICACION", True),
                urls_consulta=_lista("SUNAT_URLS_CONSULTA"),
                enriquecer_web=_booleano("SUNAT_ENRIQUECER_WEB", True),
                max_por_consulta=_entero("SUNAT_MAX_POR_CONSULTA", 10, 1, 100),
                max_busquedas_empresa=_entero("SUNAT_MAX_BUSQUEDAS_EMPRESA", 1, 0, 10),
                paginas_por_empresa=_entero("SUNAT_PAGINAS_POR_EMPRESA", 2, 1, 10),
                completitud_minima=_entero("SUNAT_COMPLETITUD_MINIMA", 60, 0, 100),
                ttl_reintento_dias=_entero("SUNAT_TTL_REINTENTO_DIAS", 7, 1, 365),
                rubro_estricto=_booleano("SUNAT_RUBRO_ESTRICTO", True),
                padron_local=_ruta("SUNAT_PADRON_LOCAL", "./padron_local.json"),
                padron_local_activo=_booleano("SUNAT_PADRON_LOCAL_ACTIVO", True),
                usar_sunat_oficial=_booleano("SUNAT_USAR_OFICIAL", True),
                oficial_timeout_ms=_entero("SUNAT_OFICIAL_TIMEOUT_MS", 120_000, 30_000, 600_000),
                oficial_espera_ms=_entero("SUNAT_OFICIAL_ESPERA_MS", 1_000, 0, 30_000),
                oficial_max_intentos=_entero("SUNAT_OFICIAL_MAX_INTENTOS", 2, 1, 5),
                oficial_pausa_segundos=_flotante("SUNAT_OFICIAL_PAUSA_SEGUNDOS", 3.0, 0.0, 120.0),
                oficial_enfriamiento_segundos=_flotante("SUNAT_OFICIAL_ENFRIAMIENTO_SEGUNDOS", 90.0, 0.0, 3600.0),
            ),
            portales=PortalesSettings(
                portales=_lista("PORTALES_ACTIVOS") or PortalesSettings.portales,
                consultas=_lista("PORTALES_QUERIES") or PortalesSettings.consultas,
                max_por_consulta=_entero("PORTALES_MAX_POR_CONSULTA", 10, 1, 100),
                paginas_por_aviso=_entero("PORTALES_PAGINAS_POR_AVISO", 1, 1, 5),
                cupo_por_portal=_entero("PORTALES_CUPO_POR_PORTAL", 0, 0, 10_000),
                exclusiones=_lista("PORTALES_EXCLUSIONES") or PortalesSettings.exclusiones,
                descubrir_por=_texto("PORTALES_DESCUBRIR_POR", "ambos").lower(),
                urls_busqueda=_lista("PORTALES_URLS_BUSQUEDA"),
                paginas_busqueda=_entero("PORTALES_PAGINAS_BUSQUEDA", 1, 1, 20),
                fetcher=_texto("PORTALES_FETCHER", "").lower(),
                exigir_ubicacion=_booleano("PORTALES_EXIGIR_UBICACION", True),
                exigir_precio=_booleano("PORTALES_EXIGIR_PRECIO", True),
                fx_pen_usd=_flotante("PORTALES_FX_PEN_USD", 3.75, 0.5, 20.0),
                deduplicar_por=_texto("PORTALES_DEDUPLICAR_POR", "url").lower(),
                perfiles_json=(_ruta("PORTALES_PERFILES_JSON", "") if _texto("PORTALES_PERFILES_JSON") else None),
                historico_ruta=_ruta("HISTORICO_DB_PATH", "./data/historico/precios.db"),
                historico_retencion_dias=_flotante("HISTORICO_RETENCION_DIAS", 730.0, 30.0, 36500.0),
                usar_fecha_publicacion=_booleano("PORTALES_USAR_FECHA_PUBLICACION", True),
            ),
        )

    # ------------------------------------------------------------------ #
    #  Utilidades
    # ------------------------------------------------------------------ #
    def asegurar_directorios(self) -> None:
        """Crea los directorios de trabajo necesarios (idempotente)."""
        for ruta in (
            self.logs.carpeta,
            self.cache.ruta.parent,
            self.exportacion.carpeta,
            self.ventas.ruta.parent,
            self.portales.historico_ruta.parent,
        ):
            try:
                ruta.mkdir(parents=True, exist_ok=True)
            except OSError:
                pass  # permisos/volumenes: no es motivo para abortar

    def ttl_dias(self, namespace: str) -> float:
        """TTL en dias para un namespace de cache (0 = sin expiracion)."""
        return float(self.cache.ttl_por_namespace.get(namespace, self.cache.ttl_dias))

    def advertencias(self) -> List[str]:
        """Problemas de configuracion detectados (no bloqueantes)."""
        avisos: List[str] = []
        if not self.serper.configurado:
            avisos.append("SERPER_API_KEY ausente: las busquedas web quedaran deshabilitadas.")
        if not self.proxies.urls:
            avisos.append("PROXY_LIST vacio: se operara en conexion directa (IP local expuesta).")
        if self.cache.backend not in {"sqlite", "json"}:
            avisos.append(f"CACHE_BACKEND='{self.cache.backend}' desconocido: se usara sqlite.")
        if self.scraper.fetcher not in {"fetcher", "dynamic"}:
            avisos.append(f"SCRAPER_FETCHER='{self.scraper.fetcher}' desconocido: se usara fetcher.")
        if self.exportacion.delimitador not in {",", ";", "|", "\t"}:
            avisos.append("EXPORT_DELIMITER inusual: verifique el separador del CSV final.")
        if not self.cache.habilitado:
            avisos.append("CACHE_ENABLED=false: se perdera el ahorro por deduplicacion de consultas.")
        if not self.alcance.es_lima(provincia=self.alcance.provincia, departamento=self.alcance.departamento):
            avisos.append(
                f"ALCANCE GEOGRAFICO no es Lima ('{self.alcance.etiqueta}'): revise GEO_DEPARTAMENTO/GEO_PROVINCIA."
            )
        if self.alcance.distritos:
            invalidos = [d for d in self.alcance.distritos if d.title() not in DISTRITOS_LIMA]
            if invalidos:
                avisos.append(f"GEO_DISTRITOS con valores fuera del catalogo de Lima: {invalidos}")
        if self.ventas.politica_error not in {"cerrado", "abierto"}:
            avisos.append("VENTAS_POLITICA_ERROR debe ser 'cerrado' o 'abierto': se usara 'cerrado'.")
        if not self.ventas.bloqueo_venta_permanente:
            avisos.append(
                "VENTAS_BLOQUEO_VENTA_PERMANENTE=false: una venta solo bloquea el aviso durante "
                f"{self.ventas.ventana_dias:.0f} dias (riesgo de revender el mismo prospecto)."
            )
        if self.ventas.retencion_dias < self.ventas.ventana_dias:
            avisos.append("VENTAS_RETENCION_DIAS < EXCLUSIVIDAD_VENTANA_DIAS: se perderia el control de exclusividad.")
        if self.serper.plan_creditos <= 0:
            avisos.append("SERPER_PLAN_CREDITOS=0: la telemetria no podra estimar los creditos restantes.")
        if not self.sunat.consultas:
            avisos.append("SUNAT_QUERIES vacio: el extractor del GRUPO 1 no tendra de donde descubrir empresas.")
        if not self.sunat.urls_consulta:
            avisos.append(
                "SUNAT_URLS_CONSULTA vacio: estado_sunat/actividad se deduciran de Serper "
                "(menos preciso). Agregue un padron de confianza si lo necesita."
            )
        if self.sunat.padron_local_activo and not Path(self.sunat.padron_local).exists():
            avisos.append(
                f"Padron local no encontrado en {self.sunat.padron_local}: el respaldo offline "
                "de SUNAT quedara inactivo (los campos fiscales podrian quedar NULL)."
            )
        if not self.portales.portales:
            avisos.append("PORTALES_ACTIVOS vacio: el GRUPO 2 no tendra portales donde buscar avisos.")
        if not self.portales.consultas:
            avisos.append("PORTALES_QUERIES vacio: el GRUPO 2 no sabra que tipo de inmueble buscar.")
        if self.portales.deduplicar_por not in {"url", "huella"}:
            avisos.append("PORTALES_DEDUPLICAR_POR debe ser 'url' o 'huella': se usara 'url'.")
        if self.portales.exigir_precio is False:
            avisos.append("PORTALES_EXIGIR_PRECIO=false: se aceptaran avisos sin precio (baja calidad A1).")
        return avisos

    def resumen_publico(self) -> Dict[str, Any]:
        """Diccionario seguro (sin secretos) para logs y diagnostico."""
        return {
            "entorno": self.entorno,
            "serper_endpoint": self.serper.endpoint,
            "serper_api_key": mascarar(self.serper.api_key),
            "proxies_configurados": len(self.proxies.urls),
            "proxy_estrategia": self.proxies.estrategia,
            "cache_backend": self.cache.backend,
            "cache_ruta": str(self.cache.ruta),
            "cache_ttl_dias": self.cache.ttl_por_namespace,
            "scraper_fetcher": self.scraper.fetcher,
            "scraper_timeout_ms": self.scraper.timeout_ms,
            "postgres": f"{self.postgres.host}:{self.postgres.puerto}/{self.postgres.base_datos}",
            "export_dir": str(self.exportacion.carpeta),
            "alcance_geografico": self.alcance.etiqueta,
            "alcance_sufijo_busqueda": self.alcance.sufijo_busqueda,
            "exclusividad_ventana_dias": self.ventas.ventana_dias,
            "exclusividad_venta_permanente": self.ventas.bloqueo_venta_permanente,
            "ventas_db": str(self.ventas.ruta),
            "meta_prospectos": self.meta.limite_prospectos,
            "serper_plan_creditos": self.serper.plan_creditos,
            "sunat_consultas": self.sunat.consultas,
            "sunat_padrones_configurados": len(self.sunat.urls_consulta),
            "sunat_padron_local": (
                str(self.sunat.padron_local)
                if self.sunat.padron_local_activo and Path(self.sunat.padron_local).exists()
                else "(inactivo)"
            ),
            "portales_activos": self.portales.portales,
            "portales_consultas": self.portales.consultas,
            "historico_db": str(self.portales.historico_ruta),
            "fx_pen_usd": self.portales.fx_pen_usd,
        }


# --------------------------------------------------------------------------- #
#  Acceso global (singleton)
# --------------------------------------------------------------------------- #
@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Devuelve la configuracion del proyecto (instancia unica)."""
    return Settings.from_env()


def recargar_settings() -> Settings:
    """Fuerza la relectura del ``.env`` (util en tests o notebooks)."""
    get_settings.cache_clear()
    return get_settings()


__all__ = [
    "Settings",
    "LogSettings",
    "SerperSettings",
    "ProxiesSettings",
    "CacheSettings",
    "ScraperSettings",
    "PostgresSettings",
    "ExportSettings",
    "AlcanceGeografico",
    "VentasSettings",
    "MetaSettings",
    "SunatSettings",
    "PortalesSettings",
    "DISTRITOS_LIMA",
    "DISTRITOS_CALLAO",
    "ALIAS_LIMA",
    "COLUMNAS_SALIDA",
    "COLUMNAS_CRITICAS",
    "COLUMNAS_EXTRA",
    "columnas_salida",
    "MOTORES_POR_DOMINIO_POR_DEFECTO",
    "get_settings",
    "recargar_settings",
    "mascarar",
    "RAIZ_PROYECTO",
    "ARCHIVO_ENV",
]


if __name__ == "__main__":  # diagnostico rapido:  python config.py
    import json

    cfg = get_settings()
    print(json.dumps(cfg.resumen_publico(), indent=2, ensure_ascii=False))
    for aviso in cfg.advertencias():
        print(f"  [!] {aviso}")
