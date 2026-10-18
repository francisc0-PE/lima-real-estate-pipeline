# 🏙️ Lima Real Estate Lead Pipeline

🌍 Choose your language: [🇺🇸 English](#-english-version) | [🇪🇸 Español](#-versión-en-español)

---

## 🇺🇸 English Version

> A robust, modular Python data pipeline that aggregates public real-estate listings and company registry data for Lima and Callao, transforming them into clean, deduplicated, and actionable CSV datasets.

### ✨ Key Features

*   **🏗️ Multi-Stage Architecture:** Isolated processing stages (`Company Data` → `Listings` → `Price History` → `CSV Export`) ensuring fault tolerance.
*   **⚙️ Flexible Execution Modes:** 
    *   `Live`: Full OSINT data extraction (requires custom extractors, not included in this public repo).
    *   `Local`: Cache-only, zero-network execution for rapid testing.
    *   `Demo`: Synthetic data generation for safe demonstrations.
*   **🛡️ Resilience:** Built-in rate limiting, exponential backoff retries, circuit breakers, and TTL-based SQLite caching.
*   **🔒 Secure & Configurable:** Environment-based 12-factor app configuration with strict validation. API keys are masked in diagnostics.
*   **💼 Business Logic Enforcement:** Strict geographic scope validation (Lima/Callao) and commercial exclusivity control (avoids reselling duplicate leads).
*   **📊 Clean Telemetry:** Rotating file logs for debugging combined with a noise-free, operator-friendly console output.

### 🛠️ Tech Stack

*   **Core:** Tested on Python 3.13
*   **Scraping & Network:** Scrapling, Playwright
*   **Data & Persistence:** SQLite (WAL mode)
*   **External APIs:** Serper API (Google Search)
*   **CLI & UI:** Rich (Terminal formatting)

### 🚀 Quick Start (Demo Mode)

Run the pipeline locally without network calls using synthetic data:

```bash
# 1. Clone the repository
git clone [https://github.com/francisc0-PE/lima-real-estate-pipeline.git](https://github.com/francisc0-PE/lima-real-estate-pipeline.git)
cd lima-real-estate-pipeline

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run the pipeline in safe demo mode without registering sales
python main.py --modo local --demo --sin-registro
```

### ⚖️ Responsible Use & Compliance

This software is designed exclusively for the aggregation of **publicly available data**. Users are strictly required to comply with:
*   Each target website's `Terms of Service` and `robots.txt`.
*   Peru's Personal Data Protection Law (**Ley N° 29733**).

*Note: All sample and demo data provided in this repository is strictly synthetic and generated for demonstration purposes.*

---

## 🇪🇸 Versión en Español

> Un pipeline de datos robusto y modular en Python que extrae avisos inmobiliarios públicos y datos del padrón de empresas para Lima y Callao, transformándolos en datasets CSV limpios, sin duplicados y listos para ventas.

### ✨ Características Principales

*   **🏗️ Arquitectura Multi-Etapa:** Fases de procesamiento aisladas (`Datos de Empresa` → `Avisos` → `Histórico de Precios` → `Exportación CSV`) que garantizan tolerancia a fallos.
*   **⚙️ Modos de Ejecución Flexibles:** 
    *   `Live`: Extracción OSINT completa en tiempo real (requiere extractores propios, no incluidos en este repositorio público).
    *   `Local`: Ejecución rápida sin red, usando solo caché y base de datos.
    *   `Demo`: Generación de datos sintéticos para pruebas seguras.
*   **🛡️ Resiliencia:** Rate limiting integrado, reintentos con backoff exponencial, circuit breakers y caché SQLite con TTL.
*   **🔒 Seguro y Configurable:** Configuración basada en entorno (App 12-factor) con validación estricta. Las API keys se ocultan en los diagnósticos.
*   **💼 Lógica de Negocio:** Validación estricta de alcance geográfico (Lima/Callao) y control de exclusividad comercial (evita revender el mismo prospecto).
*   **📊 Telemetría Limpia:** Logs rotativos en archivo para depuración, combinados con una consola limpia y amigable para el operador.

### 🛠️ Tecnologías

*   **Core:** Probado en Python 3.13
*   **Scraping y Red:** Scrapling, Playwright
*   **Base de Datos:** SQLite (Modo WAL)
*   **APIs Externas:** Serper API (Google Search)
*   **CLI y UI:** Rich (Formato en terminal)

### 🚀 Inicio Rápido (Modo Demo)

Ejecuta el pipeline localmente sin consumir red usando datos sintéticos:

```bash
# 1. Clonar el repositorio
git clone [https://github.com/francisc0-PE/lima-real-estate-pipeline.git](https://github.com/francisc0-PE/lima-real-estate-pipeline.git)
cd lima-real-estate-pipeline

# 2. Instalar dependencias
pip install -r requirements.txt

# 3. Ejecutar el pipeline en modo demo seguro sin registrar ventas
python main.py --modo local --demo --sin-registro
```

### ⚖️ Uso Responsable y Legal

Este software está diseñado exclusivamente para la agregación de **datos disponibles públicamente**. Los usuarios deben cumplir estrictamente con:
*   Los `Términos de Servicio` y el archivo `robots.txt` de cada sitio web objetivo.
*   La Ley de Protección de Datos Personales del Perú (**Ley N° 29733**).

*Nota: Todos los datos de muestra y demostración proporcionados en este repositorio son estrictamente sintéticos y generados para fines de prueba.*
