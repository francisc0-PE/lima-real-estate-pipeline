# 🏙️ Lima Real Estate Lead Pipeline

🌍 Choose your language: [🇺🇸 English](#-english-version) | [🇪🇸 Español](#-versión-en-español) | [🇫🇷 Français](#-version-française) | [🇨🇳 中文](#-中文版)

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

---

## 🇫🇷 Version Française

> Un pipeline de données robuste et modulaire en Python qui extrait les annonces immobilières publiques et les données du registre des entreprises pour Lima et Callao, les transformant en ensembles de données CSV propres, sans doublons et prêts pour l'exploitation.

### ✨ Fonctionnalités Clés

*   **🏗️ Architecture Multi-Étapes :** Phases de traitement isolées (`Données d'Entreprise` → `Annonces` → `Historique des Prix` → `Exportation CSV`) garantissant la tolérance aux pannes.
*   **⚙️ Modes d'Exécution Flexibles :** 
    *   `Live` : Extraction OSINT complète en temps réel (nécessite des extracteurs personnalisés, non inclus dans ce dépôt public).
    *   `Local` : Exécution rapide sans réseau, utilisant uniquement le cache.
    *   `Demo` : Génération de données synthétiques pour des démonstrations sécurisées.
*   **🛡️ Résilience :** Limite de requêtes intégrée, tentatives avec délai exponentiel, disjoncteurs (circuit breakers) et mise en cache SQLite avec TTL.
*   **🔒 Sécurisé et Configurable :** Configuration basée sur l'environnement (App 12-factor) avec validation stricte. Les clés API sont masquées dans les journaux.
*   **💼 Logique Métier :** Validation stricte de la portée géographique (Lima/Callao) et contrôle de l'exclusivité commerciale (évite la revente de prospects en double).
*   **📊 Télémétrie Propre :** Journaux rotatifs pour le débogage combinés à une sortie de console propre et optimisée pour l'opérateur.

### 🛠️ Technologies

*   **Core :** Testé sur Python 3.13
*   **Scraping et Réseau :** Scrapling, Playwright
*   **Base de Données :** SQLite (Mode WAL)
*   **APIs Externes :** Serper API (Google Search)
*   **CLI et UI :** Rich (Formatage de terminal)

### 🚀 Démarrage Rapide (Mode Démo)

Exécutez le pipeline localement sans requêtes réseau en utilisant des données synthétiques :

```bash
# 1. Cloner le dépôt
git clone [https://github.com/francisc0-PE/lima-real-estate-pipeline.git](https://github.com/francisc0-PE/lima-real-estate-pipeline.git)
cd lima-real-estate-pipeline

# 2. Installer les dépendances
pip install -r requirements.txt

# 3. Exécuter le pipeline en mode démo sécurisé
python main.py --modo local --demo --sin-registro
```

### ⚖️ Utilisation Responsable et Légal

Ce logiciel est conçu exclusivement pour l'agrégation de **données publiquement disponibles**. Les utilisateurs sont tenus de se conformer à :
*   Les `Conditions d'Utilisation` et le fichier `robots.txt` de chaque site cible.
*   La loi sur la protection des données personnelles du Pérou (**Loi N° 29733**).

*Note : Toutes les données d'échantillon et de démonstration fournies dans ce dépôt sont strictement synthétiques et générées à des fins de test.*

---

## 🇨🇳 中文版

> 一个强大且模块化的 Python 数据管道，用于抓取利马 (Lima) 和卡亚俄 (Callao) 的公开房地产房源和公司注册数据，并将其转化为干净、无重复且可操作的 CSV 数据集。

### ✨ 核心功能

*   **🏗️ 多阶段架构：** 独立的处理阶段（`企业数据` → `房源` → `价格历史` → `CSV 导出`），确保容错能力。
*   **⚙️ 灵活的执行模式：** 
    *   `Live`：实时完整的 OSINT 数据提取（需要自定义提取器，不包含在此公开仓库中）。
    *   `Local`：纯缓存的无网络执行，用于快速测试。
    *   `Demo`：生成合成数据以进行安全演示。
*   **🛡️ 弹性与高可用性：** 内置速率限制、指数退避重试、断路器 (circuit breakers) 以及基于 TTL 的 SQLite 缓存。
*   **🔒 安全与可配置：** 基于环境的 12-factor 应用配置，具有严格的验证。API 密钥在诊断日志中被自动屏蔽。
*   **💼 业务逻辑控制：** 严格的地理范围验证（利马/卡亚俄）和商业独占性控制（避免重复转售潜在客户）。
*   **📊 清晰的遥测记录：** 用于调试的轮转日志文件，结合了无干扰、对操作员友好的控制台输出。

### 🛠️ 技术栈

*   **核心：** 已在 Python 3.13 上测试
*   **爬虫与网络：** Scrapling, Playwright
*   **数据与持久化：** SQLite (WAL 模式)
*   **外部 API：** Serper API (Google Search)
*   **CLI 与 UI：** Rich (终端格式化)

### 🚀 快速入门（演示模式）

使用合成数据在本地无网络请求的情况下运行数据管道：

```bash
# 1. 克隆仓库
git clone [https://github.com/francisc0-PE/lima-real-estate-pipeline.git](https://github.com/francisc0-PE/lima-real-estate-pipeline.git)
cd lima-real-estate-pipeline

# 2. 安装依赖
pip install -r requirements.txt

# 3. 在安全的演示模式下运行管道，不记录销售
python main.py --modo local --demo --sin-registro
```

### ⚖️ 负责任的使用与合规性

本软件专为聚合**公开可用数据**而设计。用户必须严格遵守：
*   每个目标网站的 `服务条款` (Terms of Service) 和 `robots.txt` 文件。
*   秘鲁个人数据保护法（**第 29733 号法**）。

*注意：本仓库中提供的所有示例和演示数据均为严格合成的数据，仅供演示目的使用。*
