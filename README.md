# NYC Yellow Taxi Trips — Pipeline Medallion sobre Databricks

> **Prueba técnica — QUIND S.A.S.**
> Autor: Andrés Camilo Rojas Pinto
> Fecha de entrega: Mayo 2026

## Resumen ejecutivo
Pipeline ETL end-to-end que ingiere los datos públicos de viajes de taxi amarillo de Nueva York (enero 2023) y los transforma siguiendo una arquitectura **Medallion** (Raw → Trusted → Refined) sobre **Databricks Free Edition**, con gobierno mediante **Unity Catalog**, validaciones de calidad de datos, observabilidad y entrega de KPIs de negocio.

## Resultados y Hallazgos Clave (Ejecución Enero 2023)
**Procesamiento y Calidad de Datos:**
- **Registros procesados:** 3,066,766 (Raw) → 2,987,000 (Trusted)
- **Tasa de descarte (DQ):** 2.60% (79,766 registros). Un balance óptimo que limpia outliers sin sesgar la muestra.
- **Impacto Financiero de DQ:** La regla de tarifas > 0 (R3) descartó registros con valores negativos, **preservando $481,911** en ingresos brutos que correspondían a reembolsos/ajustes, protegiendo los KPIs operacionales.

**Insights de Negocio (KPIs):**
- **Demanda Temporal:** El pico máximo de demanda no ocurre el fin de semana, sino los **martes en la tarde (13:00-16:59)** con 119,262 viajes.
- **Rentabilidad (Ingreso/milla):** La zona intra-NYC más rentable es **Jackson Heights (Queens)** con $28.11/milla (viajes cortos y alta tarifa).
- **Volumen Económico:** **JFK Airport** domina el ingreso total generando $9.31M en el mes, a pesar de tener un bajo ingreso por milla ($5.01) debido a tarifas fijas y largas distancias.

## Stack tecnológico
| Componente | Versión / Edición | Justificación |
|---|---|---|
| Databricks | Free Edition | Sucesora de Community Edition. Incluye Unity Catalog completo, serverless compute, sin expiración. |
| Unity Catalog | Default metastore | Gobierno centralizado. |
| PySpark | 3.5+ | Procesamiento distribuido. |
| Delta Lake | 3.x | Almacenamiento ACID, time travel, schema evolution. |
| Python | 3.11+ | Lenguaje principal de los notebooks. |

## Arquitectura y Linaje
El detalle completo del linaje de datos se encuentra en [`docs/linaje.md`](docs/linaje.md).
Los elementos críticos de datos (CDEs) y reglas aplicadas están en [`docs/cdes.md`](docs/cdes.md) y el glosario de negocio en [`docs/glosario.md`](docs/glosario.md).

## Estructura del repositorio
```text
nyc-taxi-medallion-quind/
├── notebooks/
│   ├── 00_setup_unity_catalog.py
│   ├── 01_raw_ingestion.py
│   ├── 02_trusted_transformation.py
│   ├── 03_refined_kpis.py
│   └── 04_data_quality_report.py
├── docs/
│   ├── cdes.md
│   ├── glosario.md
│   └── linaje.md
├── reports/
│   └── execution_report.json
└── README.md
```

## Cómo ejecutar
1. **Prerequisitos:** Cuenta en **Databricks Free Edition**.
2. **Clonar el repositorio:** `git clone https://github.com/andresc1830/nyc-taxi-medallion-quind.git`
3. **Subir archivos fuente:** `yellow_tripdata_2023-01.parquet` y `taxi_zone_lookup.csv` a `/Volumes/nyc_taxi_andres_rojas/raw/source_files/`.
4. **Ejecutar los notebooks en orden:** 00 al 04.

## Decisiones técnicas clave
- **Infrastructure as Code:** Setup de catálogos y schemas vía notebook (00) para reproducibilidad.
- **Trazabilidad:** Columnas de auditoría (`_ingestion_timestamp`, `_source_file`) agregadas desde la capa Raw.
- **Particionado:** Capa Trusted particionada por `pickup_date` (34 particiones) para optimizar consultas.
- **Manejo de Errores:** Implementación de *retry pattern* manual (3 intentos) en lecturas para tolerancia a fallos transitorios.

## Evolución a Producción
1. **Ingesta incremental:** Reemplazar batch read por Auto Loader (cloudFiles).
2. **Orquestación:** Azure Data Factory o Databricks Workflows.
3. **CI/CD:** GitHub Actions + Databricks Asset Bundles.

## Contacto
**Andrés Camilo Rojas Pinto**
Ingeniero de Sistemas · Especialista en Pricing & Datos
📧 andresc.rojasp@gmail.com
💼 https://www.linkedin.com/in/andrés-rojas-5b9aa894