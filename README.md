# NYC Yellow Taxi Trips — Pipeline Medallion sobre Databricks

> **Prueba técnica — QUIND S.A.S.**  
> Posición: Ingeniero de Datos Senior — Azure & Gobierno  
> Autor: Andrés Camilo Rojas  
> Fecha de entrega: Mayo 2026

## Resumen ejecutivo

Pipeline ETL end-to-end que ingiere los datos públicos de viajes de taxi amarillo de Nueva York (enero 2023) y los transforma siguiendo una arquitectura **Medallion** (Raw → Trusted → Refined) sobre **Databricks Free Edition**, con gobierno mediante **Unity Catalog**, validaciones de calidad de datos, observabilidad y entrega de KPIs de negocio.

## Stack tecnológico

| Componente | Versión / Edición | Justificación |
|---|---|---|
| Databricks | Free Edition | Sucesora de Community Edition (retirada en enero 2026). Incluye Unity Catalog completo, serverless compute, sin expiración. |
| Unity Catalog | Default metastore | Gobierno centralizado, requerido por la prueba. |
| PySpark | 3.5+ (incluido en runtime de Databricks) | Procesamiento distribuido. |
| Delta Lake | 3.x (default en Databricks) | Almacenamiento ACID, time travel, schema evolution. |
| Python | 3.11+ | Lenguaje principal de los notebooks. |
| Git / GitHub | — | Control de versiones. |

> **Nota sobre Azure:** la prueba sugería Azure Databricks, pero Free Edition no corre en Azure (es Databricks-hosted). El código PySpark/Delta es 100% portable a Azure Databricks sin modificaciones — el único cambio sería el path del Volume y la configuración del workspace.

## Arquitectura

```
┌────────────────┐     ┌──────────────┐     ┌──────────────┐     ┌───────────────┐
│ Fuentes NYC TLC│ ──> │   RAW LAYER  │ ──> │ TRUSTED LAYER│ ──> │ REFINED LAYER │
│ (Parquet + CSV)│     │  (espejo)    │     │ (limpio +    │     │ (KPIs +       │
│                │     │              │     │  enriquecido)│     │  agregados)   │
└────────────────┘     └──────────────┘     └──────────────┘     └───────────────┘
                              │                    │                    │
                              └────────────────────┴────────────────────┘
                                                   │
                                          ┌────────▼────────┐
                                          │  Unity Catalog  │
                                          │  (gobierno y    │
                                          │   metadata)     │
                                          └─────────────────┘
```

Diagrama detallado en [`docs/linaje.md`](docs/linaje.md).

## Estructura del repositorio

```
nyc-taxi-medallion-quind/
├── notebooks/                          # Notebooks Databricks (exportados como .py)
│   ├── 00_setup_unity_catalog.py       # Crea catálogo, schemas y volume
│   ├── 01_raw_ingestion.py             # Ingesta Parquet + CSV → tablas Delta Raw
│   ├── 02_trusted_transformation.py    # Limpieza, validaciones, enriquecimiento
│   ├── 03_refined_kpis.py              # KPIs de demanda y eficiencia económica
│   └── 04_data_quality_report.py       # Reporte de calidad y pipeline orquestador
├── docs/
│   ├── cdes.md                         # Critical Data Elements documentados
│   ├── glosario.md                     # Glosario de términos de negocio
│   └── linaje.md                       # Diagrama de linaje (Mermaid)
├── src/
│   └── utils/                          # Helpers reutilizables (logger, validators)
├── tests/                              # Tests unitarios (pytest)
├── reports/                            # Reportes JSON de ejecución
└── README.md
```

## Cómo ejecutar

### Prerequisitos

1. Cuenta en **Databricks Free Edition** (gratuita, perpetua, sin tarjeta de crédito): https://www.databricks.com/learn/free-edition
2. Cuenta de GitHub para clonar este repositorio.

### Pasos

1. **Clonar el repositorio** (en tu workspace de Databricks o localmente):
```bash
   git clone https://github.com/andresc1830/nyc-taxi-medallion-quind.git
```

2. **Importar los notebooks** a tu workspace de Databricks:
   - Workspace → tu carpeta personal → Import → seleccionar los archivos `.py` de `notebooks/`.

3. **Descargar los archivos fuente** y subirlos al volume:
   - Parquet: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet
   - CSV: https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
   - Subir ambos a `/Volumes/nyc_taxi_andres_rojas/raw/source_files/` (el notebook 00 crea el volume).

4. **Ejecutar los notebooks en orden**:
   - `00_setup_unity_catalog` (una vez, crea la infraestructura)
   - `01_raw_ingestion`
   - `02_trusted_transformation`
   - `03_refined_kpis`
   - `04_data_quality_report`

## Decisiones técnicas clave

### Por qué Free Edition y no Free Trial de 14 días
Free Edition es perpetua y suficiente para el volumen de la prueba (un mes de datos, ~50MB en parquet). El Free Trial expiraría justo durante la fase de sustentación oral, lo cual es un riesgo innecesario.

### Por qué notebooks tradicionales y no Delta Live Tables
DLT añade dependencias y abstracciones específicas de Databricks que dificultan la portabilidad. Notebooks PySpark son código estándar que corre en cualquier Spark.

### Por qué Infrastructure as Code (Notebook 00)
Crear el catálogo y schemas por código en lugar de UI permite reproducibilidad: cualquiera clona el repo y replica la estructura exacta corriendo un notebook.

### Por qué columnas de auditoría (`_ingestion_timestamp`, `_source_file`)
Permiten trazabilidad en pipelines de múltiples archivos y debugging cuando un dataset llega corrupto.

### Reglas de validación aplicadas (capa Trusted)
- `pickup_datetime < dropoff_datetime` (lógica temporal)
- `trip_distance > 0` (viaje real)
- `fare_amount > 0` (transacción válida)
- `trip_duration_minutes <= 360` (descarta viajes > 6h, atípicos para taxis urbanos)

### Particionado
Las tablas Trusted y Refined se particionan por `pickup_date` para optimizar queries por franja temporal (los KPIs principales son temporales).

## Limitaciones conocidas

- **Volumen de prueba:** un solo mes (~3M filas). En producción se procesarían años de datos con particionado y Z-ORDER más agresivos.
- **Sin orquestador externo:** los notebooks se ejecutan manualmente o vía Databricks Workflows. En producción se usaría Airflow / Azure Data Factory para SLAs y dependencias complejas.
- **Free Edition:** limitaciones de cómputo serverless. No reflejan el rendimiento real de un workspace empresarial.

## Cómo evolucionaría a producción

1. **Ingesta incremental:** reemplazar batch read por **Auto Loader (cloudFiles)** con checkpoints.
2. **Orquestación:** Azure Data Factory o Databricks Workflows con dependencias, alertas y reintentos.
3. **CI/CD:** GitHub Actions con `dbx` o Databricks Asset Bundles para deployment automático a dev/stage/prod.
4. **Monitoring:** Datadog / Azure Monitor con alertas sobre métricas de DQ y SLAs.
5. **Tests de regresión:** suite pytest sobre datos sintéticos en cada PR.
6. **Catálogo multi-ambiente:** `nyc_taxi_dev`, `nyc_taxi_stg`, `nyc_taxi_prd` con políticas de acceso por rol.

## Sustentación

Repositorio público y notebooks ejecutables disponibles para revisión en vivo durante la sustentación oral.

## Contacto

**Andrés Camilo Rojas**  
Sistemas Engineer | Pricing & Data Specialist  
andresc.rojasp@gmail.com · (https://www.linkedin.com/in/andr%C3%A9s-rojas-5b9aa894/)