# Linaje de datos del pipeline

Este documento describe el flujo de datos del pipeline ETL Medallion: cómo cada campo fluye desde la fuente hasta los KPIs finales, qué transformaciones sufre, y dónde reside en cada capa.

---

## Diagrama de linaje general

```mermaid
flowchart LR
    %% Fuentes externas
    SRC1[NYC TLC Open Data<br/>yellow_tripdata_2023-01.parquet<br/>3,066,766 registros]
    SRC2[NYC TLC Open Data<br/>taxi_zone_lookup.csv<br/>265 zonas]

    %% Capa Raw
    R1[(raw.yellow_trips_raw<br/>3,066,766 registros<br/>21 columnas)]
    R2[(raw.taxi_zones_raw<br/>265 registros<br/>6 columnas)]

    %% Capa Trusted
    T1[(trusted.yellow_trips_trusted<br/>2,987,000 registros<br/>29 columnas)]

    %% Capa Refined
    K1[(refined.kpi_demanda_temporal<br/>42 filas)]
    K2[(refined.kpi_eficiencia_economica<br/>145 filas)]
    K3[(refined.kpi_impacto_calidad_datos<br/>5 filas)]
    DQ[(refined.data_quality_report<br/>5 filas)]

    %% Outputs finales
    JSON[reports/execution_report.json<br/>Metadata + métricas]

    %% Flujo
    SRC1 -->|Ingesta directa| R1
    SRC2 -->|Ingesta directa| R2

    R1 -->|"5 reglas DQ + Join + Enriquecimiento<br/>(Notebook 02)"| T1
    R2 -.->|LEFT JOIN<br/>pickup_location_id| T1

    T1 -->|"Agrupación por franja×día<br/>(Notebook 03)"| K1
    T1 -->|"Agrupación por borough×zona<br/>+ Window function<br/>(Notebook 03)"| K2
    R1 -->|"Cuantificación impacto DQ<br/>(Notebook 03)"| K3

    K3 -->|"Consolidación + metadata<br/>(Notebook 04)"| DQ
    T1 -.->|Métricas resumen| JSON
    K1 -.->|KPI highlights| JSON
    K2 -.->|KPI highlights| JSON
    DQ -.->|Reglas + ingreso descartado| JSON

    %% Estilos
    classDef raw fill:#fff4e6,stroke:#cc6600,color:#000
    classDef trusted fill:#e6f4ff,stroke:#0066cc,color:#000
    classDef refined fill:#e6ffe6,stroke:#006600,color:#000
    classDef output fill:#f0f0f0,stroke:#333,color:#000

    class R1,R2 raw
    class T1 trusted
    class K1,K2,K3,DQ refined
    class JSON,SRC1,SRC2 output
```

---

## Linaje detallado de los 3 CDEs

### CDE 1: `pickup_datetime`

```mermaid
flowchart LR
    A[NYC TLC Parquet<br/>tpep_pickup_datetime] --> B[raw.yellow_trips_raw<br/>tpep_pickup_datetime<br/>timestamp_ntz]
    B -->|Rename + R1| C[trusted.yellow_trips_trusted<br/>pickup_datetime]
    C -->|hour, dayofweek| D[trusted.yellow_trips_trusted<br/>pickup_hour, pickup_dayofweek]
    D -->|Map a categorías| E[refined.kpi_demanda_temporal<br/>franja_horaria, dia_semana_nombre]
```

**Transformaciones aplicadas:**
1. **Raw → Trusted:** renombre `tpep_pickup_datetime` → `pickup_datetime`. Validación R1 (debe ser menor que `dropoff_datetime`).
2. **Trusted (mismo nivel):** derivación de `pickup_hour = hour(pickup_datetime)` y `pickup_dayofweek = dayofweek(pickup_datetime)`.
3. **Trusted → Refined:** mapeo a categorías legibles con `F.when()` (6 franjas horarias, 7 días en español).

### CDE 2: `fare_amount`

```mermaid
flowchart LR
    A[NYC TLC Parquet<br/>fare_amount] --> B[raw.yellow_trips_raw<br/>fare_amount<br/>double]
    B -->|R3: fare > 0| C[trusted.yellow_trips_trusted<br/>fare_amount]
    C -->|AVG, SUM| D[refined.kpi_demanda_temporal<br/>tarifa_promedio, tarifa_total]
    C -->|AVG fare/distance| E[refined.kpi_eficiencia_economica<br/>ingreso_por_milla]
    C -->|SUM| F[refined.kpi_eficiencia_economica<br/>ingreso_total]
    B -->|Filtro R3 inverso| G[refined.kpi_impacto_calidad_datos<br/>ingreso_descartado]
```

**Transformaciones aplicadas:**
1. **Raw → Trusted:** validación R3 (debe ser > 0). Los registros con `fare ≤ 0` se descartan pero se cuantifican para KPI 3.
2. **Trusted → Refined.kpi_demanda_temporal:** agregaciones por franja×día (`AVG`, `SUM`).
3. **Trusted → Refined.kpi_eficiencia_economica:** cálculo de `ingreso_por_milla = AVG(fare_amount / trip_distance)`.
4. **Raw → Refined.kpi_impacto_calidad_datos:** lectura directa de Raw para cuantificar lo descartado.

### CDE 3: `pickup_location_id`

```mermaid
flowchart LR
    A1[NYC TLC Parquet<br/>PULocationID] --> B1[raw.yellow_trips_raw<br/>PULocationID]
    A2[NYC TLC CSV<br/>LocationID + Borough + Zone] --> B2[raw.taxi_zones_raw<br/>LocationID, Borough, Zone]
    B1 -->|Rename| C[trusted.yellow_trips_trusted<br/>pickup_location_id]
    B2 -->|LEFT JOIN| C
    C -->|Enriquecimiento| D[trusted.yellow_trips_trusted<br/>pickup_borough, pickup_zone]
    D -->|GROUP BY| E[refined.kpi_eficiencia_economica<br/>pickup_borough, pickup_zone]
```

**Transformaciones aplicadas:**
1. **Raw → Trusted:** renombre `PULocationID` → `pickup_location_id`. LEFT JOIN con `taxi_zones_raw` para enriquecer con `Borough` y `Zone`.
2. **Trusted (mismo nivel):** las columnas `pickup_borough` y `pickup_zone` quedan disponibles junto al ID.
3. **Trusted → Refined:** agrupación por `(pickup_borough, pickup_zone)` con cálculo de métricas económicas.

---

## Notas técnicas sobre el linaje

### Por qué LEFT JOIN y no INNER JOIN con `taxi_zones_raw`

Se eligió **LEFT JOIN** en lugar de INNER JOIN para no perder viajes cuyo `pickup_location_id` no tuviera match en el lookup. Los viajes sin match quedan con `pickup_borough = NULL` y se reportan en métricas de DQ, en lugar de ser descartados silenciosamente. En este pipeline la cobertura fue del 100%, pero el LEFT JOIN es defensivo para producción.

### Particionamiento físico

La tabla `trusted.yellow_trips_trusted` está particionada físicamente por **`pickup_date`** (34 particiones — los 31 días de enero + 3 particiones extra de timestamps fuera del rango esperado, comportamiento típico de datos TLC). Esto optimiza consultas que filtran por fecha en la capa Refined.

Las tablas Refined no están particionadas porque son pequeñas (42, 145, 5 filas) — el particionamiento agregaría overhead sin beneficio.

### Trazabilidad por `pipeline_run_id`

Cada ejecución del pipeline genera un UUID único que se persiste en:
- Columna `pipeline_run_id` de `refined.data_quality_report`
- Campo `execution_metadata.execution_id` en `reports/execution_report.json`

Esto permite reconstruir qué versión del pipeline produjo qué reportes — base para auditoría y debugging en producción.

---

## Evolución del linaje a producción

En un entorno productivo, este linaje se documentaría adicionalmente en:

- **Unity Catalog Lineage:** UC genera linaje automático en SQL queries y transformaciones PySpark. Visible en la pestaña "Lineage" de cada tabla del Catalog Explorer.
- **OpenLineage / Marquez:** estándar abierto para capturar linaje de pipelines de datos. Se integra con Spark y Airflow.
- **Data catalog tool externo:** Alation, Collibra, DataHub para gobierno avanzado y descubrimiento de datos para no técnicos.