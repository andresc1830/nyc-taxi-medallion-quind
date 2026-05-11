# Critical Data Elements (CDEs)

## ¿Qué es un CDE?

Un **Critical Data Element** es un campo de datos cuya calidad es crítica para la toma de decisiones de negocio. Si un CDE tiene valores incorrectos, nulos o inconsistentes, los análisis y reportes que dependen de él pierden validez.

Para este pipeline se identifican **3 CDEs** sobre el dataset NYC Yellow Taxi Trips. La selección no es arbitraria: son los únicos campos cuyo fallo rompería al menos un KPI obligatorio de la prueba. Cada CDE alimenta directamente un análisis crítico.

| CDE | KPI al que alimenta | Regla de calidad |
|---|---|---|
| `pickup_datetime` | KPI 1 (Patrón de demanda temporal) | R1: `pickup < dropoff` |
| `fare_amount` | KPI 2 (Eficiencia económica) + KPI 3 (Impacto DQ) | R3: `fare > 0` |
| `pickup_location_id` | KPI 2 (Eficiencia económica por zona) | Join con `taxi_zones_raw` |

---

## CDE 1 — `pickup_datetime`

| Atributo | Valor |
|---|---|
| **Nombre técnico** | `pickup_datetime` (renombrado desde `tpep_pickup_datetime` en la fuente) |
| **Tipo de dato** | `timestamp_ntz` (timestamp sin zona horaria, se asume Eastern Time según TLC) |
| **Dominio de negocio** | Movilidad / Operaciones |
| **Propietario** | NYC TLC (publicador) — Equipo de pricing analítico (consumidor) |

### Definición de negocio

Marca el **instante exacto** en que el medidor del taxi se activa al recoger un pasajero. Es el origen temporal de todo viaje y la base sobre la cual se construyen todas las métricas de demanda (franja horaria, día de semana, día del mes, estacionalidad).

### Regla de calidad aplicada

**Regla R1:** `pickup_datetime < dropoff_datetime`

La hora de pickup debe ser estrictamente anterior a la hora de dropoff. Cualquier registro que viole esta regla indica un error de medición del medidor del taxi o corrupción de datos en transmisión.

**Resultado en este pipeline:**
- Registros descartados: **1,121** (0.0366% del total)
- Ingreso asociado: $22,480.58

### Ubicación de los datos

| Capa | Tabla | Columna |
|---|---|---|
| Raw | `nyc_taxi_andres_rojas.raw.yellow_trips_raw` | `tpep_pickup_datetime` |
| Trusted | `nyc_taxi_andres_rojas.trusted.yellow_trips_trusted` | `pickup_datetime` |
| Refined | `nyc_taxi_andres_rojas.refined.kpi_demanda_temporal` | derivado en `franja_horaria` y `dia_semana_nombre` |

### Impacto si el CDE falla

- **KPI 1 (Patrón de demanda temporal):** se rompe completamente. No se puede determinar pico de demanda, franjas más rentables, ni patrones semanales.
- **Métricas operativas:** cálculo de duración de viaje (`trip_duration_minutes`) imposible.
- **Auditoría:** sin `pickup_datetime` confiable, no se puede reconciliar facturación con turnos de conductor.

---

## CDE 2 — `fare_amount`

| Atributo | Valor |
|---|---|
| **Nombre técnico** | `fare_amount` |
| **Tipo de dato** | `double` (USD) |
| **Dominio de negocio** | Finanzas / Pricing |
| **Propietario** | NYC TLC (publicador) — Equipo financiero (consumidor) |

### Definición de negocio

Monto en dólares estadounidenses cobrado al pasajero por el viaje base, **sin incluir** extras, peajes, propinas ni recargos. Es la métrica económica central del dataset y el insumo principal para calcular ingreso por zona, por franja horaria y por borough.

### Regla de calidad aplicada

**Regla R3:** `fare_amount > 0`

Una transacción válida implica un cobro positivo. Sin embargo, NYC TLC publica registros con `fare_amount` negativo que representan **reembolsos o ajustes contables** posteriores. Estos registros se descartan en la capa Trusted porque distorsionan análisis operacionales, pero se preservan en Raw para auditoría financiera.

**Resultado en este pipeline:**
- Registros descartados: **26,159** (0.853% del total)
- Ingreso asociado: **-$481,911.23** (valor negativo: corresponde a reembolsos)

> **Hallazgo importante:** al descartar registros con `fare ≤ 0` se **preservan** $481K en ingreso bruto que de otra forma serían restados erróneamente como ajustes contables. La regla no solo es de calidad, es de preservación de ingreso económico para análisis operacional.

### Ubicación de los datos

| Capa | Tabla | Columna |
|---|---|---|
| Raw | `nyc_taxi_andres_rojas.raw.yellow_trips_raw` | `fare_amount` |
| Trusted | `nyc_taxi_andres_rojas.trusted.yellow_trips_trusted` | `fare_amount` |
| Refined | `nyc_taxi_andres_rojas.refined.kpi_demanda_temporal` | agregado en `tarifa_promedio` y `tarifa_total` |
| Refined | `nyc_taxi_andres_rojas.refined.kpi_eficiencia_economica` | agregado en `tarifa_promedio`, `ingreso_total`, `ingreso_por_milla` |
| Refined | `nyc_taxi_andres_rojas.refined.kpi_impacto_calidad_datos` | cuantificación del ingreso descartado por regla |

### Impacto si el CDE falla

- **KPI 2 (Eficiencia económica):** todos los rankings de rentabilidad se invalidan.
- **KPI 3 (Impacto de DQ):** no se puede cuantificar valor económico descartado.
- **Reportes financieros:** ingresos totales reportados a stakeholders dejan de ser confiables.

---

## CDE 3 — `pickup_location_id`

| Atributo | Valor |
|---|---|
| **Nombre técnico** | `pickup_location_id` (renombrado desde `PULocationID` en la fuente) |
| **Tipo de dato** | `bigint` (entero 1-265) |
| **Dominio de negocio** | Geografía / Operaciones |
| **Propietario** | NYC TLC (taxonomía) — Equipo de operaciones (consumidor) |

### Definición de negocio

Identificador numérico de la **zona geográfica de NYC** donde se origina el viaje. Cada `pickup_location_id` corresponde a una zona registrada en la tabla `taxi_zones_raw` (lookup), que asocia el ID con un nombre de zona y borough. Permite análisis espacial y segmentación de demanda por área.

### Regla de calidad aplicada

**Regla implícita (join con `taxi_zones_raw`):**

El `pickup_location_id` debe tener correspondencia en `taxi_zones_raw.LocationID`. Si no hay match, el viaje conserva borough/zona como `NULL` y se registra en el reporte de DQ (`viajes_sin_zona`).

**Resultado en este pipeline:**
- Total viajes después de validaciones: 2,987,000
- Viajes con `pickup_borough = NULL` (sin match en zones): **0**
- **Cobertura del lookup: 100%**

### Ubicación de los datos

| Capa | Tabla | Columna |
|---|---|---|
| Raw | `nyc_taxi_andres_rojas.raw.yellow_trips_raw` | `PULocationID` |
| Raw | `nyc_taxi_andres_rojas.raw.taxi_zones_raw` | `LocationID` (tabla de referencia) |
| Trusted | `nyc_taxi_andres_rojas.trusted.yellow_trips_trusted` | `pickup_location_id`, `pickup_borough`, `pickup_zone` |
| Refined | `nyc_taxi_andres_rojas.refined.kpi_eficiencia_economica` | `pickup_borough`, `pickup_zone` |

### Impacto si el CDE falla

- **KPI 2 (Eficiencia económica):** no se puede calcular rentabilidad por zona, perdiendo capacidad de optimización geográfica de la flota.
- **Operaciones:** decisiones sobre dónde concentrar conductores se basan en datos incompletos.
- **Compliance regulatorio:** NYC TLC exige reportes geográficos periódicos para licencias.

---

## Cómo se monitorean los CDEs

Las reglas de calidad asociadas a estos CDEs se ejecutan automáticamente en el pipeline (Notebook 02) y los resultados se consolidan en dos artefactos:

1. **Tabla `refined.data_quality_report`** — registro estructurado con `regla_id`, `registros_passed`, `registros_failed`, `pct_failed`, `ingreso_descartado`, `pipeline_run_id` y `execution_timestamp`.

2. **Archivo `reports/execution_report.json`** — reporte JSON estructurado por run del pipeline con todas las métricas de calidad consolidadas, KPIs destacados y tiempos por etapa.

### Evolución a producción

En un escenario productivo, las métricas de los CDEs alimentarían:

- **Dashboard de DQ** en Power BI / Databricks SQL Dashboard con alertas automáticas cuando los % de descarte excedan umbrales definidos.
- **Notificaciones** a Slack / email al equipo de gobierno cuando un CDE viole umbrales.
- **Bloqueo de pipeline downstream** si la tasa de descarte supera un threshold crítico (ej. >5% en un día).
- **Historificación** del `data_quality_report` con `pipeline_run_id` para análisis de tendencias de calidad a lo largo del tiempo.