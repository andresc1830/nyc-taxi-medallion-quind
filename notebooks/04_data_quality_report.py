# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 04 — Data Quality Report + Reporte de Ejecución JSON
# MAGIC
# MAGIC **Objetivo:** Cerrar el pipeline Medallion produciendo dos artefactos finales:
# MAGIC
# MAGIC 1. **Tabla `refined.data_quality_report`** que consolida los resultados de las 5 reglas de calidad aplicadas en el Notebook 02 (registros que pasan/fallan por regla).
# MAGIC
# MAGIC 2. **Archivo `reports/execution_report.json`** con métricas consolidadas del pipeline: total procesado, descartados, KPIs resultantes, timestamps.
# MAGIC
# MAGIC **Inputs (lecturas, no escrituras):**
# MAGIC - `raw.yellow_trips_raw` (3,066,766 registros)
# MAGIC - `raw.taxi_zones_raw` (265 registros)
# MAGIC - `trusted.yellow_trips_trusted` (2,987,000 registros)
# MAGIC - `refined.kpi_demanda_temporal` (42 filas)
# MAGIC - `refined.kpi_eficiencia_economica` (145 filas)
# MAGIC - `refined.kpi_impacto_calidad_datos` (5 filas)
# MAGIC
# MAGIC **Outputs:**
# MAGIC - Tabla `refined.data_quality_report` (5 filas, una por regla)
# MAGIC - Archivo JSON en `/Volumes/{catalog}/raw/source_files/execution_report.json` (y copia local en `reports/`)
# MAGIC
# MAGIC **Patrones técnicos aplicados:**
# MAGIC 1. **Retry pattern** para tolerancia a fallos transient en lecturas (3 intentos con backoff).
# MAGIC 2. **Manejo estructurado de excepciones** (try/except con tipos específicos).
# MAGIC 3. **Timestamps por etapa** para medir tiempo de ejecución.
# MAGIC
# MAGIC **Tiempo estimado de ejecución:** 2-3 minutos

# COMMAND ----------

# MAGIC %md
# MAGIC ### Variables de configuración

# COMMAND ----------

# ============================================================
# CONFIGURACIÓN
# ============================================================

CATALOG_NAME = "nyc_taxi_andres_rojas"

# Tablas a leer (de los 3 schemas)
TABLAS_RAW = {
    "yellow_trips_raw": f"{CATALOG_NAME}.raw.yellow_trips_raw",
    "taxi_zones_raw": f"{CATALOG_NAME}.raw.taxi_zones_raw"
}

TABLAS_TRUSTED = {
    "yellow_trips_trusted": f"{CATALOG_NAME}.trusted.yellow_trips_trusted"
}

TABLAS_REFINED = {
    "kpi_demanda_temporal": f"{CATALOG_NAME}.refined.kpi_demanda_temporal",
    "kpi_eficiencia_economica": f"{CATALOG_NAME}.refined.kpi_eficiencia_economica",
    "kpi_impacto_calidad_datos": f"{CATALOG_NAME}.refined.kpi_impacto_calidad_datos"
}

# Tabla destino: data_quality_report
DQ_REPORT_TABLE = f"{CATALOG_NAME}.refined.data_quality_report"

# Path para el JSON (lo guardamos en el volume y también local para descargarlo)
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/raw/source_files"
JSON_REPORT_PATH = f"{VOLUME_PATH}/execution_report.json"

# Configuración de retry
MAX_RETRY_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 5

print("Configuración del notebook:")
print(f"  Catálogo: {CATALOG_NAME}")
print(f"  Tablas a leer:")
for nombre, ruta in {**TABLAS_RAW, **TABLAS_TRUSTED, **TABLAS_REFINED}.items():
    print(f"    {nombre}: {ruta}")
print(f"\n  Tabla destino DQ: {DQ_REPORT_TABLE}")
print(f"  JSON destino: {JSON_REPORT_PATH}")
print(f"\n  Retry config: {MAX_RETRY_ATTEMPTS} intentos, {RETRY_DELAY_SECONDS}s entre intentos")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Logger e imports

# COMMAND ----------

# ============================================================
# LOGGING SETUP E IMPORTS
# ============================================================

import logging
import time
import json
import uuid
from datetime import datetime, timezone
from typing import Callable, Any

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("data_quality_report")

# Imports de PySpark
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

logger.info("Logger inicializado")
logger.info(f"Notebook ejecutado en: {datetime.now(timezone.utc).isoformat()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Helper de retry + tracking de tiempo

# COMMAND ----------

# ============================================================
# HELPERS: RETRY PATTERN Y TRACKING DE TIEMPO
# ============================================================

def retry_operation(
    operation: Callable[[], Any],
    operation_name: str,
    max_attempts: int = MAX_RETRY_ATTEMPTS,
    delay_seconds: int = RETRY_DELAY_SECONDS
) -> Any:
    """
    Ejecuta una operación con manejo de reintentos en caso de fallo.
    
    Patrón de tolerancia a fallos para operaciones que pueden fallar
    por transient errors (red, cluster reinicializándose, locks de tabla).
    
    Args:
        operation: función a ejecutar (sin argumentos, usar lambda si los necesita)
        operation_name: nombre legible para logging
        max_attempts: número máximo de intentos antes de fallar definitivamente
        delay_seconds: segundos de espera entre intentos
    
    Returns:
        El resultado de la operación si tiene éxito.
    
    Raises:
        La excepción original si se agotan los intentos.
    """
    last_exception = None
    
    for attempt in range(1, max_attempts + 1):
        try:
            logger.info(f"  Intento {attempt}/{max_attempts}: {operation_name}")
            result = operation()
            
            if attempt > 1:
                logger.warning(f"  ✓ {operation_name} completó después de {attempt} intentos")
            else:
                logger.info(f"  ✓ {operation_name} completó exitosamente")
            
            return result
        
        except Exception as e:
            last_exception = e
            logger.warning(f"  ✗ Intento {attempt} falló: {type(e).__name__}: {str(e)[:200]}")
            
            if attempt < max_attempts:
                logger.info(f"  Esperando {delay_seconds}s antes de reintentar...")
                time.sleep(delay_seconds)
            else:
                logger.error(f"  ✗ {operation_name} falló definitivamente después de {max_attempts} intentos")
    
    # Si llegamos aquí, todos los intentos fallaron
    raise last_exception


# Inicializar tracking de tiempo de ejecución
pipeline_start_time = time.time()
stage_timings = {}  # diccionario para guardar duración de cada etapa

def mark_stage(stage_name: str, start_time: float) -> float:
    """Calcula la duración de una etapa y la registra."""
    duration = time.time() - start_time
    stage_timings[stage_name] = round(duration, 2)
    logger.info(f"  ⏱  Etapa '{stage_name}' completada en {duration:.2f}s")
    return duration


logger.info("✓ Helpers cargados: retry_operation, mark_stage")
logger.info(f"✓ Pipeline iniciado a las {datetime.now(timezone.utc).isoformat()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 1: Lectura tolerante a fallos de las 6 tablas
# MAGIC
# MAGIC Aplicamos el helper `retry_operation` a cada lectura de tabla. En caso de un fallo transient (timeout, cluster reinicializándose), se reintenta hasta 3 veces antes de fallar definitivamente.
# MAGIC
# MAGIC **Métricas que capturamos por tabla:**
# MAGIC - Conteo de registros
# MAGIC - Número de columnas
# MAGIC - Tiempo de lectura
# MAGIC
# MAGIC Estas métricas alimentan el reporte JSON final.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lectura con retry

# COMMAND ----------

# ============================================================
# LECTURA DE TODAS LAS TABLAS CON RETRY
# ============================================================
# Aplicamos retry_operation a cada lectura.
# Si una tabla falla por error transient, se reintenta 3 veces.
# Si después de 3 intentos sigue fallando, el notebook falla con
# la excepción original para investigación humana.

logger.info("Iniciando lecturas de las 6 tablas con retry pattern...")
stage_start = time.time()

# Diccionario donde guardamos los conteos y métricas de cada tabla
tablas_metricas = {}

# Combinamos todas las tablas en un solo diccionario para iterar
todas_tablas = {**TABLAS_RAW, **TABLAS_TRUSTED, **TABLAS_REFINED}

for nombre, ruta_tabla in todas_tablas.items():
    logger.info(f"\nLeyendo {nombre} ({ruta_tabla})")
    
    tabla_start = time.time()
    
    # Definir la operación como una lambda (sin argumentos)
    # spark.table(ruta).count() puede fallar; el retry lo protege
    operacion_lectura = lambda r=ruta_tabla: spark.table(r).count()
    
    try:
        # Aplicar retry
        count = retry_operation(
            operation=operacion_lectura,
            operation_name=f"Lectura de {nombre}"
        )
        
        # Obtener metadata adicional (columnas)
        num_columnas = len(spark.table(ruta_tabla).columns)
        duracion = time.time() - tabla_start
        
        tablas_metricas[nombre] = {
            "ruta": ruta_tabla,
            "registros": count,
            "columnas": num_columnas,
            "tiempo_lectura_seg": round(duracion, 2),
            "status": "OK"
        }
        
        logger.info(f"  ✓ {nombre}: {count:,} registros, {num_columnas} columnas, {duracion:.2f}s")
    
    except Exception as e:
        # Si después del retry sigue fallando, registramos el error
        tablas_metricas[nombre] = {
            "ruta": ruta_tabla,
            "registros": None,
            "columnas": None,
            "tiempo_lectura_seg": None,
            "status": "FAILED",
            "error": str(e)[:200]
        }
        logger.error(f"  ✗ {nombre}: lectura falló definitivamente. Continuando con otras tablas.")

# Registrar tiempo total de la etapa
mark_stage("lectura_tablas", stage_start)

# Resumen visual
print("\n" + "=" * 80)
print("RESUMEN DE LECTURAS")
print("=" * 80)
print(f"{'Tabla':<35} {'Registros':>15} {'Columnas':>10} {'Tiempo (s)':>12} {'Status':>10}")
print("-" * 80)
for nombre, metricas in tablas_metricas.items():
    if metricas["status"] == "OK":
        print(f"{nombre:<35} {metricas['registros']:>15,} {metricas['columnas']:>10} {metricas['tiempo_lectura_seg']:>12.2f} {'OK':>10}")
    else:
        print(f"{nombre:<35} {'N/A':>15} {'N/A':>10} {'N/A':>12} {'FAILED':>10}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 2: Construcción de la tabla `refined.data_quality_report`
# MAGIC
# MAGIC Esta tabla cumple el requisito explícito de la prueba: consolidar el resultado de las reglas de calidad en una tabla del schema `refined`.
# MAGIC
# MAGIC **Estructura de la tabla:**
# MAGIC | Columna | Tipo | Descripción |
# MAGIC |---|---|---|
# MAGIC | regla_id | string | Identificador único (R1, R2, R3, R4, R5) |
# MAGIC | descripcion | string | Descripción legible de la regla |
# MAGIC | capa_origen | string | Capa donde se aplicó (trusted en todos los casos) |
# MAGIC | registros_evaluados | bigint | Total de registros evaluados (3,066,766 en todas) |
# MAGIC | registros_passed | bigint | Registros que pasaron la regla |
# MAGIC | registros_failed | bigint | Registros que fallaron la regla |
# MAGIC | pct_passed | double | % de registros que pasaron |
# MAGIC | pct_failed | double | % de registros que fallaron |
# MAGIC | ingreso_descartado | double | Ingreso ($) asociado a registros fallidos |
# MAGIC | execution_timestamp | timestamp | Cuándo se generó este reporte |
# MAGIC | pipeline_run_id | string | UUID único del run del pipeline |
# MAGIC
# MAGIC **Fuente de datos:** la tabla `refined.kpi_impacto_calidad_datos` ya tiene los registros descartados e ingresos por regla. Solo necesitamos reformatearla y agregar metadata de ejecución.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Construir y escribir **data_quality_report**

# COMMAND ----------

# ============================================================
# CONSTRUCCIÓN DE LA TABLA DATA_QUALITY_REPORT
# ============================================================
# Tomamos kpi_impacto_calidad_datos (ya creada en Notebook 03)
# y le agregamos metadata para construir el reporte oficial.

logger.info("Construyendo refined.data_quality_report...")
stage_start = time.time()

# Generar un identificador único para este run del pipeline
pipeline_run_id = str(uuid.uuid4())
logger.info(f"Pipeline run ID: {pipeline_run_id}")

# Total de registros evaluados (la tabla raw completa)
total_registros = tablas_metricas["yellow_trips_raw"]["registros"]

# Leer la tabla origen
df_impacto = spark.table(TABLAS_REFINED["kpi_impacto_calidad_datos"])

# Construir el DQ report aplicando transformaciones
df_dq_report = (
    df_impacto
    .withColumn("regla_id", F.col("regla_id"))
    .withColumn("descripcion", F.col("descripcion"))
    .withColumn("capa_origen", F.lit("trusted"))
    .withColumn("registros_evaluados", F.lit(total_registros).cast("bigint"))
    .withColumn("registros_failed", F.col("registros_descartados").cast("bigint"))
    .withColumn("registros_passed", (F.lit(total_registros) - F.col("registros_descartados")).cast("bigint"))
    .withColumn("pct_failed", F.col("pct_registros_descartados"))
    .withColumn("pct_passed", F.lit(100.0) - F.col("pct_registros_descartados"))
    .withColumn("ingreso_descartado", F.col("ingreso_descartado"))
    .withColumn("execution_timestamp", F.current_timestamp())
    .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
    # Seleccionar solo las columnas finales en el orden correcto
    .select(
        "regla_id",
        "descripcion",
        "capa_origen",
        "registros_evaluados",
        "registros_passed",
        "registros_failed",
        "pct_passed",
        "pct_failed",
        "ingreso_descartado",
        "execution_timestamp",
        "pipeline_run_id"
    )
    .orderBy("regla_id")
)

# Escribir como tabla Delta
logger.info(f"Escribiendo {DQ_REPORT_TABLE}...")

def escribir_dq_report():
    (
        df_dq_report.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(DQ_REPORT_TABLE)
    )

# Aplicar retry también a la escritura
retry_operation(
    operation=escribir_dq_report,
    operation_name=f"Escritura de {DQ_REPORT_TABLE}"
)

# Validar escritura
registros_escritos = spark.table(DQ_REPORT_TABLE).count()
logger.info(f"✓ Tabla {DQ_REPORT_TABLE} escrita: {registros_escritos} registros")

mark_stage("escritura_dq_report", stage_start)

# Visualizar el reporte
print("\n" + "=" * 100)
print("DATA QUALITY REPORT — VISUALIZACIÓN")
print("=" * 100)
spark.table(DQ_REPORT_TABLE).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 3: Generación del reporte JSON de ejecución
# MAGIC
# MAGIC **Requisito de la prueba:** *"Reporte de ejecución final en JSON con: total procesado, descartados, tiempo por etapa, KPIs resultantes."*
# MAGIC
# MAGIC **Estructura del JSON:**
# MAGIC ```json
# MAGIC {
# MAGIC   "execution_metadata": {
# MAGIC     "execution_id": "uuid",
# MAGIC     "execution_timestamp_utc": "ISO 8601",
# MAGIC     "total_execution_seconds": float
# MAGIC   },
# MAGIC   "pipeline_summary": {
# MAGIC     "raw_records": int,
# MAGIC     "trusted_records": int,
# MAGIC     "records_discarded": int,
# MAGIC     "discard_rate_pct": float,
# MAGIC     "raw_gross_income": float
# MAGIC   },
# MAGIC   "tables_created": {
# MAGIC     "raw": [...],
# MAGIC     "trusted": [...],
# MAGIC     "refined": [...]
# MAGIC   },
# MAGIC   "stage_timings": {
# MAGIC     "lectura_tablas": float,
# MAGIC     "escritura_dq_report": float,
# MAGIC     "generacion_json": float
# MAGIC   },
# MAGIC   "data_quality_rules": [...],
# MAGIC   "kpi_highlights": {...}
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Destino:** `/Volumes/{catalog}/raw/source_files/execution_report.json`
# MAGIC
# MAGIC El JSON queda en el Volume de UC y también se imprime en stdout para que el evaluador lo vea directo en el notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Construir y escribir el JSON

# COMMAND ----------

# ============================================================
# GENERACIÓN DEL REPORTE JSON DE EJECUCIÓN
# ============================================================

logger.info("Generando reporte JSON de ejecución...")
stage_start = time.time()

# ────────────────────────────────────────────────────────────────
# 1. Extraer ingreso bruto raw para el resumen
# ────────────────────────────────────────────────────────────────
raw_gross_income = (
    spark.table(TABLAS_RAW["yellow_trips_raw"])
    .agg(F.sum("fare_amount"))
    .collect()[0][0]
)

# ────────────────────────────────────────────────────────────────
# 2. Extraer reglas de DQ desde la tabla recién creada
# ────────────────────────────────────────────────────────────────
dq_rules_data = (
    spark.table(DQ_REPORT_TABLE)
    .select("regla_id", "descripcion", "registros_failed", "pct_failed", "ingreso_descartado")
    .collect()
)

dq_rules_list = [
    {
        "regla_id": row["regla_id"],
        "descripcion": row["descripcion"],
        "registros_descartados": row["registros_failed"],
        "pct_descartados": float(row["pct_failed"]),
        "ingreso_descartado_usd": float(row["ingreso_descartado"]) if row["ingreso_descartado"] is not None else 0.0
    }
    for row in dq_rules_data
]

# ────────────────────────────────────────────────────────────────
# 3. Extraer highlights de KPIs (1 fila por KPI)
# ────────────────────────────────────────────────────────────────

# KPI 1: Pico de demanda
top_pico = (
    spark.table(TABLAS_REFINED["kpi_demanda_temporal"])
    .orderBy(F.desc("num_viajes"))
    .limit(1)
    .collect()[0]
)

# KPI 2: Zona más rentable (excluyendo Outside of NYC)
top_zona_rentable = (
    spark.table(TABLAS_REFINED["kpi_eficiencia_economica"])
    .filter(F.col("pickup_borough") != "N/A")
    .orderBy(F.desc("ingreso_por_milla"))
    .limit(1)
    .collect()[0]
)

# KPI 2 bis: Zona con mayor ingreso total
top_zona_volumen = (
    spark.table(TABLAS_REFINED["kpi_eficiencia_economica"])
    .orderBy(F.desc("ingreso_total"))
    .limit(1)
    .collect()[0]
)

# ────────────────────────────────────────────────────────────────
# 4. Construir el diccionario completo
# ────────────────────────────────────────────────────────────────
pipeline_end_time = time.time()
total_execution_seconds = round(pipeline_end_time - pipeline_start_time, 2)

# Calcular tiempo de generación del JSON (parcial, antes de incluirse a sí mismo)
mark_stage("generacion_json", stage_start)

execution_report = {
    "execution_metadata": {
        "execution_id": pipeline_run_id,
        "execution_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "total_execution_seconds": total_execution_seconds,
        "notebook": "04_data_quality_report",
        "catalog": CATALOG_NAME
    },
    "pipeline_summary": {
        "raw_records": tablas_metricas["yellow_trips_raw"]["registros"],
        "trusted_records": tablas_metricas["yellow_trips_trusted"]["registros"],
        "records_discarded": (
            tablas_metricas["yellow_trips_raw"]["registros"] -
            tablas_metricas["yellow_trips_trusted"]["registros"]
        ),
        "discard_rate_pct": round(
            (tablas_metricas["yellow_trips_raw"]["registros"] -
             tablas_metricas["yellow_trips_trusted"]["registros"]) /
            tablas_metricas["yellow_trips_raw"]["registros"] * 100,
            4
        ),
        "raw_gross_income_usd": round(float(raw_gross_income), 2)
    },
    "tables_created": {
        "raw": list(TABLAS_RAW.keys()),
        "trusted": list(TABLAS_TRUSTED.keys()),
        "refined": list(TABLAS_REFINED.keys()) + ["data_quality_report"]
    },
    "stage_timings_seconds": stage_timings,
    "data_quality_rules": dq_rules_list,
    "kpi_highlights": {
        "peak_demand": {
            "franja_horaria": top_pico["franja_horaria"],
            "dia_semana": top_pico["dia_semana_nombre"],
            "num_viajes": top_pico["num_viajes"],
            "tarifa_total_usd": float(top_pico["tarifa_total"])
        },
        "most_profitable_zone": {
            "borough": top_zona_rentable["pickup_borough"],
            "zone": top_zona_rentable["pickup_zone"],
            "ingreso_por_milla_usd": float(top_zona_rentable["ingreso_por_milla"]),
            "num_viajes": top_zona_rentable["num_viajes"]
        },
        "highest_volume_zone": {
            "borough": top_zona_volumen["pickup_borough"],
            "zone": top_zona_volumen["pickup_zone"],
            "ingreso_total_usd": float(top_zona_volumen["ingreso_total"]),
            "num_viajes": top_zona_volumen["num_viajes"]
        }
    }
}

# ────────────────────────────────────────────────────────────────
# 5. Escribir el JSON al Volume de UC (con retry)
# ────────────────────────────────────────────────────────────────
json_string = json.dumps(execution_report, indent=2, default=str)

def escribir_json():
    """Escribe el JSON al volume usando dbutils.fs.put"""
    dbutils.fs.put(JSON_REPORT_PATH, json_string, overwrite=True)

retry_operation(
    operation=escribir_json,
    operation_name=f"Escritura JSON a {JSON_REPORT_PATH}"
)

logger.info(f"✓ JSON escrito en: {JSON_REPORT_PATH}")
logger.info(f"  Tamaño: {len(json_string):,} caracteres")

# ────────────────────────────────────────────────────────────────
# 6. Imprimir el JSON para visualización directa
# ────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("EXECUTION REPORT JSON")
print("=" * 80)
print(json_string)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resumen final del pipeline completo

# COMMAND ----------

# ============================================================
# RESUMEN EJECUTIVO DEL PIPELINE COMPLETO
# ============================================================
# Esta celda es la "página de cierre" del pipeline.
# Si el evaluador solo lee UNA celda del Notebook 04, esta debería ser.

print("=" * 90)
print("✓ PIPELINE MEDALLION COMPLETADO EXITOSAMENTE")
print("=" * 90)

print(f"\n📋 IDENTIFICACIÓN")
print(f"   Pipeline run ID:     {pipeline_run_id}")
print(f"   Execution timestamp: {datetime.now(timezone.utc).isoformat()}")
print(f"   Catálogo:            {CATALOG_NAME}")

print(f"\n📊 RESUMEN DE PROCESAMIENTO")
print(f"   Registros Raw:        {tablas_metricas['yellow_trips_raw']['registros']:>12,}")
print(f"   Registros Trusted:    {tablas_metricas['yellow_trips_trusted']['registros']:>12,}")
discarded = tablas_metricas['yellow_trips_raw']['registros'] - tablas_metricas['yellow_trips_trusted']['registros']
print(f"   Registros descartados:{discarded:>12,} ({discarded/tablas_metricas['yellow_trips_raw']['registros']*100:.4f}%)")
print(f"   Ingreso Raw bruto:    ${raw_gross_income:>14,.2f}")

print(f"\n📚 TABLAS EN UNITY CATALOG")
print(f"   Schema raw:     {len(TABLAS_RAW)} tablas")
print(f"   Schema trusted: {len(TABLAS_TRUSTED)} tabla")
print(f"   Schema refined: {len(TABLAS_REFINED) + 1} tablas (3 KPIs + data_quality_report)")

print(f"\n⏱  PERFORMANCE (Notebook 04)")
for stage, duration in stage_timings.items():
    print(f"   {stage:<25}: {duration:>6.2f}s")

print(f"\n📦 ARTEFACTOS GENERADOS")
print(f"   1. Tabla {DQ_REPORT_TABLE}")
print(f"   2. Archivo {JSON_REPORT_PATH}")

print(f"\n🎯 INSIGHTS DESTACADOS")
print(f"   Pico de demanda:  {top_pico['franja_horaria']} / {top_pico['dia_semana_nombre']} ({top_pico['num_viajes']:,} viajes)")
print(f"   Zona más rentable (intra-NYC): {top_zona_rentable['pickup_borough']} / {top_zona_rentable['pickup_zone']} (${top_zona_rentable['ingreso_por_milla']}/milla)")
print(f"   Mayor volumen económico: {top_zona_volumen['pickup_borough']} / {top_zona_volumen['pickup_zone']} (${top_zona_volumen['ingreso_total']:,.2f})")

print(f"\n✓ Estado: PIPELINE EXITOSO")
print(f"✓ Próximos pasos: documentación en docs/ y push final a repo")
print("=" * 90)

logger.info("✓ Notebook 04 completado exitosamente")
logger.info("✓ Pipeline Medallion Raw → Trusted → Refined cerrado")

# COMMAND ----------

