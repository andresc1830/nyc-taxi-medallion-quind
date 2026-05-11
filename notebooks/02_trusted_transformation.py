# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 02 — Capa Trusted: Limpieza, validación y enriquecimiento
# MAGIC
# MAGIC **Objetivo:** Tomar los datos de la capa Raw, aplicar reglas de calidad de datos, enriquecerlos con información de zonas, y producir una tabla Trusted lista para ser consumida por la capa Refined (KPIs).
# MAGIC
# MAGIC **Inputs:**
# MAGIC - `nyc_taxi_andres_rojas.raw.yellow_trips_raw` (3,066,766 registros)
# MAGIC - `nyc_taxi_andres_rojas.raw.taxi_zones_raw` (265 registros)
# MAGIC
# MAGIC **Outputs:**
# MAGIC - Tabla `nyc_taxi_andres_rojas.trusted.yellow_trips_trusted` (registros válidos enriquecidos)
# MAGIC - Métricas de validación que alimentarán el `data_quality_report` en el Notebook 04
# MAGIC
# MAGIC **Reglas de validación aplicadas:**
# MAGIC
# MAGIC | Regla | Descripción | Justificación |
# MAGIC |---|---|---|
# MAGIC | R1 | `pickup_datetime < dropoff_datetime` | Lógica temporal: no puede llegarse antes de salir |
# MAGIC | R2 | `trip_distance > 0` | Un viaje real implica desplazamiento |
# MAGIC | R3 | `fare_amount > 0` | Una transacción válida implica cobro positivo |
# MAGIC | R4 | `trip_duration_minutes <= 360` | Outlier: viajes >6h son atípicos para taxis urbanos |
# MAGIC | R5 | `trip_duration_minutes >= 1` | Outlier: viajes <1min son errores de medición |
# MAGIC
# MAGIC **Transformaciones aplicadas:**
# MAGIC 1. Estandarización de nombres de columnas a `snake_case`
# MAGIC 2. Cálculo de columnas derivadas: `pickup_date`, `pickup_hour`, `pickup_dayofweek`, `trip_duration_minutes`
# MAGIC 3. Join con tabla de zonas para enriquecer pickup con `borough` y `zone_name`
# MAGIC 4. Particionado por `pickup_date` para optimizar queries temporales
# MAGIC
# MAGIC **Tiempo estimado de ejecución:** 2-3 minutos

# COMMAND ----------

# MAGIC %md
# MAGIC ### Variables de configuración

# COMMAND ----------

# ============================================================
# CONFIGURACIÓN
# ============================================================
# Mismo patrón que el notebook 01: paths centralizados.
# Si cambia el catálogo, se modifica aquí UNA VEZ.

CATALOG_NAME = "nyc_taxi_andres_rojas"
RAW_SCHEMA = "raw"
TRUSTED_SCHEMA = "trusted"

# Tablas fuente (capa Raw)
TRIPS_RAW_TABLE = f"{CATALOG_NAME}.{RAW_SCHEMA}.yellow_trips_raw"
ZONES_RAW_TABLE = f"{CATALOG_NAME}.{RAW_SCHEMA}.taxi_zones_raw"

# Tabla destino (capa Trusted)
TRIPS_TRUSTED_TABLE = f"{CATALOG_NAME}.{TRUSTED_SCHEMA}.yellow_trips_trusted"

# Reglas de validación (umbrales) - centralizados para fácil ajuste
MIN_TRIP_DURATION_MINUTES = 1     # viajes < 1 min son errores
MAX_TRIP_DURATION_MINUTES = 360   # viajes > 6 horas son atípicos para taxis urbanos

print("Configuración del notebook:")
print(f"  Tabla origen viajes: {TRIPS_RAW_TABLE}")
print(f"  Tabla origen zonas:  {ZONES_RAW_TABLE}")
print(f"  Tabla destino:       {TRIPS_TRUSTED_TABLE}")
print(f"\nReglas de validación temporal:")
print(f"  Duración mínima: {MIN_TRIP_DURATION_MINUTES} minuto")
print(f"  Duración máxima: {MAX_TRIP_DURATION_MINUTES} minutos ({MAX_TRIP_DURATION_MINUTES/60}h)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Logger e imports

# COMMAND ----------

# ============================================================
# LOGGING SETUP E IMPORTS
# ============================================================
# Mismo logger del notebook 01. En notebooks futuros este código
# se centralizará en src/utils/logger.py para evitar duplicación.

import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("trusted_transformation")

# Imports de PySpark
from pyspark.sql import functions as F
from pyspark.sql import DataFrame  # solo para type hints en funciones helper

# Diccionario que va a acumular métricas de validación por regla
# Esto alimentará la tabla data_quality_report en el Notebook 04
dq_metrics = {}

logger.info("Logger inicializado")
logger.info(f"Notebook ejecutado en: {datetime.now().isoformat()}")

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Parte 1: Lectura de la capa Raw

# COMMAND ----------

# MAGIC %md
# MAGIC ### Leer las dos tablas Raw

# COMMAND ----------

# ============================================================
# LECTURA DE TABLAS RAW
# ============================================================
# Leemos las dos tablas que creó el Notebook 01.
# Capturamos el conteo inicial para calcular descartes posteriores.

logger.info("Leyendo tablas de la capa Raw...")

# Tabla principal: viajes
df_trips_raw = spark.table(TRIPS_RAW_TABLE)
total_trips_raw = df_trips_raw.count()
logger.info(f"Registros leídos de {TRIPS_RAW_TABLE}: {total_trips_raw:,}")

# Tabla de lookup: zonas
df_zones_raw = spark.table(ZONES_RAW_TABLE)
total_zones = df_zones_raw.count()
logger.info(f"Registros leídos de {ZONES_RAW_TABLE}: {total_zones}")

# Guardar conteo inicial en métricas DQ (será baseline para todos los descartes)
dq_metrics["registros_iniciales"] = total_trips_raw

print("\nVista previa de yellow_trips_raw (3 filas):")
df_trips_raw.select(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "fare_amount",
    "PULocationID",
    "DOLocationID"
).show(3, truncate=False)

print("\nVista previa de taxi_zones_raw (3 filas):")
df_zones_raw.select("LocationID", "Borough", "Zone").show(3, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 2: Validaciones de calidad de datos
# MAGIC
# MAGIC Aplicamos cinco reglas en orden secuencial. Cada regla:
# MAGIC 1. Cuenta cuántos registros pasan/fallan
# MAGIC 2. Registra el resultado en `dq_metrics` para el reporte de DQ
# MAGIC 3. Pasa solo los registros válidos al siguiente paso
# MAGIC
# MAGIC **Estrategia:** filtrado en cascada. Cada filtro reduce el dataset y la siguiente regla opera sobre el resultado del anterior.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cálculo de columna derivada **trip_duration_minutes**

# COMMAND ----------

# ============================================================
# COLUMNA DERIVADA: trip_duration_minutes
# ============================================================
# Algunas reglas de validación dependen de la duración del viaje.
# Como la fuente solo tiene pickup y dropoff datetime, calculamos
# la duración en minutos como columna derivada.
#
# Fórmula: (dropoff_unix - pickup_unix) / 60.0
# El /60.0 (con decimal) fuerza división float para no perder precisión.

logger.info("Calculando columna derivada: trip_duration_minutes")

df_trips_with_duration = df_trips_raw.withColumn(
    "trip_duration_minutes",
    (
        F.unix_timestamp(F.col("tpep_dropoff_datetime")) -
        F.unix_timestamp(F.col("tpep_pickup_datetime"))
    ) / 60.0
)

# Validación visual: ver el rango de duraciones
print("Estadísticas de trip_duration_minutes:")
df_trips_with_duration.select("trip_duration_minutes").describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aplicar las 5 reglas de validación en cascada

# COMMAND ----------

# ============================================================
# APLICACIÓN DE LAS 5 REGLAS DE VALIDACIÓN
# ============================================================
# Estrategia: filtrado en cascada. Cada filter() es una regla.
# Después de cada filtro, contamos cuántos registros descartamos
# y guardamos la métrica en dq_metrics.

logger.info("Iniciando validaciones de calidad de datos...")
print("=" * 70)
print("APLICACIÓN DE REGLAS DE VALIDACIÓN")
print("=" * 70)

# Conteo inicial (antes de validar)
df_current = df_trips_with_duration
records_before = total_trips_raw
print(f"\n[INICIO] Registros antes de validar: {records_before:,}")

# ────────────────────────────────────────────────────────────────
# REGLA R1: pickup_datetime < dropoff_datetime
# ────────────────────────────────────────────────────────────────
df_current = df_current.filter(
    F.col("tpep_pickup_datetime") < F.col("tpep_dropoff_datetime")
)
records_after = df_current.count()
descartados_r1 = records_before - records_after

dq_metrics["R1_pickup_before_dropoff"] = {
    "regla": "pickup_datetime < dropoff_datetime",
    "descartados": descartados_r1,
    "passed": records_after,
    "pct_descartados": round(descartados_r1 / total_trips_raw * 100, 4)
}

print(f"\n[R1] pickup < dropoff")
print(f"     Descartados: {descartados_r1:,} ({descartados_r1/total_trips_raw*100:.4f}%)")
print(f"     Quedan: {records_after:,}")
records_before = records_after

# ────────────────────────────────────────────────────────────────
# REGLA R2: trip_distance > 0
# ────────────────────────────────────────────────────────────────
df_current = df_current.filter(F.col("trip_distance") > 0)
records_after = df_current.count()
descartados_r2 = records_before - records_after

dq_metrics["R2_distance_greater_than_zero"] = {
    "regla": "trip_distance > 0",
    "descartados": descartados_r2,
    "passed": records_after,
    "pct_descartados": round(descartados_r2 / total_trips_raw * 100, 4)
}

print(f"\n[R2] trip_distance > 0")
print(f"     Descartados: {descartados_r2:,} ({descartados_r2/total_trips_raw*100:.4f}%)")
print(f"     Quedan: {records_after:,}")
records_before = records_after

# ────────────────────────────────────────────────────────────────
# REGLA R3: fare_amount > 0
# ────────────────────────────────────────────────────────────────
df_current = df_current.filter(F.col("fare_amount") > 0)
records_after = df_current.count()
descartados_r3 = records_before - records_after

dq_metrics["R3_fare_greater_than_zero"] = {
    "regla": "fare_amount > 0",
    "descartados": descartados_r3,
    "passed": records_after,
    "pct_descartados": round(descartados_r3 / total_trips_raw * 100, 4)
}

print(f"\n[R3] fare_amount > 0")
print(f"     Descartados: {descartados_r3:,} ({descartados_r3/total_trips_raw*100:.4f}%)")
print(f"     Quedan: {records_after:,}")
records_before = records_after

# ────────────────────────────────────────────────────────────────
# REGLA R4: trip_duration_minutes <= 360 (no más de 6 horas)
# ────────────────────────────────────────────────────────────────
df_current = df_current.filter(F.col("trip_duration_minutes") <= MAX_TRIP_DURATION_MINUTES)
records_after = df_current.count()
descartados_r4 = records_before - records_after

dq_metrics["R4_duration_max_6h"] = {
    "regla": f"trip_duration_minutes <= {MAX_TRIP_DURATION_MINUTES}",
    "descartados": descartados_r4,
    "passed": records_after,
    "pct_descartados": round(descartados_r4 / total_trips_raw * 100, 4)
}

print(f"\n[R4] duration <= {MAX_TRIP_DURATION_MINUTES} min ({MAX_TRIP_DURATION_MINUTES/60}h)")
print(f"     Descartados: {descartados_r4:,} ({descartados_r4/total_trips_raw*100:.4f}%)")
print(f"     Quedan: {records_after:,}")
records_before = records_after

# ────────────────────────────────────────────────────────────────
# REGLA R5: trip_duration_minutes >= 1 (al menos 1 minuto)
# ────────────────────────────────────────────────────────────────
df_current = df_current.filter(F.col("trip_duration_minutes") >= MIN_TRIP_DURATION_MINUTES)
records_after = df_current.count()
descartados_r5 = records_before - records_after

dq_metrics["R5_duration_min_1min"] = {
    "regla": f"trip_duration_minutes >= {MIN_TRIP_DURATION_MINUTES}",
    "descartados": descartados_r5,
    "passed": records_after,
    "pct_descartados": round(descartados_r5 / total_trips_raw * 100, 4)
}

print(f"\n[R5] duration >= {MIN_TRIP_DURATION_MINUTES} min")
print(f"     Descartados: {descartados_r5:,} ({descartados_r5/total_trips_raw*100:.4f}%)")
print(f"     Quedan: {records_after:,}")

# ────────────────────────────────────────────────────────────────
# RESUMEN DE VALIDACIONES
# ────────────────────────────────────────────────────────────────
df_trips_validated = df_current
total_validados = df_trips_validated.count()
total_descartados = total_trips_raw - total_validados
pct_descartados_total = total_descartados / total_trips_raw * 100

dq_metrics["resumen"] = {
    "registros_iniciales": total_trips_raw,
    "registros_validos": total_validados,
    "total_descartados": total_descartados,
    "pct_descartados": round(pct_descartados_total, 4)
}

print("\n" + "=" * 70)
print("RESUMEN")
print("=" * 70)
print(f"Registros iniciales:  {total_trips_raw:,}")
print(f"Registros válidos:    {total_validados:,}")
print(f"Total descartados:    {total_descartados:,} ({pct_descartados_total:.4f}%)")

logger.info(f"Validaciones completadas. Válidos: {total_validados:,}, Descartados: {total_descartados:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspeccionar el diccionario **dq_metrics**

# COMMAND ----------

# ============================================================
# INSPECCIÓN DEL DICCIONARIO DE MÉTRICAS DQ
# ============================================================
# Este diccionario se va a serializar como JSON en el Notebook 04
# y también va a alimentar la tabla refined.data_quality_report.

import json

print("Contenido completo de dq_metrics:")
print(json.dumps(dq_metrics, indent=2, default=str))

logger.info(f"dq_metrics tiene {len(dq_metrics)} entradas registradas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 3: Enriquecimiento y estandarización
# MAGIC
# MAGIC Tres acciones:
# MAGIC 1. **Join con `taxi_zones_raw`** para enriquecer cada viaje con borough/zona de pickup
# MAGIC 2. **Cálculo de columnas derivadas** que la capa Refined necesitará (pickup_date, pickup_hour, pickup_dayofweek, average_speed_mph)
# MAGIC 3. **Estandarización de nombres** a `snake_case` consistente

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join con zonas

# COMMAND ----------

# ============================================================
# JOIN CON TAXI ZONES
# ============================================================
# Enriquecemos cada viaje con borough y zona de pickup.
# Decisión: solo enriquecer pickup, NO dropoff.
#   Razón: KPI 2 (eficiencia económica por zona) usa la zona donde
#   se ORIGINA el viaje. Si quisiéramos dropoff también, requeriría
#   un segundo join (lo dejamos como evolución futura).
#
# Tipo de join: LEFT join.
#   Razón: si hay un PULocationID en viajes que no existe en zones,
#   queremos conservar el viaje (con borough=null), no eliminarlo.
#   Un INNER join silenciosamente perdería esos registros.

logger.info("Aplicando join con tabla de zonas...")

# Renombramos columnas en df_zones para evitar ambigüedad después del join
df_zones_for_join = df_zones_raw.select(
    F.col("LocationID").alias("zone_location_id"),
    F.col("Borough").alias("pickup_borough"),
    F.col("Zone").alias("pickup_zone"),
    F.col("service_zone").alias("pickup_service_zone")
)

# Aplicar el join
df_trips_enriched = df_trips_validated.join(
    df_zones_for_join,
    df_trips_validated.PULocationID == df_zones_for_join.zone_location_id,
    "left"
).drop("zone_location_id")  # eliminar columna duplicada después del join

# Validar que el join no perdió registros (era LEFT, no debería)
total_after_join = df_trips_enriched.count()

if total_after_join == total_validados:
    logger.info(f"✓ Join exitoso: {total_after_join:,} registros (sin pérdidas)")
else:
    logger.error(f"✗ Pérdida en join: esperados {total_validados:,}, obtenidos {total_after_join:,}")

# Verificar cuántos viajes tienen pickup_borough = NULL (PULocationID que no estaba en zones)
nulls_in_borough = df_trips_enriched.filter(F.col("pickup_borough").isNull()).count()
logger.info(f"Viajes con pickup_borough = NULL: {nulls_in_borough}")

dq_metrics["join_zones"] = {
    "registros_pre_join": total_validados,
    "registros_post_join": total_after_join,
    "viajes_sin_zona": nulls_in_borough
}

print(f"\nMuestra del DataFrame enriquecido (3 filas):")
df_trips_enriched.select(
    "tpep_pickup_datetime",
    "PULocationID",
    "pickup_borough",
    "pickup_zone",
    "trip_distance",
    "fare_amount"
).show(3, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Columnas derivadas + estandarización de nombres

# COMMAND ----------

# ============================================================
# COLUMNAS DERIVADAS Y ESTANDARIZACIÓN DE NOMBRES
# ============================================================
# Calculamos columnas que la capa Refined va a necesitar:
#   - pickup_date: fecha sin hora (para particionado y agrupación temporal)
#   - pickup_hour: hora del día (0-23) para análisis de franjas horarias
#   - pickup_dayofweek: día de semana (1-7, 1=domingo)
#   - average_speed_mph: distancia / duración en horas (para KPI de eficiencia)
#
# Estandarización: nombres del NYC TLC son inconsistentes:
#   - tpep_pickup_datetime  -> pickup_datetime (eliminar prefijo proveedor)
#   - PULocationID          -> pickup_location_id (snake_case)
#   - DOLocationID          -> dropoff_location_id
# Esto facilita el código posterior y es buena práctica de gobierno.

logger.info("Calculando columnas derivadas y estandarizando nombres...")

df_trips_final = (
    df_trips_enriched
    # Renombrado de columnas a snake_case consistente
    .withColumnRenamed("VendorID", "vendor_id")
    .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
    .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
    .withColumnRenamed("RatecodeID", "ratecode_id")
    .withColumnRenamed("PULocationID", "pickup_location_id")
    .withColumnRenamed("DOLocationID", "dropoff_location_id")
    # Columnas derivadas temporales (usan los nombres ya renombrados)
    .withColumn("pickup_date", F.to_date(F.col("pickup_datetime")))
    .withColumn("pickup_hour", F.hour(F.col("pickup_datetime")))
    .withColumn("pickup_dayofweek", F.dayofweek(F.col("pickup_datetime")))
    # Columna derivada para KPI de eficiencia: velocidad promedio en mph
    # Fórmula: distancia (millas) / duración (horas) = mph
    # CUIDADO: trip_duration_minutes ya está validado >= 1, así que no hay división por cero
    .withColumn(
        "average_speed_mph",
        F.col("trip_distance") / (F.col("trip_duration_minutes") / 60.0)
    )
)

logger.info(f"Columnas derivadas calculadas. Total columnas: {len(df_trips_final.columns)}")

print(f"\nSchema final del DataFrame Trusted ({len(df_trips_final.columns)} columnas):")
df_trips_final.printSchema()

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Parte 4: Escritura como tabla Delta particionada
# MAGIC
# MAGIC **Decisión de particionado:** `pickup_date`
# MAGIC
# MAGIC **Justificación:**
# MAGIC - Los KPIs principales son temporales (franja horaria, día de semana)
# MAGIC - Queries típicas: `WHERE pickup_date BETWEEN ... AND ...` o `GROUP BY pickup_date`
# MAGIC - Un mes con ~31 particiones de ~96K registros cada una es óptimo (regla: ~100K-1M por partición)
# MAGIC - Evitar PULocationID como partición principal (265 zonas → demasiadas particiones pequeñas)
# MAGIC
# MAGIC **Optimización adicional (no aplicada acá, sí en evolución a producción):**
# MAGIC `OPTIMIZE ... ZORDER BY (pickup_location_id, pickup_hour)` después de la escritura,
# MAGIC para ordenamiento físico secundario dentro de cada partición.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Escritura particionada

# COMMAND ----------

# ============================================================
# ESCRITURA COMO TABLA DELTA PARTICIONADA
# ============================================================
# Particionamos por pickup_date para optimizar queries temporales.
# mode="overwrite" + overwriteSchema=true para idempotencia y permitir
# evolución de schema durante desarrollo.

logger.info(f"Iniciando escritura a {TRIPS_TRUSTED_TABLE}")
logger.info(f"Particionado por: pickup_date")

(
    df_trips_final.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("pickup_date")
        .saveAsTable(TRIPS_TRUSTED_TABLE)
)

logger.info(f"✓ Tabla {TRIPS_TRUSTED_TABLE} escrita exitosamente")

# Validación post-escritura
written_count = spark.table(TRIPS_TRUSTED_TABLE).count()
logger.info(f"Registros en la tabla escrita: {written_count:,}")

if written_count == total_validados:
    logger.info("✓ Validación exitosa: el conteo escrito coincide con el validado")
else:
    logger.error(f"✗ MISMATCH: validados {total_validados:,}, escritos {written_count:,}")

# Verificar particiones creadas
partitions = spark.sql(f"""
    SHOW PARTITIONS {TRIPS_TRUSTED_TABLE}
""").count()
logger.info(f"Total particiones creadas: {partitions}")

dq_metrics["escritura_trusted"] = {
    "registros_escritos": written_count,
    "particiones_creadas": partitions,
    "tabla_destino": TRIPS_TRUSTED_TABLE
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validación final + resumen consolidado

# COMMAND ----------

# ============================================================
# RESUMEN FINAL DEL NOTEBOOK 02
# ============================================================
# Salida final que muestra todo lo logrado.
# Esta celda sirve como evidencia de ejecución exitosa.

print("=" * 70)
print("RESUMEN DE LA CAPA TRUSTED")
print("=" * 70)

# 1. Información de la tabla creada
print(f"\nTabla destino: {TRIPS_TRUSTED_TABLE}")
print(f"Registros totales:    {dq_metrics['resumen']['registros_validos']:,}")
print(f"Columnas:             {len(spark.table(TRIPS_TRUSTED_TABLE).columns)}")
print(f"Particiones físicas:  {dq_metrics['escritura_trusted']['particiones_creadas']}")

# 2. Reporte de calidad de datos consolidado
print(f"\n{'─' * 70}")
print("REPORTE DE CALIDAD DE DATOS")
print(f"{'─' * 70}")
print(f"\nDescartes por regla:")
print(f"  R1 (pickup<dropoff):   {dq_metrics['R1_pickup_before_dropoff']['descartados']:>8,} ({dq_metrics['R1_pickup_before_dropoff']['pct_descartados']:>6.4f}%)")
print(f"  R2 (distance>0):       {dq_metrics['R2_distance_greater_than_zero']['descartados']:>8,} ({dq_metrics['R2_distance_greater_than_zero']['pct_descartados']:>6.4f}%)")
print(f"  R3 (fare>0):           {dq_metrics['R3_fare_greater_than_zero']['descartados']:>8,} ({dq_metrics['R3_fare_greater_than_zero']['pct_descartados']:>6.4f}%)")
print(f"  R4 (duration<=6h):     {dq_metrics['R4_duration_max_6h']['descartados']:>8,} ({dq_metrics['R4_duration_max_6h']['pct_descartados']:>6.4f}%)")
print(f"  R5 (duration>=1min):   {dq_metrics['R5_duration_min_1min']['descartados']:>8,} ({dq_metrics['R5_duration_min_1min']['pct_descartados']:>6.4f}%)")
print(f"\n  Total descartados:    {dq_metrics['resumen']['total_descartados']:>8,} ({dq_metrics['resumen']['pct_descartados']:>6.4f}%)")
print(f"  Tasa de validez:      {100 - dq_metrics['resumen']['pct_descartados']:>6.4f}%")

# 3. Vista previa de la tabla Trusted final
print(f"\n{'─' * 70}")
print("MUESTRA DE LA TABLA TRUSTED (5 filas)")
print(f"{'─' * 70}")
spark.table(TRIPS_TRUSTED_TABLE).select(
    "pickup_datetime",
    "pickup_borough",
    "pickup_zone",
    "trip_distance",
    "trip_duration_minutes",
    "average_speed_mph",
    "fare_amount",
    "total_amount"
).show(5, truncate=False)

# 4. Distribución por borough (sneak peek de lo que vendrá en KPIs)
print(f"\n{'─' * 70}")
print("DISTRIBUCIÓN DE VIAJES POR BOROUGH (top 5)")
print(f"{'─' * 70}")
spark.sql(f"""
    SELECT 
        pickup_borough,
        COUNT(*) AS num_trips,
        ROUND(AVG(fare_amount), 2) AS avg_fare,
        ROUND(AVG(trip_distance), 2) AS avg_distance_miles
    FROM {TRIPS_TRUSTED_TABLE}
    GROUP BY pickup_borough
    ORDER BY num_trips DESC
    LIMIT 5
""").show(truncate=False)

logger.info("✓ Capa Trusted completada exitosamente")
logger.info(f"Listo para Notebook 03 (Refined / KPIs)")