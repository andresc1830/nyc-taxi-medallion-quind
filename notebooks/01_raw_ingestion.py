# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 01 — Capa Raw: Ingesta de archivos fuente
# MAGIC
# MAGIC **Objetivo:** Cargar los archivos del Volume `source_files` como tablas Delta en el schema `raw`, sin transformaciones más allá del tipado básico y la adición de metadata de auditoría.
# MAGIC
# MAGIC **Inputs:**
# MAGIC - `/Volumes/nyc_taxi_andres_rojas/raw/source_files/yellow_tripdata_2023-01.parquet`
# MAGIC - `/Volumes/nyc_taxi_andres_rojas/raw/source_files/taxi_zone_lookup.csv`
# MAGIC
# MAGIC **Outputs:**
# MAGIC - Tabla `nyc_taxi_andres_rojas.raw.yellow_trips_raw`
# MAGIC - Tabla `nyc_taxi_andres_rojas.raw.taxi_zones_raw`
# MAGIC
# MAGIC **Principios aplicados:**
# MAGIC 1. **Idempotencia:** uso `mode("overwrite")` para que el notebook sea re-ejecutable sin duplicar datos.
# MAGIC 2. **Trazabilidad:** agrego columnas `_ingestion_timestamp` y `_source_file` a cada tabla Raw para saber cuándo y de dónde vino cada registro.
# MAGIC 3. **Cero transformación lógica:** la capa Raw es fiel a la fuente. Cualquier limpieza o validación va en Trusted (Notebook 02).
# MAGIC
# MAGIC **Tiempo estimado de ejecución:** 1-2 minutos

# COMMAND ----------

# MAGIC %md
# MAGIC **Variables de configuración**

# COMMAND ----------

# ============================================================
# CONFIGURACIÓN
# ============================================================
# Centralizar paths y nombres en variables facilita mantenimiento.
# Si cambia el catálogo o el volume, se modifica aquí UNA VEZ
# y se propaga a todas las operaciones del notebook.

CATALOG_NAME = "nyc_taxi_andres_rojas"
RAW_SCHEMA = "raw"
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{RAW_SCHEMA}/source_files"

# Archivos fuente (paths absolutos al volume de UC)
PARQUET_FILE = f"{VOLUME_PATH}/yellow_tripdata_2023-01.parquet"
CSV_FILE = f"{VOLUME_PATH}/taxi_zone_lookup.csv"

# Tablas destino (formato: catalog.schema.table)
TRIPS_TABLE = f"{CATALOG_NAME}.{RAW_SCHEMA}.yellow_trips_raw"
ZONES_TABLE = f"{CATALOG_NAME}.{RAW_SCHEMA}.taxi_zones_raw"

print("Paths configurados:")
print(f"  Parquet: {PARQUET_FILE}")
print(f"  CSV:     {CSV_FILE}")
print(f"\nTablas destino:")
print(f"  Trips: {TRIPS_TABLE}")
print(f"  Zones: {ZONES_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Logger e imports**

# COMMAND ----------

# ============================================================
# LOGGING SETUP
# ============================================================
# La prueba pide niveles INFO/WARNING/ERROR en cada etapa.
# Uso el módulo `logging` estándar de Python.
# En notebooks posteriores este logger se centralizará en src/utils/logger.py
# (refactor que haremos cuando los 4 notebooks compartan el mismo logger).

import logging
from datetime import datetime
import os

# Configurar el logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("raw_ingestion")

# Imports de PySpark que usaremos en este notebook
from pyspark.sql import functions as F

logger.info("Logger inicializado correctamente")
logger.info(f"Notebook ejecutado en: {datetime.now().isoformat()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 1: Ingesta del Parquet de viajes (yellow_tripdata)

# COMMAND ----------

# MAGIC %md
# MAGIC **Leer el Parquet**

# COMMAND ----------

# ============================================================
# LECTURA DEL PARQUET (yellow_tripdata_2023-01.parquet)
# ============================================================
# El parquet es un formato columnar tipado. Spark infiere el schema
# automáticamente desde los metadatos del archivo, por eso NO necesitamos
# pasarle un schema explícito (sí lo haríamos con CSV, ver Parte 2).

logger.info(f"Iniciando lectura del archivo: {PARQUET_FILE}")

df_trips_raw = (
    spark.read
        .format("parquet")
        .load(PARQUET_FILE)
)

# Acción: contar registros (esto materializa la lectura)
total_records = df_trips_raw.count()
logger.info(f"Registros leídos del parquet: {total_records:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Inspeccionar el DataFrame**
# MAGIC

# COMMAND ----------

# ============================================================
# INSPECCIÓN DEL DATAFRAME LEÍDO
# ============================================================
# Antes de escribir, validamos que la lectura es correcta.
# Tres chequeos básicos que un senior siempre hace:
#  1. ¿Cuántas columnas y de qué tipo? -> printSchema()
#  2. ¿Cómo se ven los datos? -> show()
#  3. ¿El conteo tiene sentido? -> ya lo hicimos arriba

print("=" * 60)
print("ESTRUCTURA DEL DATAFRAME (Schema)")
print("=" * 60)
df_trips_raw.printSchema()

print("\n" + "=" * 60)
print("PRIMERAS 5 FILAS")
print("=" * 60)
df_trips_raw.show(5, truncate=False)

print(f"\nTotal columnas: {len(df_trips_raw.columns)}")
print(f"Total registros: {total_records:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Agregar metadata y escribir como tabla Delta**

# COMMAND ----------

# ============================================================
# ESCRITURA COMO TABLA DELTA EN EL SCHEMA RAW
# ============================================================
# Antes de escribir, agregamos dos columnas de auditoría:
#   - _ingestion_timestamp: cuándo se cargó este registro al lake
#   - _source_file: de qué archivo vino este registro
# Esto es CRÍTICO para trazabilidad en pipelines reales con múltiples archivos.

source_file_name = os.path.basename(PARQUET_FILE)

df_trips_with_metadata = (
    df_trips_raw
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_file_name))
)

logger.info(f"Metadata agregada. Iniciando escritura a {TRIPS_TABLE}")

# Escribir como tabla Delta gestionada por Unity Catalog
(
    df_trips_with_metadata.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TRIPS_TABLE)
)

logger.info(f"✓ Tabla {TRIPS_TABLE} escrita exitosamente")

# Validar conteo después de escritura
written_count = spark.table(TRIPS_TABLE).count()
logger.info(f"Registros en la tabla escrita: {written_count:,}")

if written_count == total_records:
    logger.info("✓ Validación exitosa: el conteo coincide con la lectura")
else:
    logger.error(f"✗ MISMATCH: leídos {total_records:,}, escritos {written_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 2: Ingesta del CSV de zonas (taxi_zone_lookup)

# COMMAND ----------

# MAGIC %md
# MAGIC **Leer el CSV**

# COMMAND ----------

# ============================================================
# INGESTA DEL CSV DE TAXI ZONES
# ============================================================
# A diferencia del parquet, el CSV NO viene tipado.
# Opciones para leerlo:
#   - Pasar un schema explícito (más seguro, recomendado para producción)
#   - Usar inferSchema=True (Spark lee el archivo dos veces para deducir tipos)
# Para este CSV pequeño (~12KB, 265 filas) inferSchema es razonable.
# En producción se especificaría schema explícito (decisión documentada en README).

logger.info(f"Iniciando lectura del CSV: {CSV_FILE}")

df_zones_raw = (
    spark.read
        .format("csv")
        .option("header", "true")        # primera fila son nombres de columna
        .option("inferSchema", "true")   # deducir tipos automáticamente
        .load(CSV_FILE)
)

zones_count = df_zones_raw.count()
logger.info(f"Registros leídos del CSV: {zones_count}")

print("\nSchema inferido del CSV:")
df_zones_raw.printSchema()

print("\nPrimeras 5 filas:")
df_zones_raw.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **Escribir tabla de zonas**

# COMMAND ----------

# ============================================================
# ESCRITURA DEL CSV COMO TABLA DELTA
# ============================================================
# Mismo patrón que el parquet: agregar metadata + escribir Delta

source_file_name_csv = os.path.basename(CSV_FILE)

df_zones_with_metadata = (
    df_zones_raw
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_file_name_csv))
)

logger.info(f"Iniciando escritura a {ZONES_TABLE}")

(
    df_zones_with_metadata.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(ZONES_TABLE)
)

written_zones = spark.table(ZONES_TABLE).count()
logger.info(f"✓ Tabla {ZONES_TABLE} escrita: {written_zones} registros")

if written_zones == zones_count:
    logger.info("✓ Validación exitosa: el conteo coincide con la lectura")
else:
    logger.error(f"✗ MISMATCH en zonas: leídos {zones_count}, escritos {written_zones}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Validación final consolidada**

# COMMAND ----------

# ============================================================
# VALIDACIÓN FINAL DE LA CAPA RAW
# ============================================================
# Resumen consolidado de todo lo que cargamos.
# Esta celda es la "hoja de cierre" del notebook: si alguien la corre
# de nuevo después, debe ver exactamente este resumen.

print("=" * 60)
print("RESUMEN DE LA INGESTA RAW")
print("=" * 60)

print(f"\nTablas creadas en {CATALOG_NAME}.{RAW_SCHEMA}:")
spark.sql(f"SHOW TABLES IN {CATALOG_NAME}.{RAW_SCHEMA}").show(truncate=False)

print(f"\n--- {TRIPS_TABLE} ---")
print(f"Registros: {spark.table(TRIPS_TABLE).count():,}")
print(f"Columnas: {len(spark.table(TRIPS_TABLE).columns)}")

print(f"\n--- {ZONES_TABLE} ---")
print(f"Registros: {spark.table(ZONES_TABLE).count():,}")
print(f"Columnas: {len(spark.table(ZONES_TABLE).columns)}")

print("\nVerificación de columnas de auditoría en yellow_trips_raw:")
spark.sql(f"""
    SELECT 
        _source_file,
        MIN(_ingestion_timestamp) AS first_ingestion,
        COUNT(*) AS records
    FROM {TRIPS_TABLE}
    GROUP BY _source_file
""").show(truncate=False)

logger.info("✓ Capa Raw completada exitosamente")

# COMMAND ----------

