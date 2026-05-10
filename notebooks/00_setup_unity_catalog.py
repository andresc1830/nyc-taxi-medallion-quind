# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # Notebook 00 — Setup de Unity Catalog
# MAGIC
# MAGIC **Objetivo:** Crear la estructura de gobierno de datos en Unity Catalog para la prueba técnica QUIND.
# MAGIC
# MAGIC **Lo que hace este notebook:**
# MAGIC 1. Crea el catálogo `nyc_taxi_andres_rojas` que contendrá toda la solución
# MAGIC 2. Crea los 3 schemas de la arquitectura Medallion: `raw`, `trusted`, `refined`
# MAGIC 3. Crea un Volume llamado `source_files` para almacenar los archivos fuente (Parquet y CSV)
# MAGIC 4. Verifica que todo quedó creado correctamente
# MAGIC
# MAGIC **Por qué este notebook existe:** Toda la infraestructura de gobierno se crea por código (Infrastructure as Code) para garantizar reproducibilidad. Cualquier persona puede correr este notebook y replicar la estructura exacta.
# MAGIC
# MAGIC **Tiempo estimado de ejecución:** 30 segundos

# COMMAND ----------

# Variables centrales del proyecto
# Si necesitas cambiar el nombre del catálogo, hazlo aquí UNA VEZ y se propaga al resto del notebook
CATALOG_NAME = "nyc_taxi_andres_rojas"
SCHEMAS = ["raw", "trusted", "refined"]
VOLUME_NAME = "source_files"

print(f"Catálogo a crear: {CATALOG_NAME}")
print(f"Schemas: {SCHEMAS}")
print(f"Volume: {VOLUME_NAME}")

# COMMAND ----------

# Crear el catálogo principal
# IF NOT EXISTS evita errores si ya existe (idempotencia)
# COMMENT documenta el propósito del catálogo (buena práctica de gobierno)
spark.sql(f"""
    CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}
    COMMENT 'Prueba técnica QUIND - NYC Yellow Taxi Trips - Arquitectura Medallion (Raw -> Trusted -> Refined)'
""")

print(f"✓ Catálogo {CATALOG_NAME} creado o ya existente")

# COMMAND ----------

# Crear los 3 schemas de la arquitectura Medallion
# Cada schema tiene un propósito claro y documentado

schema_descriptions = {
    "raw": "Capa Raw - Ingesta sin transformación. Solo tipado básico desde fuentes originales.",
    "trusted": "Capa Trusted - Datos limpios, validados (pickup<dropoff, distance>0, fare>0) y enriquecidos con Taxi Zones.",
    "refined": "Capa Refined - KPIs de negocio: demanda temporal, eficiencia económica, reporte de calidad."
}

for schema, description in schema_descriptions.items():
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{schema}
        COMMENT '{description}'
    """)
    print(f"✓ Schema {CATALOG_NAME}.{schema} listo")

# COMMAND ----------

# Crear un Volume gestionado en el schema raw
# Los Volumes son el lugar correcto en UC para almacenar archivos (no son tablas)
# Aquí van a vivir los archivos parquet y CSV antes de ser cargados a tablas Delta

spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.raw.{VOLUME_NAME}
    COMMENT 'Almacena archivos fuente: yellow_tripdata_2023-01.parquet y taxi_zone_lookup.csv'
""")

print(f"✓ Volume {CATALOG_NAME}.raw.{VOLUME_NAME} listo")
print(f"\nRuta del volume: /Volumes/{CATALOG_NAME}/raw/{VOLUME_NAME}/")

# COMMAND ----------

# Verificar que todo quedó creado correctamente
print("=" * 60)
print("VERIFICACIÓN DEL SETUP")
print("=" * 60)

print(f"\n1. Schemas en {CATALOG_NAME}:")
spark.sql(f"SHOW SCHEMAS IN {CATALOG_NAME}").show(truncate=False)

print(f"\n2. Volumes en {CATALOG_NAME}.raw:")
spark.sql(f"SHOW VOLUMES IN {CATALOG_NAME}.raw").show(truncate=False)

print(f"\n3. Detalles del catálogo:")
spark.sql(f"DESCRIBE CATALOG EXTENDED {CATALOG_NAME}").show(truncate=False)

print("\n✓ Setup de Unity Catalog completado exitosamente")

# COMMAND ----------

# MAGIC %md
# MAGIC