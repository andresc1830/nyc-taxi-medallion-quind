# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 03 — Capa Refined: Cálculo de KPIs de negocio
# MAGIC
# MAGIC **Objetivo:** Tomar la tabla `trusted.yellow_trips_trusted` y producir tres tablas analíticas en `refined` que respondan preguntas concretas de negocio.
# MAGIC
# MAGIC **Inputs:**
# MAGIC - `nyc_taxi_andres_rojas.trusted.yellow_trips_trusted` (2,987,000 registros)
# MAGIC
# MAGIC **Outputs:**
# MAGIC | Tabla | Pregunta de negocio | Tipo |
# MAGIC |---|---|---|
# MAGIC | `refined.kpi_demanda_temporal` | ¿Cuándo viaja la gente y cuál es el patrón? | OBLIGATORIO |
# MAGIC | `refined.kpi_eficiencia_economica` | ¿Qué zonas son las más rentables? | OBLIGATORIO |
# MAGIC | `refined.kpi_impacto_calidad_datos` | ¿Cuánto ingreso perdimos por filtrar datos malos? | OPCIONAL |
# MAGIC
# MAGIC **KPI 1 — Patrón de demanda temporal**
# MAGIC Métricas: número de viajes, duración promedio, tarifa promedio.
# MAGIC Dimensiones: franja horaria (6 franjas) × día de la semana.
# MAGIC Identificación de picos.
# MAGIC
# MAGIC **KPI 2 — Eficiencia económica por zona**
# MAGIC Métricas: ingreso promedio por milla, velocidad promedio.
# MAGIC Dimensiones: borough y zona.
# MAGIC Ranking top 10 zonas más rentables.
# MAGIC
# MAGIC **KPI 3 — Impacto de calidad de datos (opcional)**
# MAGIC Comparación de ingresos totales con y sin registros filtrados.
# MAGIC Cuantifica el impacto económico de las reglas de validación.
# MAGIC
# MAGIC **Tiempo estimado de ejecución:** 3-5 minutos
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Variables de configuración

# COMMAND ----------

# ============================================================
# CONFIGURACIÓN
# ============================================================

CATALOG_NAME = "nyc_taxi_andres_rojas"
TRUSTED_SCHEMA = "trusted"
REFINED_SCHEMA = "refined"

# Tabla origen
TRIPS_TRUSTED_TABLE = f"{CATALOG_NAME}.{TRUSTED_SCHEMA}.yellow_trips_trusted"

# Tablas destino (3 KPIs)
KPI1_TABLE = f"{CATALOG_NAME}.{REFINED_SCHEMA}.kpi_demanda_temporal"
KPI2_TABLE = f"{CATALOG_NAME}.{REFINED_SCHEMA}.kpi_eficiencia_economica"
KPI3_TABLE = f"{CATALOG_NAME}.{REFINED_SCHEMA}.kpi_impacto_calidad_datos"

print("Configuración del notebook:")
print(f"  Tabla origen: {TRIPS_TRUSTED_TABLE}")
print(f"\nTablas destino (KPIs):")
print(f"  KPI 1: {KPI1_TABLE}")
print(f"  KPI 2: {KPI2_TABLE}")
print(f"  KPI 3: {KPI3_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Logger e imports

# COMMAND ----------

# ============================================================
# LOGGING SETUP E IMPORTS
# ============================================================

import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("refined_kpis")

# Imports de PySpark
from pyspark.sql import functions as F
from pyspark.sql.window import Window  # NUEVO: para window functions en ranking

logger.info("Logger inicializado")
logger.info(f"Notebook ejecutado en: {datetime.now().isoformat()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lectura de la tabla Trusted

# COMMAND ----------

# ============================================================
# LECTURA DE LA CAPA TRUSTED
# ============================================================
# Leemos la tabla producida por el Notebook 02.
# Esta tabla ya tiene:
#   - Datos validados (5 reglas aplicadas)
#   - Columnas derivadas (pickup_date, pickup_hour, pickup_dayofweek, average_speed_mph)
#   - Enriquecimiento por zonas (pickup_borough, pickup_zone)

logger.info(f"Leyendo {TRIPS_TRUSTED_TABLE}...")

df_trips = spark.table(TRIPS_TRUSTED_TABLE)
total_trips = df_trips.count()

logger.info(f"Registros leídos: {total_trips:,}")
logger.info(f"Columnas disponibles: {len(df_trips.columns)}")

# Verificar que tenemos las columnas que vamos a usar para KPIs
columnas_requeridas = [
    "pickup_datetime", "pickup_hour", "pickup_dayofweek", "pickup_date",
    "pickup_borough", "pickup_zone", "pickup_location_id",
    "trip_distance", "trip_duration_minutes", "average_speed_mph",
    "fare_amount", "total_amount"
]

columnas_presentes = set(df_trips.columns)
columnas_faltantes = [c for c in columnas_requeridas if c not in columnas_presentes]

if columnas_faltantes:
    logger.error(f"✗ Faltan columnas: {columnas_faltantes}")
else:
    logger.info(f"✓ Todas las {len(columnas_requeridas)} columnas requeridas están presentes")

print("\nMuestra de las columnas que usaremos para KPIs:")
df_trips.select(
    "pickup_datetime",
    "pickup_hour",
    "pickup_dayofweek",
    "pickup_borough",
    "trip_distance",
    "trip_duration_minutes",
    "average_speed_mph",
    "fare_amount"
).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI 1: Patrón de demanda temporal
# MAGIC
# MAGIC **Métricas:** número de viajes, duración promedio, tarifa promedio.
# MAGIC
# MAGIC **Dimensiones:**
# MAGIC - **Franja horaria (6 categorías):**
# MAGIC   - `madrugada` (00:00-05:59) - demanda baja, viajes nocturnos
# MAGIC   - `mañana_temprano` (06:00-09:59) - pico de demanda laboral matutino
# MAGIC   - `mañana_media` (10:00-12:59) - demanda media
# MAGIC   - `tarde` (13:00-16:59) - demanda media-alta
# MAGIC   - `pico_tarde` (17:00-19:59) - pico de demanda laboral vespertino
# MAGIC   - `noche` (20:00-23:59) - demanda alta nocturna
# MAGIC
# MAGIC - **Día de la semana:** lunes, martes, ... domingo
# MAGIC
# MAGIC **Total combinaciones:** 6 franjas × 7 días = 42 filas en la tabla resultante.
# MAGIC
# MAGIC **Identificación de picos:** la tabla incluye una columna `es_pico` que marca las filas dentro del top 20% en número de viajes.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Crear franjas horarias y nombrar días

# COMMAND ----------

# ============================================================
# PREPARACIÓN DE DIMENSIONES PARA KPI 1
# ============================================================
# Creamos dos columnas categóricas:
#   1. franja_horaria: agrupación de pickup_hour en 6 categorías
#   2. dia_semana_nombre: pickup_dayofweek como nombre legible (Lunes, etc.)
#
# Usamos F.when().otherwise() que es equivalente a CASE WHEN en SQL.

logger.info("Creando dimensiones temporales para KPI 1...")

df_with_dimensions = (
    df_trips
    # Dimensión 1: franja horaria (6 categorías)
    .withColumn(
        "franja_horaria",
        F.when((F.col("pickup_hour") >= 0) & (F.col("pickup_hour") < 6), "1_madrugada")
         .when((F.col("pickup_hour") >= 6) & (F.col("pickup_hour") < 10), "2_mañana_temprano")
         .when((F.col("pickup_hour") >= 10) & (F.col("pickup_hour") < 13), "3_mañana_media")
         .when((F.col("pickup_hour") >= 13) & (F.col("pickup_hour") < 17), "4_tarde")
         .when((F.col("pickup_hour") >= 17) & (F.col("pickup_hour") < 20), "5_pico_tarde")
         .otherwise("6_noche")
    )
    # Dimensión 2: día de semana como nombre
    # Spark dayofweek: 1=Domingo, 2=Lunes, ..., 7=Sábado
    .withColumn(
        "dia_semana_nombre",
        F.when(F.col("pickup_dayofweek") == 1, "Domingo")
         .when(F.col("pickup_dayofweek") == 2, "Lunes")
         .when(F.col("pickup_dayofweek") == 3, "Martes")
         .when(F.col("pickup_dayofweek") == 4, "Miércoles")
         .when(F.col("pickup_dayofweek") == 5, "Jueves")
         .when(F.col("pickup_dayofweek") == 6, "Viernes")
         .otherwise("Sábado")
    )
)

# Verificación visual: distribución de franjas
print("Distribución de viajes por franja horaria:")
df_with_dimensions.groupBy("franja_horaria").count().orderBy("franja_horaria").show(truncate=False)

print("Distribución de viajes por día de la semana:")
df_with_dimensions.groupBy("dia_semana_nombre").count().orderBy(F.desc("count")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Celda 7 — Calcular el KPI 1

# COMMAND ----------

# ============================================================
# CÁLCULO DEL KPI 1: DEMANDA TEMPORAL
# ============================================================
# Agrupamos por (franja_horaria, dia_semana_nombre) y calculamos:
#   - num_viajes: cuántos viajes ocurrieron
#   - duracion_promedio_min: promedio de duración
#   - tarifa_promedio: promedio de fare_amount
#   - tarifa_total: suma de fare_amount (para entender volumen económico)
#
# Resultado: 42 filas (6 franjas × 7 días)

logger.info("Calculando KPI 1...")

df_kpi1 = (
    df_with_dimensions
    .groupBy("franja_horaria", "dia_semana_nombre")
    .agg(
        F.count("*").alias("num_viajes"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("duracion_promedio_min"),
        F.round(F.avg("fare_amount"), 2).alias("tarifa_promedio"),
        F.round(F.sum("fare_amount"), 2).alias("tarifa_total")
    )
)

# Identificar picos: top 20% de combinaciones por número de viajes
# Usamos un threshold dinámico calculado sobre los propios resultados.

# Primero calculamos el threshold: el percentil 80 del num_viajes
threshold = df_kpi1.approxQuantile("num_viajes", [0.80], 0.01)[0]
logger.info(f"Umbral para considerar 'pico': num_viajes >= {threshold:,.0f}")

df_kpi1_final = (
    df_kpi1
    .withColumn(
        "es_pico",
        F.when(F.col("num_viajes") >= threshold, True).otherwise(False)
    )
    .orderBy("franja_horaria", "dia_semana_nombre")
)

# Conteo total para validación
total_kpi1 = df_kpi1_final.count()
logger.info(f"✓ KPI 1 calculado: {total_kpi1} combinaciones (esperado: 42)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualizar resultados del KPI 1

# COMMAND ----------

# ============================================================
# VISUALIZACIÓN DE RESULTADOS DEL KPI 1
# ============================================================

print("=" * 80)
print("KPI 1 — TABLA COMPLETA (42 filas, ordenada por franja y día)")
print("=" * 80)
df_kpi1_final.show(42, truncate=False)

print("\n" + "=" * 80)
print("TOP 10 COMBINACIONES CON MÁS VIAJES")
print("=" * 80)
df_kpi1_final.orderBy(F.desc("num_viajes")).limit(10).show(truncate=False)

print("\n" + "=" * 80)
print("FRANJAS HORARIAS Y SU TOTAL DE VIAJES (agregado)")
print("=" * 80)
df_kpi1_final.groupBy("franja_horaria").agg(
    F.sum("num_viajes").alias("total_viajes"),
    F.round(F.avg("tarifa_promedio"), 2).alias("tarifa_promedio_franja")
).orderBy("franja_horaria").show(truncate=False)

print("\n" + "=" * 80)
print("PICOS IDENTIFICADOS")
print("=" * 80)
df_kpi1_final.filter(F.col("es_pico") == True).orderBy(F.desc("num_viajes")).show(truncate=False)

logger.info("✓ KPI 1 calculado y visualizado")

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## KPI 2: Eficiencia económica por zona
# MAGIC
# MAGIC **Métricas:**
# MAGIC - `ingreso_por_milla`: ingreso promedio por milla recorrida (fare_amount / trip_distance promediados)
# MAGIC - `velocidad_promedio_mph`: velocidad promedio en millas por hora
# MAGIC - `tarifa_promedio`: tarifa promedio del viaje
# MAGIC - `distancia_promedio_millas`: distancia promedio del viaje
# MAGIC - `num_viajes`: número de viajes desde la zona
# MAGIC
# MAGIC **Dimensiones:** `pickup_borough`, `pickup_zone`
# MAGIC
# MAGIC **Ranking:** top 10 zonas más rentables (por `ingreso_por_milla`).
# MAGIC
# MAGIC **Justificación de la métrica principal `ingreso_por_milla`:**
# MAGIC Una zona es "rentable" si genera más dinero por unidad de distancia. Zonas con
# MAGIC viajes cortos pero caros (ej. Manhattan céntrico, con tarifas mínimas elevadas)
# MAGIC serán más rentables que zonas con viajes largos pero económicos por milla.
# MAGIC
# MAGIC **Decisión técnica:** se excluyen zonas con menos de 100 viajes para evitar
# MAGIC distorsión estadística (una zona con 2 viajes puede tener métricas extremas no
# MAGIC representativas).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calcular KPI 2 con métricas por zona

# COMMAND ----------

# ============================================================
# CÁLCULO DEL KPI 2: EFICIENCIA ECONÓMICA POR ZONA
# ============================================================
# Agrupamos por (pickup_borough, pickup_zone) y calculamos métricas
# de rentabilidad y eficiencia.
#
# Excluimos zonas con menos de 100 viajes (filtro de calidad estadística).

logger.info("Calculando KPI 2: eficiencia económica por zona...")

UMBRAL_MIN_VIAJES = 100

df_kpi2_base = (
    df_trips
    .filter(F.col("pickup_borough").isNotNull())  # excluir zonas no identificadas
    .groupBy("pickup_borough", "pickup_zone")
    .agg(
        F.count("*").alias("num_viajes"),
        F.round(F.avg("trip_distance"), 2).alias("distancia_promedio_millas"),
        F.round(F.avg("fare_amount"), 2).alias("tarifa_promedio"),
        F.round(F.avg("average_speed_mph"), 2).alias("velocidad_promedio_mph"),
        F.round(F.sum("fare_amount"), 2).alias("ingreso_total"),
        # Métrica clave: ingreso por milla
        # Fórmula: avg(fare_amount / trip_distance)
        # Cuidado: trip_distance ya está validado > 0, así que no hay división por cero
        F.round(
            F.avg(F.col("fare_amount") / F.col("trip_distance")),
            2
        ).alias("ingreso_por_milla")
    )
    .filter(F.col("num_viajes") >= UMBRAL_MIN_VIAJES)
)

total_zonas = df_kpi2_base.count()
logger.info(f"✓ Zonas analizadas (con >= {UMBRAL_MIN_VIAJES} viajes): {total_zonas}")

print("\nMuestra de las primeras 5 zonas:")
df_kpi2_base.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Agregar ranking con window functions

# COMMAND ----------

# ============================================================
# RANKING DE ZONAS MÁS RENTABLES (Window Function)
# ============================================================
# Usamos una window function para calcular el ranking SIN agregar
# (manteniendo todas las filas, solo añadiendo una columna de rank).
#
# Window functions son CRÍTICAS en data engineering. Permiten:
#   - Rankings (ROW_NUMBER, RANK, DENSE_RANK)
#   - Running totals (SUM OVER PARTITION BY...)
#   - Comparaciones con fila anterior (LAG, LEAD)
#
# Aquí: ranking global por ingreso_por_milla descendente.

logger.info("Calculando ranking de rentabilidad...")

# Definir la ventana: ordenamos TODA la tabla por ingreso_por_milla DESC
ventana_ranking = Window.orderBy(F.desc("ingreso_por_milla"))

df_kpi2_final = (
    df_kpi2_base
    .withColumn("ranking_rentabilidad", F.row_number().over(ventana_ranking))
    .withColumn(
        "es_top_10",
        F.when(F.col("ranking_rentabilidad") <= 10, True).otherwise(False)
    )
    .orderBy("ranking_rentabilidad")
)

logger.info(f"✓ KPI 2 calculado: {df_kpi2_final.count()} zonas con ranking")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualizar KPI 2

# COMMAND ----------

# ============================================================
# VISUALIZACIÓN DEL KPI 2
# ============================================================

print("=" * 90)
print("KPI 2 — TOP 10 ZONAS MÁS RENTABLES (por ingreso_por_milla)")
print("=" * 90)
df_kpi2_final.filter(F.col("es_top_10") == True).show(truncate=False)

print("\n" + "=" * 90)
print("KPI 2 — TOP 10 ZONAS CON MAYOR INGRESO TOTAL (volumen)")
print("=" * 90)
df_kpi2_final.orderBy(F.desc("ingreso_total")).limit(10).show(truncate=False)

print("\n" + "=" * 90)
print("KPI 2 — RESUMEN POR BOROUGH (agregado)")
print("=" * 90)
df_kpi2_final.groupBy("pickup_borough").agg(
    F.count("*").alias("num_zonas_analizadas"),
    F.sum("num_viajes").alias("total_viajes"),
    F.sum("ingreso_total").alias("ingreso_total_borough"),
    F.round(F.avg("ingreso_por_milla"), 2).alias("ingreso_milla_promedio_borough"),
    F.round(F.avg("velocidad_promedio_mph"), 2).alias("velocidad_promedio_borough")
).orderBy(F.desc("ingreso_total_borough")).show(truncate=False)

logger.info("✓ KPI 2 calculado y visualizado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI 3: Impacto de calidad de datos en ingresos (opcional)
# MAGIC
# MAGIC **Objetivo:** Cuantificar el impacto económico de las reglas de validación.
# MAGIC Responde la pregunta: *"¿Cuánto dinero representan los registros que descartamos?"*
# MAGIC
# MAGIC **Importancia para negocio:**
# MAGIC Las reglas de calidad de datos no son neutras — descartan registros que en algunos casos podrían representar ingresos reales (errores de medición pero transacciones legítimas). Este KPI cuantifica el "costo de la calidad de datos".
# MAGIC
# MAGIC **Metodología:**
# MAGIC 1. Leer la tabla Raw completa (sin filtros aplicados).
# MAGIC 2. Re-aplicar cada regla individualmente y medir el `fare_amount` total de los registros descartados.
# MAGIC 3. Calcular el porcentaje de ingreso descartado vs. el ingreso total bruto.
# MAGIC
# MAGIC **Output:** tabla `refined.kpi_impacto_calidad_datos` con una fila por regla:
# MAGIC - `regla_id`, `descripcion`
# MAGIC - `registros_descartados`, `pct_registros_descartados`
# MAGIC - `ingreso_descartado`, `pct_ingreso_descartado`
# MAGIC - `ingreso_promedio_descartado_por_registro`
# MAGIC
# MAGIC **Insight clave esperado:** las reglas pueden descartar más % de registros que % de ingresos (si lo que se descarta son viajes pequeños), o al revés. Cualquiera de los dos casos es informativo.

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Calcular KPI 3

# COMMAND ----------

# ============================================================
# KPI 3: IMPACTO DE LAS REGLAS DE DQ EN INGRESOS
# ============================================================
# Leemos la tabla Raw (sin filtros) y aplicamos cada regla individualmente
# para medir cuánto ingreso descartaríamos por aplicar SOLO esa regla.

logger.info("Calculando KPI 3: impacto de DQ en ingresos...")

# Leer tabla Raw
df_raw = spark.table(f"{CATALOG_NAME}.raw.yellow_trips_raw")
total_registros_raw = df_raw.count()
ingreso_total_raw = df_raw.agg(F.sum("fare_amount")).collect()[0][0]

logger.info(f"Tabla Raw: {total_registros_raw:,} registros, ingreso total bruto: ${ingreso_total_raw:,.2f}")

# Calcular columna derivada para reglas R4 y R5 (necesitan duración)
df_raw_with_duration = df_raw.withColumn(
    "trip_duration_minutes",
    (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60.0
)

# Definir las 5 reglas y su lógica de FALLO (lo OPUESTO al filtro)
# La idea: para medir el impacto de R1, contamos los registros que FALLAN R1
reglas = [
    {
        "id": "R1",
        "descripcion": "pickup_datetime < dropoff_datetime",
        "filtro_fallo": F.col("tpep_pickup_datetime") >= F.col("tpep_dropoff_datetime")
    },
    {
        "id": "R2",
        "descripcion": "trip_distance > 0",
        "filtro_fallo": F.col("trip_distance") <= 0
    },
    {
        "id": "R3",
        "descripcion": "fare_amount > 0",
        "filtro_fallo": F.col("fare_amount") <= 0
    },
    {
        "id": "R4",
        "descripcion": "trip_duration_minutes <= 360",
        "filtro_fallo": F.col("trip_duration_minutes") > 360
    },
    {
        "id": "R5",
        "descripcion": "trip_duration_minutes >= 1",
        "filtro_fallo": F.col("trip_duration_minutes") < 1
    }
]

# Calcular impacto por regla
filas_kpi3 = []
for regla in reglas:
    df_fallos = df_raw_with_duration.filter(regla["filtro_fallo"])
    
    registros_fallo = df_fallos.count()
    
    # Sum puede devolver None si no hay registros, manejamos con coalesce
    ingreso_fallo_result = df_fallos.agg(F.sum("fare_amount")).collect()[0][0]
    ingreso_fallo = ingreso_fallo_result if ingreso_fallo_result is not None else 0.0
    
    ingreso_promedio = ingreso_fallo / registros_fallo if registros_fallo > 0 else 0.0
    
    filas_kpi3.append({
        "regla_id": regla["id"],
        "descripcion": regla["descripcion"],
        "registros_descartados": registros_fallo,
        "pct_registros_descartados": round(registros_fallo / total_registros_raw * 100, 4),
        "ingreso_descartado": round(ingreso_fallo, 2),
        "pct_ingreso_descartado": round(ingreso_fallo / ingreso_total_raw * 100, 4),
        "ingreso_promedio_descartado_por_registro": round(ingreso_promedio, 2)
    })
    
    logger.info(f"  {regla['id']}: {registros_fallo:,} registros, ${ingreso_fallo:,.2f} de ingresos")

# Crear DataFrame con los resultados
df_kpi3 = spark.createDataFrame(filas_kpi3)

logger.info(f"✓ KPI 3 calculado: {df_kpi3.count()} reglas analizadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualizar KPI 3

# COMMAND ----------

# ============================================================
# VISUALIZACIÓN DEL KPI 3
# ============================================================

print("=" * 100)
print("KPI 3 — IMPACTO DE LAS REGLAS DE DQ EN INGRESOS")
print("=" * 100)

df_kpi3.orderBy(F.desc("ingreso_descartado")).show(truncate=False)

# Totales agregados
total_registros_descartados = df_kpi3.agg(F.sum("registros_descartados")).collect()[0][0]
total_ingreso_descartado = df_kpi3.agg(F.sum("ingreso_descartado")).collect()[0][0]

print(f"\nIngreso total bruto (sin filtros):       ${ingreso_total_raw:,.2f}")
print(f"Suma de ingreso descartado por reglas:    ${total_ingreso_descartado:,.2f}")
print(f"Pct de ingreso 'perdido' por DQ:          {total_ingreso_descartado/ingreso_total_raw*100:.4f}%")

# Nota: la suma de descartes por regla puede ser MAYOR que el total de descartes
# porque las reglas no son mutuamente excluyentes (un registro puede fallar varias).

logger.info("✓ KPI 3 calculado y visualizado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escritura de los KPIs como tablas Delta en `refined`
# MAGIC
# MAGIC Los 3 DataFrames calculados se escriben como tablas Delta gestionadas por Unity Catalog:
# MAGIC - `df_kpi1_final` → `refined.kpi_demanda_temporal`
# MAGIC - `df_kpi2_final` → `refined.kpi_eficiencia_economica`
# MAGIC - `df_kpi3` → `refined.kpi_impacto_calidad_datos`
# MAGIC
# MAGIC **Decisiones de escritura:**
# MAGIC - `mode("overwrite")` para idempotencia
# MAGIC - Sin particionado (las tablas son pequeñas: 42, 145 y 5 filas respectivamente)
# MAGIC - `overwriteSchema=true` para permitir evolución durante desarrollo

# COMMAND ----------

# MAGIC %md
# MAGIC ### Escribir y validar las 3 tablas

# COMMAND ----------

# ============================================================
# ESCRITURA DE LOS 3 KPIs COMO TABLAS DELTA EN REFINED
# ============================================================

logger.info("Escribiendo los 3 KPIs como tablas Delta en refined...")

# Lista de tuplas (dataframe, nombre_tabla, descripcion) para iterar
kpis_a_escribir = [
    (df_kpi1_final, KPI1_TABLE, "KPI 1: Patrón de demanda temporal (franja × día)"),
    (df_kpi2_final, KPI2_TABLE, "KPI 2: Eficiencia económica por zona (ranking top 10)"),
    (df_kpi3, KPI3_TABLE, "KPI 3: Impacto de DQ en ingresos (5 reglas)")
]

resultados_escritura = []

for df, tabla, descripcion in kpis_a_escribir:
    registros_pre = df.count()
    
    logger.info(f"Escribiendo {tabla}...")
    
    (
        df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(tabla)
    )
    
    # Validar conteo post-escritura
    registros_post = spark.table(tabla).count()
    
    if registros_pre == registros_post:
        logger.info(f"  ✓ {tabla}: {registros_post} registros escritos")
        status = "OK"
    else:
        logger.error(f"  ✗ MISMATCH en {tabla}: pre={registros_pre}, post={registros_post}")
        status = "MISMATCH"
    
    resultados_escritura.append({
        "tabla": tabla.split(".")[-1],  # solo el nombre, sin catalog.schema
        "descripcion": descripcion,
        "registros": registros_post,
        "status": status
    })

# Resumen consolidado
print("=" * 90)
print("RESUMEN DE TABLAS ESCRITAS EN refined")
print("=" * 90)
for r in resultados_escritura:
    print(f"\n  {r['tabla']}")
    print(f"    Descripción: {r['descripcion']}")
    print(f"    Registros: {r['registros']}")
    print(f"    Status: {r['status']}")

# Listar todas las tablas en refined para verificación visual
print("\n" + "=" * 90)
print(f"TABLAS EN {CATALOG_NAME}.refined:")
print("=" * 90)
spark.sql(f"SHOW TABLES IN {CATALOG_NAME}.refined").show(truncate=False)

logger.info("✓ Las 3 tablas de KPIs escritas exitosamente en refined")

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Validación end-to-end y resumen final

# COMMAND ----------

# ============================================================
# VALIDACIÓN END-TO-END Y RESUMEN FINAL
# ============================================================
# Esta celda valida la consistencia de toda la cadena Medallion
# y produce un resumen ejecutivo del Notebook 03.

print("=" * 90)
print("RESUMEN EJECUTIVO — NOTEBOOK 03 (REFINED / KPIs)")
print("=" * 90)

# ────────────────────────────────────────────────────────────────
# Sección 1: Trazabilidad Raw → Trusted → Refined
# ────────────────────────────────────────────────────────────────
print("\n📊 TRAZABILIDAD DEL PIPELINE")
print("-" * 90)
raw_count = spark.table(f"{CATALOG_NAME}.raw.yellow_trips_raw").count()
trusted_count = spark.table(f"{CATALOG_NAME}.trusted.yellow_trips_trusted").count()

print(f"Raw (yellow_trips_raw):        {raw_count:>10,} registros")
print(f"Trusted (yellow_trips_trusted): {trusted_count:>10,} registros (-{raw_count-trusted_count:,}, {(raw_count-trusted_count)/raw_count*100:.2f}% descartados)")

# ────────────────────────────────────────────────────────────────
# Sección 2: Tablas Refined creadas
# ────────────────────────────────────────────────────────────────
print("\n📈 TABLAS REFINED CREADAS")
print("-" * 90)
kpi1_count = spark.table(KPI1_TABLE).count()
kpi2_count = spark.table(KPI2_TABLE).count()
kpi3_count = spark.table(KPI3_TABLE).count()
print(f"KPI 1 (kpi_demanda_temporal):       {kpi1_count:>4} filas (6 franjas × 7 días)")
print(f"KPI 2 (kpi_eficiencia_economica):   {kpi2_count:>4} filas (zonas con >= 100 viajes)")
print(f"KPI 3 (kpi_impacto_calidad_datos):  {kpi3_count:>4} filas (5 reglas de validación)")

# ────────────────────────────────────────────────────────────────
# Sección 3: Insights de negocio destacados
# ────────────────────────────────────────────────────────────────
print("\n🎯 INSIGHTS DE NEGOCIO CLAVE")
print("-" * 90)

# Pico de demanda
top_pico = spark.table(KPI1_TABLE).orderBy(F.desc("num_viajes")).limit(1).collect()[0]
print(f"\nPico de demanda:")
print(f"  Franja: {top_pico['franja_horaria']}  |  Día: {top_pico['dia_semana_nombre']}")
print(f"  Viajes: {top_pico['num_viajes']:,}  |  Tarifa total: ${top_pico['tarifa_total']:,.2f}")

# Zona más rentable (excluyendo Outside of NYC por ser caso especial)
top_zona = (
    spark.table(KPI2_TABLE)
    .filter(F.col("pickup_borough") != "N/A")
    .orderBy(F.desc("ingreso_por_milla"))
    .limit(1)
    .collect()[0]
)
print(f"\nZona más rentable intra-NYC:")
print(f"  {top_zona['pickup_borough']} / {top_zona['pickup_zone']}")
print(f"  Ingreso por milla: ${top_zona['ingreso_por_milla']}  |  Viajes: {top_zona['num_viajes']:,}")

# Zona con mayor volumen económico
top_volumen = spark.table(KPI2_TABLE).orderBy(F.desc("ingreso_total")).limit(1).collect()[0]
print(f"\nZona con mayor ingreso total:")
print(f"  {top_volumen['pickup_borough']} / {top_volumen['pickup_zone']}")
print(f"  Ingreso total: ${top_volumen['ingreso_total']:,.2f}  |  Viajes: {top_volumen['num_viajes']:,}")

# Regla DQ con mayor impacto
top_regla = spark.table(KPI3_TABLE).orderBy(F.desc("ingreso_descartado")).limit(1).collect()[0]
print(f"\nRegla DQ con mayor impacto positivo:")
print(f"  {top_regla['regla_id']}: {top_regla['descripcion']}")
print(f"  Registros: {top_regla['registros_descartados']:,}  |  Ingreso: ${top_regla['ingreso_descartado']:,.2f}")

# ────────────────────────────────────────────────────────────────
# Sección 4: Estado del catálogo
# ────────────────────────────────────────────────────────────────
print("\n📚 ESTADO FINAL DE UNITY CATALOG")
print("-" * 90)
print(f"\nTablas en {CATALOG_NAME}:")
for schema in ["raw", "trusted", "refined"]:
    print(f"\n  Schema {schema}:")
    spark.sql(f"SHOW TABLES IN {CATALOG_NAME}.{schema}").show(truncate=False)

print("\n" + "=" * 90)
print("✓ NOTEBOOK 03 COMPLETADO EXITOSAMENTE")
print("=" * 90)

logger.info("✓ Capa Refined completada exitosamente")
logger.info("Listo para Notebook 04 (Data Quality Report + Pipeline Orquestador)")

# COMMAND ----------

