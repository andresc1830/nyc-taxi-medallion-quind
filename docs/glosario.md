# Glosario de términos de negocio

Este glosario define los **5 términos clave** del proyecto NYC Yellow Taxi Trips. Su objetivo es alinear el vocabulario entre el equipo de datos, los analistas de negocio y los stakeholders. Cada término incluye su definición operativa, contexto en el pipeline, y ejemplo concreto.

---

## 1. Viaje válido

**Definición:** Registro de viaje en taxi que cumple las cinco reglas de validación de calidad de datos definidas en el pipeline:

1. **R1** — La hora de pickup es estrictamente anterior a la hora de dropoff
2. **R2** — La distancia recorrida es mayor a cero millas
3. **R3** — La tarifa cobrada es mayor a cero dólares
4. **R4** — La duración del viaje es menor o igual a 360 minutos (6 horas)
5. **R5** — La duración del viaje es mayor o igual a 1 minuto

**Dónde vive:** todos los registros válidos quedan en la tabla `nyc_taxi_andres_rojas.trusted.yellow_trips_trusted`. Los inválidos se descartan y se registran en `refined.data_quality_report`.

**Por qué importa:** los análisis de negocio (KPIs) solo pueden ser confiables si operan sobre viajes válidos. Un dashboard que mezcle viajes válidos con cancelaciones, viajes fantasma o reembolsos genera métricas distorsionadas.

**Ejemplo real del pipeline:**
- Total registros raw: 3,066,766
- Viajes válidos (Trusted): 2,987,000
- Descartados: 79,766 (2.60%)

---

## 2. Hora pico (peak hour)

**Definición:** Combinación de **franja horaria** y **día de la semana** cuya cantidad de viajes está en el **top 20% (percentil 80 o superior)** del total de combinaciones analizadas.

**Cálculo en el pipeline:**
- Se generan 42 combinaciones únicas (6 franjas horarias × 7 días)
- Se ordenan por `num_viajes` descendente
- Las 8 con más viajes (top 20% de 42) se marcan con `es_pico = true`

**Franjas horarias usadas:**

| Franja | Rango | Categoría |
|---|---|---|
| `1_madrugada` | 00:00-05:59 | Demanda baja nocturna |
| `2_mañana_temprano` | 06:00-09:59 | Pico matutino |
| `3_mañana_media` | 10:00-12:59 | Demanda media |
| `4_tarde` | 13:00-16:59 | Pico distribuido |
| `5_pico_tarde` | 17:00-19:59 | Rush hour vespertino |
| `6_noche` | 20:00-23:59 | Alta demanda nocturna |

**Por qué importa:** identificar horas pico permite optimizar la asignación de flota, ajustar precios dinámicos, planificar turnos de conductores y dimensionar capacidad operativa.

**Ejemplo real del pipeline:**
- Pico de demanda absoluto: `4_tarde` / `Martes` con 119,262 viajes
- Umbral para considerar "pico": ≥95,432 viajes

---

## 3. Borough

**Definición:** División administrativa de primer nivel en la ciudad de Nueva York. NYC está dividida en **cinco boroughs**: Manhattan, Brooklyn, Queens, The Bronx y Staten Island. Cada borough contiene a su vez múltiples **zonas** (zones) más granulares.

**Dónde vive:**
- Tabla origen: `raw.taxi_zones_raw` (columna `Borough`)
- Tabla enriquecida: `trusted.yellow_trips_trusted` (columna `pickup_borough`, vía join con zones)
- Tablas agregadas: `refined.kpi_eficiencia_economica`

**Valores posibles en este dataset:**

| Borough | Significado |
|---|---|
| `Manhattan` | Distrito central, máxima densidad de taxis |
| `Brooklyn` | Borough residencial al sur |
| `Queens` | Incluye aeropuertos JFK y LaGuardia |
| `Bronx` | Borough norte, menor actividad de taxis amarillos |
| `Staten Island` | Borough sur, prácticamente sin servicio de taxi amarillo |
| `EWR` | Newark International Airport (técnicamente en New Jersey) |
| `Unknown` | Zona con borough no clasificado en el lookup oficial |
| `N/A` | Viajes con origen "Outside of NYC" |

**Por qué importa:** el borough es la dimensión geográfica principal para análisis estratégico. Determina cobertura del servicio, regulación tarifaria diferenciada (especialmente aeropuertos) y oportunidades de expansión operativa.

**Ejemplo real del pipeline:**
- Manhattan domina con 89% de los viajes y $39.6M de ingreso total
- Queens es 2do en ingreso ($14.1M) gracias a aeropuertos JFK y LaGuardia
- Brooklyn y Bronx tienen actividad marginal (<1% del ingreso total combinados)

---

## 4. Ingreso por milla (income per mile)

**Definición:** Métrica de eficiencia económica que mide el ingreso promedio generado por cada milla recorrida en una zona. Se calcula como el promedio del cociente `fare_amount / trip_distance` sobre todos los viajes originados en esa zona.

**Fórmula:**
```
ingreso_por_milla = AVG(fare_amount / trip_distance)
```

**Por qué se usa el promedio del cociente y no el cociente de los totales:**
- `AVG(fare_amount / trip_distance)` da peso igual a cada viaje
- `SUM(fare_amount) / SUM(trip_distance)` da más peso a viajes largos
- Para análisis de "rentabilidad de la zona", el promedio del cociente refleja mejor el comportamiento típico

**Dónde vive:** columna `ingreso_por_milla` en `refined.kpi_eficiencia_economica`.

**Por qué importa:** permite comparar la rentabilidad económica de zonas con perfiles muy distintos. Una zona con viajes cortos pero caros (Manhattan céntrico) puede ser más rentable por milla que una zona con viajes largos pero económicos (aeropuertos con tarifa fija). Esta métrica es clave para decisiones de asignación de flota basadas en rentabilidad unitaria, no en volumen.

**Ejemplo real del pipeline:**

| Zona | Borough | Ingreso/milla | Análisis |
|---|---|---|---|
| Jackson Heights | Queens | $28.11 | Viajes cortos y caros — alta rentabilidad |
| JFK Airport | Queens | $5.01 | Viajes largos con tarifa fija — baja rentabilidad por milla pero alto volumen económico |
| Outside of NYC | N/A | $41.64 | Caso especial — viajes desde fuera de NYC |

**Contraste con `ingreso_total`:** una zona puede tener bajo ingreso por milla pero alto ingreso total (aeropuertos) o viceversa (zonas residenciales con pocos viajes premium). Ambas métricas son complementarias.

---

## 5. Pipeline run

**Definición:** Una ejecución completa del pipeline ETL, desde la lectura de fuentes hasta la generación del reporte de ejecución. Cada pipeline run se identifica con un **UUID único** (`pipeline_run_id`) que permite trazabilidad, debugging y auditoría.

**Cómo se genera:** al inicio del Notebook 04, mediante `uuid.uuid4()`.

**Dónde se registra:**
- Columna `pipeline_run_id` en `refined.data_quality_report` (una fila por regla DQ, todas con el mismo run_id)
- Campo `execution_metadata.execution_id` en `reports/execution_report.json`
- Logs estructurados del pipeline

**Por qué importa:** en producción se ejecutan múltiples runs por día (uno por archivo nuevo de NYC TLC, por ejemplo). El `pipeline_run_id` permite:

- **Trazabilidad:** *"¿cuándo y bajo qué condiciones se generó este reporte?"*
- **Debugging:** localizar el run específico donde se introdujo un dato anómalo
- **Auditoría:** demostrar a auditores externos qué versiones del pipeline produjeron qué reportes
- **Rollback:** identificar y deshacer ejecuciones erróneas

**Ejemplo real del pipeline:**
- Pipeline run ID actual: `37538f36-50aa-4e56-84cd-b1d4ed7ea635`
- Fecha de ejecución: 2026-05-11T10:54:47Z (UTC)
- Notebook orquestador: `04_data_quality_report`

### Evolución a producción

En un escenario productivo, cada `pipeline_run_id` quedaría asociado a:
- Versión del código del notebook (commit hash de Git)
- Versión del schema de las tablas Delta
- Configuración del cluster usada
- Volumen de datos procesados (input/output bytes)

Esta combinación de metadata es la base de un sistema de **observabilidad de pipelines** maduro.