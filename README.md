# ETL Saber 11 — Valle del Cauca antes, durante y después de la pandemia (Entrega 2)

Equipo: Valeria Gómez · Lauren Marin · Carlos Camacho — Curso ETL (G51), UAO

> Borrador. Cada integrante completa su sección (ver guía del equipo, sección 6).

## 1. Objetivo refinado
¿Cómo cambió el rendimiento en Saber 11 y su relación con las condiciones socioeconómicas del hogar (estrato, educación de los padres, internet y computador, NSE) entre 2019 (antes), 2020 (durante) y 2025 (después de la pandemia) en el Valle del Cauca?

## 2. Arquitectura
_(Valeria: insertar `docs/arquitectura.png`)_

Tres ramas en paralelo producen archivos con la misma estructura (ver sección 4.1), que se apilan, se validan con Great Expectations y se cargan en un modelo estrella en MySQL.

| Rama | Fuente | Responsable |
|---|---|---|
| CSV | Resultados Saber 11 2019 | Carlos |
| API | datos.gov.co, Saber 11 2020 | Lauren |
| DB | Base de la entrega 1 (2025) | Valeria |

## 3. Pasos del pipeline y diseño del DAG (Valeria)
DAG `saber11_valle_etl` (`dags/saber11_etl_dag.py`):

```
extract_api -> transform_api ┐
read_csv    -> transform_csv ├-> merge -> quality_check -> load -> cleanup_staging -> dashboard_update
read_db     -> transform_db  ┘
```

| Tarea | Entrada → salida | Reintentos |
|---|---|---|
| extract_api | API → raw_api.parquet | 3 (5 min, progresivo) |
| transform_api | raw_api → staging_api | 0 |
| read_csv | CSV 2019 → raw_csv.parquet | 1 |
| transform_csv | raw_csv → staging_csv | 0 |
| read_db | MySQL raw_saber11 → raw_db.parquet | 2 (30 s) |
| transform_db | raw_db → staging_db | 0 |
| merge | 3 staging → staging_merged | 0 |
| quality_check | staging_merged → aprueba o falla | 0 |
| load | staging_merged → DW | 0 |
| cleanup_staging | borra temporales (solo si load terminó bien) | 0 |
| dashboard_update | DW → tableros | 0 |

Parámetros: `@daily` técnico (los datos son históricos: la ejecución diaria cumple la consigna de extracción programada, pero en la práctica no cambia nada), `catchup=False`, `max_active_runs=1`, reintento global 0. Los datos pasan entre tareas por archivos parquet en `data/staging`, nunca por XCom.

## 4. Decisiones de transformación (Valeria)

### 4.1 Estructura común del dataset unificado
Las tres fuentes se llevan a las mismas 25 columnas, en este orden y con estos tipos, antes de apilarlas. Esta estructura está definida en `etl/esquema.py` y `validar_esquema()` la verifica en cada rama y en el `merge`.

| Grupo | Columna | Tipo | Regla |
|---|---|---|---|
| Identificación | estu_consecutivo | texto | nunca nulo, sin duplicados |
| | periodo | texto | formato AAAAS (ej. 20191), nunca nulo |
| | anio | entero | primeros 4 dígitos de periodo |
| | semestre | entero | último dígito de periodo |
| Ubicación | estu_depto_reside, estu_mcpio_reside | texto | residencia del estudiante |
| Colegio | cole_codigo_icfes | entero | -1 = inscripción individual |
| | cole_nombre_establecimiento, cole_naturaleza, cole_calendario, cole_area_ubicacion | texto | |
| Socioeconómicas | fami_estratovivienda, fami_educacionpadre, fami_educacionmadre, fami_tieneinternet, fami_tienecomputador, fami_personashogar | texto | |
| | estu_nse_individual | entero | 1 a 4; -1 si falta |
| | estu_inse_individual | decimal | nulo si falta |
| Resultados | punt_global | entero | 0 a 500, puede ser nulo |
| | punt_matematicas, punt_lectura_critica, punt_c_naturales, punt_sociales_ciudadanas, punt_ingles | entero | 0 a 100, pueden ser nulos |

Filtros: solo Valle del Cauca (comparando `UPPER(TRIM(estu_depto_reside))`) y solo los periodos del año de cada fuente, aplicados al leer. Si una fuente no trae una columna (por ejemplo NSE/INSE en 2019 y 2020), se crea en nulo; no se inventan valores.

### 4.2 Limpieza compartida (`etl/limpieza.py`)
Es la limpieza de la entrega 1 convertida en una sola función, `limpiar()`, que las tres ramas usan como último paso:
- Texto: repara caracteres corruptos con ftfy, quita espacios y escribe los faltantes como `No informa`. Valores como `nan`, `None` o `NULL` escritos como texto se tratan como faltantes.
- Estudiantes sin colegio: código `-1`, nombre `Sin colegio (inscripción individual)` y `No aplica` en naturaleza, calendario y área.
- NSE faltante = `-1`; INSE faltante = nulo.
- Deduplicación por `estu_consecutivo`, priorizando el registro con colegio sobre la inscripción individual.
- Los renombres `punt_c_naturales → punt_naturales` y `cole_codigo_icfes → cole_cod_icfes` se hacen solo al cargar al DW.

### 4.3 Diferencias con la entrega 1
- Un mismo estudiante en dos colegios distintos, un periodo que no sea AAAAS o un puntaje con decimales detienen el proceso con un error claro (antes solo era un aviso o pasaban sin revisar).
- Las filas que no encuentran su llave en una dimensión también detienen la carga (antes se descartaban).
- DW nuevo (`saber11_dw_e2`) separado del nacional de la entrega 1, con `COLLATE utf8mb4_bin` para que "Si"/"Sí" o "VALLE"/"Valle" no se mezclen al cargar dimensiones, y `dim_tiempo.etapa_pandemia` (Antes / Durante / Después).

## 5. API

La rama API trae Saber 11 de 2020 desde datos.gov.co, la plataforma de datos abiertos del Estado. Esta plataforma publica cada conjunto en una dirección con forma **`https://www.datos.gov.co/resource/<id>.json`**, y permite pedir solo las filas que se necesitan en lugar de bajar el archivo completo.

El año 2020 está partido en dos publicaciones, una por periodo, y ninguna cubre sola el año:

| Conjunto | Periodo en la API | Filas del país | Filas de Valle |
|---|---|---|---|
| **a8xr-en99** | **20201** | 15.435 | 6.301 |
| **rnvb-vnyh** | **20204** | 504.872 | 35.442 |

Se usan las dos y se unen en **raw_api.parquet**

**Las consultas**
- El filtro de Valle **upper(estu_depto_reside) = 'VALLE'**;  se hace dentro de la consulta, para no descargar más de 500.000 filas del país.
- Se ordena por **estu_consecutivo** y se pagina de 10.000 en 10.000. Sin un orden fijo, las páginas se pueden repetir o saltar filas.
- Se usa la versión **`/resource/<id>.json`** porque funciona sin credenciales; la versión **/api/v3/views/...** pide autenticación.
- El token de aplicación **SOCRATA_APP_TOKEN** es opcional: sin token funciona, solo que mas limitado; si se define, se envía en la cabecera **X-App-Token**.
- Ante errores temporales o límites de uso la descarga se reintenta sola y adicional, la tarea **extract_api** del DAG tiene 3 reintentos con espera progresiva.

**Diferencias entre Datos del ICFES**
- **a8xr-en99** no publica **estu_nse_individual** ni **estu_inse_individual**; se crean en nulo (NSE queda en **-1**).
- La API omite los campos nulos en el JSON, y por eso **raw_api.parquet** tiene columnas que algunos registros no traen. Solo se conservan las 23 columnas del esquema.

**Decisión sobre periodo en Datos del 20202**
El conjunto 2020-2 publica el periodo como **20204**; la estructura de las tres ramas solo admite semestres 1 o 2 y ninguna de las dos publicaciones trae un **20202**, así que la corrección que tomamos no genera ninguna alteracion con ningún dato. Nosotros decidimos tratar el **20204** como el segundo semestre de 2020 y reasignarlo a **20202**, la fuente no explica qué significa el 4, por ellos nuestra decision; el cambio se hace en **transform_api**, antes de la limpieza compartida, y **raw_api.parquet** conserva el valor original. La ficha de la fuente está en **docs/ficha_fuente_api.md**.

## 6. Great Expectations

La tarea **quality_check** revisa **staging_merged.parquet** con Great Expectations usamos la versión 1.24.0, que es compatible con la imagen de Airflow del proyecto; aqui se define un grupo de reglas y un Checkpoint que la ejecuta; si alguna regla falla, la tarea termina con error, **load** no corre y los archivos de staging se conservan para revisarlos, tener en cuenta que el resultado de cada ejecución queda en **resultado_gx.json**, dentro de la carpeta de staging.

| Que revisamos | Regla | Por que |
|---|---|---|
| Estructura | Existen las 25 columnas, en el orden del esquema | El merge y la carga dependen de la estructura comun |
| Llaves | **estu_consecutivo** y **periodo** sin nulos; **estu_consecutivo** único | Es la llave de cada estudiante en la tabla de hechos |
| Tiempo | **periodo** con formato AAAAS, año 2019, 2020 o 2025 y semestre 1 o 2 | Cada fuente aporta un año; un periodo raro indica un error de la rama |
| Cobertura | Los tres años presentes, con un mínimo de filas por año | Detecta una carga cortada o una fuente que llegó vacía |
| Ubicación | **estu_depto_reside** empieza por VALLE | solo estamos evaluando Valle del Cauca |
| Puntajes | **punt_global** entre 0 y 500; las otras cinco pruebas entre 0 y 100 | Fuera de ese rango el dato es imposible. Los nulos no cuentan |
| Socioeconómicas | NSE entre -1 y 4; internet y computador solo **Si**, **No** o **No informa** | Mantiene las categorias cerradas que usan las dimensiones |
| Colegio | **cole_codigo_icfes** mayor o igual a -1 | -1 es el código de inscripción individual |

**Mínimos de filas por año** 
**MIN_FILAS_POR_ANIO** en **quality_check.py** en 2019: _DATOS DE CARLOS_ · 2020: 40.000 · 2025: 45.000, fijamos cerca del 90 % del conteo real de cada fuente, para detectar cargas incompletas sin que un descarte normal de duplicados dispare la regla.


## 7. EDA y visualizaciones (Carlos)
_pendiente_

## 8. Modelo de datos
Estrella: `fact_resultadossaber11` + `dim_colegio`, `dim_ubicacion`, `dim_socioeconomica`, `dim_tiempo` (`sql/esquema_dw.sql`).

## 9. Cómo correrlo desde cero
1. Instalar Docker Desktop y Git.
2. `git clone <url>` y `cd` a la carpeta.
3. `cp .env.example .env` (Windows: `copy .env.example .env`) y cambiar las contraseñas.
4. Poner el CSV 2019 en `data/input/` (nombre en `CSV_2019_PATH`).
5. `docker compose up -d --build` y esperar a que `airflow-init` termine.
6. Cargar la tabla raw de la entrega 1 en el MySQL del proyecto (ver `docs/fuente_db_carga_raw.md`).
7. Abrir http://localhost:8080, activar `saber11_valle_etl` y ejecutarlo.

## 10. Estructura
```
dags/        DAG (solo tareas y dependencias)
etl/         esquema, limpieza compartida, ramas (api, csv, db), merge, calidad, carga
sql/         esquema del DW y script de arranque de MySQL
docs/        fichas de fuentes, diagramas, muestra de referencia
notebooks/   EDA
dashboards/  tableros
tests/       pruebas de la limpieza compartida
```
