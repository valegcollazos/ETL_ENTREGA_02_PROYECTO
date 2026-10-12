# ETL Saber 11 — Valle del Cauca antes, durante y después de la pandemia (Entrega 2)

## 1. Objetivo refinado
¿Cómo cambió el rendimiento en Saber 11 y su relación con las condiciones socioeconómicas del hogar (estrato, educación de los padres, internet y computador, NSE) entre 2019 (antes), 2020 (durante) y 2025 (después de la pandemia) en el Valle del Cauca?

**Propósito del pipeline.** Automatizar de forma reproducible la extracción, transformación, validación y carga de los resultados de Saber 11 en el Valle del Cauca para tres momentos (2019, 2020 y 2025), de modo que las tres fuentes queden unificadas en un solo modelo de datos y las comparaciones entre periodos se hagan siempre sobre datos validados.

**Insights que debe entregar.**
- Cómo cambió el puntaje global y el de cada prueba entre antes (2019), durante (2020) y después (2025) de la pandemia.
- Cómo se relaciona el rendimiento con el estrato, la educación de los padres, el acceso a internet y computador, y el nivel socioeconómico (NSE) del estudiante.
- Si esas diferencias socioeconómicas se mantuvieron, aumentaron o disminuyeron después de la pandemia.

## 2. Arquitectura

![Diagrama de arquitectura](docs/arquitectura.png)

*Figura 1. Arquitectura del pipeline: tres fuentes (CSV 2019, API 2020 y base SQL 2025), un DAG de Airflow en Docker, validación con Great Expectations antes de cargar y un DW en estrella en MySQL. Versión vectorial en `docs/arquitectura.svg`.*

Tres ramas en paralelo producen archivos con la misma estructura (ver sección 4.1), que se apilan, se validan con Great Expectations y se cargan en un modelo estrella en MySQL.

| Rama | Fuente | Año |
|---|---|---|
| CSV | Resultados Saber 11 | 2019 |
| API | datos.gov.co, Saber 11 | 2020 |
| DB | Base de la entrega 1 | 2025 |

## 3. Pasos del pipeline y diseño del DAG.
DAG `saber11_valle_etl` (`dags/saber11_etl_dag.py`):

```
extract_api -> transform_api ┐
read_csv    -> transform_csv ├-> merge -> quality_check -> load -> cleanup_staging -> dashboard_update
read_db     -> transform_db  ┘
```
![DAG saber11_valle_etl en Airflow](docs/dag_graph.png)

*Figura 2. Vista Graph del DAG en Airflow: tres ramas en paralelo que convergen en `merge`.*

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

## 4. Decisiones de transformación.

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

### 4.4 Resultados de la rama DB (2025)
Ficha completa en `docs/ficha_fuente_db.md`.

| Etapa | Resultado |
|---|---|
| Tabla `raw_saber11` (nacional, entrega 1) | 661.891 filas |
| Tras filtro Valle + 2025 (`read_db`) | 50.875 filas (20251: 11.947 · 20252: 38.928) |
| `staging_db.parquet` (`transform_db`) | 50.875 filas, 25 columnas |
| Fact en el DW tras `load` | 50.875 filas |
| Dimensiones | `dim_colegio` 1.267 · `dim_ubicacion` 42 · `dim_socioeconomica` 8.699 · `dim_tiempo` 2 |

Cifras con solo la rama DB; se actualizan cuando corran las tres fuentes.

Hallazgos: 7.856 estudiantes sin colegio (inscripción individual), 0 filas con llaves nulas, 0 duplicados y 0 dobles inscripciones; 376 colegios con atributos distintos entre registros (aviso informativo de la limpieza).

Pruebas realizadas:
- `pytest`: 9 pruebas de la limpieza, todas pasan.
- `read_db` y `transform_db` corren en local y dentro de Airflow (`airflow tasks test`).
- Repetibilidad: cada ejecución sobreescribe el staging (50.875 filas).
- Idempotencia de la carga: cargar dos veces el mismo staging deja 50.875 filas en la fact.
- Codificación: `etapa_pandemia` se guarda en UTF-8 ("Después").

### 4.5 Resultados de la rama CSV (2019)
Ficha completa en `docs/ficha_fuente_csv.md`. Fuente: `2019.csv`, datos abiertos ICFES Saber 11 2019, nacional, 459,6 MB, 94 columnas.

| Etapa | Resultado |
|---|---|
| CSV completo (nacional) | 680.893 filas (20191: 66.104 · 20192: 614.789) |
| Tras filtro Valle + 2019 (`read_csv`) | 57.341 filas (20191: 11.624 · 20192: 45.717) |
| `staging_csv.parquet` (`transform_csv`) | 57.341 filas, 25 columnas |

Transformaciones, en el orden en que se aplican:

**`read_csv`** (CSV → `raw_csv.parquet`)
1. **Lectura por bloques.** El archivo se lee en bloques de 100.000 filas para no cargar todo el país en memoria (máximo 216 MB, unos 8 s).
2. **Encoding.** El CSV no viene en UTF-8 sino en latin-1 (ISO-8859-1). `read_csv` intenta UTF-8, como en la entrega 1, y al fallar lo lee en latin-1. Se verificó que las 66.104 filas de 20191 coinciden, en las 23 columnas del esquema, con una versión UTF-8 del mismo periodo.
3. **Tipos y nulos al leer.** Todo se lee como texto, con los mismos valores nulos de la entrega 1 (`""`, `NA`, `N/A`, `null`, `NULL`). Los tipos los pone después la limpieza compartida.
4. **Selección de columnas.** De las 94 columnas solo se leen las 23 de entrada del esquema; las otras 71 no se usan.
   - Entre las que no se usan están los códigos DANE, que vienen en notación científica (`3.05001E+11`) porque el archivo se guardó desde Excel.
   - Los nombres de columna se pasan a minúscula sin espacios; ya venían así.
5. **Filtro Valle del Cauca.** Se compara `UPPER(TRIM(estu_depto_reside))` con `VALLE` y `VALLE DEL CAUCA`. El único valor con "VALLE" en el archivo es `VALLE`. Quedan 57.341 filas de 680.893 (8,4 %).
6. **Filtro de año.** Se conservan los periodos que empiezan por 2019, que son `20191` y `20192`. El archivo solo trae 2019, así que este filtro no descarta filas; protege la rama si se le pasa un histórico de varios años.
7. **Escritura.** `raw_csv.parquet` queda con 57.341 filas y 23 columnas de texto, y se sobreescribe en cada ejecución.

**`transform_csv`** (`raw_csv.parquet` → `staging_csv.parquet`)

8. **Columnas faltantes.** Las que no existen en el CSV se crean en nulo. En 2019 no faltó ninguna.
9. **Limpieza compartida.** Se llama `limpiar(df, fuente="csv_2019")` (sección 4.2), con este resultado:
   - *Llaves nulas descartadas:* 0.
   - *Inscripción individual (sin colegio):* 10.784 estudiantes. Quedan con código `-1`, nombre `Sin colegio (inscripción individual)` y `No aplica` en naturaleza, calendario y área.
   - *Texto:*
     - ftfy no reparó ningún valor.
     - Se quitaron espacios sobrantes del nombre del colegio en 1.287 filas.
     - Los faltantes quedaron como `No informa`: estrato 14.575, educación del padre 14.257, educación de la madre 14.220, internet 14.306, computador 12.849 y personas en el hogar 12.736.
   - *Numéricos:* puntajes a entero (`punt_ingles` con 152 nulos), INSE a decimal (12.899 nulos) y NSE a entero con `-1` si falta (12.899).
   - *Tiempo:* `anio` = 2019; `semestre` 1 (11.624 filas) y 2 (45.717 filas).
   - *Duplicados:* 0 filas idénticas y 0 dobles inscripciones.
10. **Control de la rama.** Se verifica que solo queden `VALLE` y 2019; si no, la tarea falla.
11. **Escritura.** `staging_csv.parquet` queda con 57.341 filas y 25 columnas, sin índice. Cumple `validar_esquema()` y se sobreescribe en cada ejecución.

Hallazgos:
- **Inscritos individuales sin datos socioeconómicos.** Solo 5 de 10.784 los tienen. Entre los escolarizados falta el 8,2 % de los estratos.
- **Puntajes.**
  - Todos están dentro de su rango.
  - `punt_global` coincide con la fórmula ponderada del ICFES en las 57.189 filas con los cinco puntajes.
  - Un estudiante tiene 0 en todas las pruebas y se conserva; GX decide si el lote pasa.
- **Semestres con poblaciones distintas.**
  - `20191` es calendario B: 7.462 de 7.484 escolarizados son de colegios no oficiales, con puntaje global medio de 284,4.
  - `20192` es calendario A: 30.774 de 39.073 escolarizados son de colegios oficiales, con puntaje global medio de 245,1.
  - Por eso 2019 se compara con el año completo, no con un solo semestre.

Pruebas realizadas:
- 5 pruebas de la rama en `tests/test_rama_csv.py` (filtro, varios archivos, variable de entorno, errores, esquema, repetibilidad y columnas faltantes). Se corren con `pytest -q`.
- `read_csv` y `transform_csv` corren sobre el archivo completo.
- Repetibilidad: dos ejecuciones seguidas dan el mismo staging (57.341 filas).

### 4.6 Decisiones pendientes para el esquema v2
- Nombres de departamento y municipio entre años (por ejemplo `VALLE` vs `VALLE DEL CAUCA`, o `CALI` vs `SANTIAGO DE CALI`): hoy quedarían como lugares distintos en `dim_ubicacion`.
- Categorías que cambian de redacción entre años (educación de los padres, `Si`/`Sí`): el DW no las mezcla por error, pero el dashboard las mostraría separadas.

## 5. API elegida.
_pendiente_

## 6. Suite de Great Expectations.
_pendiente_

## 7. EDA y visualizaciones 
_pendiente_

## 8. Modelo de datos

Modelo en estrella en MySQL (base `saber11_dw_e2`, script `sql/esquema_dw.sql`). **Granularidad:** una fila de la fact por estudiante (`bk_estu_consecutivo`).

![Modelo de datos en estrella](docs/modelo_datos.png)

*Figura 3. Modelo en estrella: la fact `fact_resultadossaber11` y sus cuatro dimensiones.*

| Tabla | Contenido | Llaves |
|---|---|---|
| `fact_resultadossaber11` | Puntaje global, puntajes por prueba (matemáticas, lectura crítica, naturales, sociales y ciudadanas, inglés) e INSE individual | PK `id_resultado`; FK `id_colegio`, `id_ubicacion`, `id_socioeconomico`, `id_tiempo`; único `bk_estu_consecutivo` |
| `dim_colegio` | Código ICFES, nombre, naturaleza, calendario y área de ubicación del colegio | PK `id_colegio`; único `cole_cod_icfes` (`-1` = inscripción individual) |
| `dim_ubicacion` | Departamento y municipio de residencia del estudiante | PK `id_ubicacion`; único (departamento, municipio) |
| `dim_socioeconomica` | Estrato, educación del padre y de la madre, internet, computador, personas en el hogar y NSE | PK `id_socioeconomico`; único la combinación de las 7 columnas |
| `dim_tiempo` | Periodo, año, semestre y etapa de la pandemia (Antes / Durante / Después) | PK `id_tiempo`; único `periodo` |

Decisiones de diseño:
- Cada dimensión guarda una fila por combinación única de valores (restricción `UNIQUE`). Con `INSERT IGNORE` esto hace que la carga sea idempotente: cargar dos veces lo mismo no duplica filas.
- La fact no admite llaves nulas hacia las dimensiones: si una fila no encuentra su llave, `load` se detiene con un error en vez de descartarla.
- Todas las tablas usan `COLLATE utf8mb4_bin`, para que "Si"/"Sí" o "VALLE"/"Valle" no se mezclen al cargar las dimensiones.
- `dim_tiempo.etapa_pandemia` clasifica cada periodo (2019 Antes, 2020 Durante, 2025 Después) y facilita filtrar en el dashboard.
- La limpieza usa `punt_c_naturales` y `cole_codigo_icfes`; al cargar al DW se renombran a `punt_naturales` y `cole_cod_icfes`.

## 9. Cómo correrlo desde cero
Probado en Windows con PowerShell y Docker Desktop.

1. Instalar Docker Desktop y Git.
2. `git clone https://github.com/valegcollazos/ETL_ENTREGA_02_PROYECTO.git` y entrar a la carpeta.
3. Crear el `.env`: `copy .env.example .env` (Mac/Linux: `cp .env.example .env`) y cambiar `MYSQL_ROOT_PASSWORD` y `MYSQL_PASSWORD`.
4. Descargar el CSV 2019 (`2019.rar`, enlace: _pendiente_), descomprimirlo y guardarlo como `data/input/saber11_2019.csv` (es el nombre de `CSV_2019_PATH`). Viene en latin-1; `read_csv` lo detecta solo.
5. `docker compose up -d --build`. Con `docker compose ps`, `mysql` y `postgres` deben estar `healthy`; `airflow-init` termina solo con `exited (0)`. El MySQL del proyecto queda en el puerto 3307.
6. Descargar [raw_saber11.zip](https://drive.google.com/file/d/1AN8FpzTootHtXAvN6IH5E8Vl9OXivSOs/view?usp=drive_link), descomprimirlo en `dumps/` y cargarlo siguiendo `docs/fuente_db_carga_raw.md`. Verificación (debe dar 661891):

```powershell
   "SELECT COUNT(*) FROM saber11.raw_saber11;" | docker compose exec -T mysql sh -c 'mysql -u$MYSQL_USER -p$MYSQL_PASSWORD'
```

7. Abrir http://localhost:8080 (usuario y contraseña `admin`), activar `saber11_valle_etl` y ejecutarlo con *Trigger DAG*. En la vista Grid todas las tareas deben quedar en verde.
8. Revisar el resultado (deben aparecer 2019, 2020 y 2025):

```powershell
   "SELECT t.anio, t.etapa_pandemia, COUNT(*) FROM fact_resultadossaber11 f JOIN dim_tiempo t USING (id_tiempo) GROUP BY t.anio, t.etapa_pandemia;" | docker compose exec -T mysql sh -c 'mysql -u$MYSQL_USER -p$MYSQL_PASSWORD saber11_dw_e2'
```

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
