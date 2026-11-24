# Streaming con Kafka: de la tabla de hechos a la decisión

Este documento describe la parte de streaming de la entrega final: cómo los registros del
modelo dimensional se publican en Kafka, cómo se predice el riesgo de cada estudiante en
tiempo real y de dónde sale la información que consume el dashboard.

## 1. Problema operativo que justifica el streaming

Un modelo que predice "estudiante en riesgo" solo sirve si alguien lo usa para actuar.
La decisión operativa es **a qué colegios y municipios priorizar apoyo o refuerzo**.
En lugar de esperar a un reporte mensual, cada estudiante que llega al sistema se
puntúa en el momento y el dashboard mantiene al día la lista de colegios prioritarios
(ver la sección 7 y el código en `streaming/dashboard/app.py`). Kafka desacopla quien
produce los datos (el ETL) de quien los consume (el modelo y el dashboard).

## 2. Arquitectura del flujo

```
Fuentes (CSV 2019, API 2020, BD 2025)
        │  DAG de Airflow: extract → transform → merge → quality_check → load
        ▼
Modelo dimensional (MySQL, saber11_dw_e2)
        │  tarea publicar_kafka (después de load)
        ▼
Productor ──► Kafka, tópico saber11.estudiantes
                       │
                       ▼
              Consumidor (carga el modelo .joblib una vez, predice por evento)
                 │                       │
                 ▼                       ▼
   Kafka, tópico saber11.predicciones   data/streaming/predicciones.jsonl
                                              │
                                              ▼
                                   Dashboard (Streamlit)
```

- Kafka corre en modo KRaft, un solo nodo (`apache/kafka:3.9.0`), definido en `docker-compose.yml`.
- Los tópicos `saber11.estudiantes` y `saber11.predicciones` los crea el servicio `kafka-init` (1 partición cada uno).
- El reporte estático (analítico) consulta directamente el modelo dimensional.

## 3. Tabla de hechos: significado, grano y reejecuciones

**Tabla:** `fact_resultadossaber11` (esquema en `sql/esquema_dw.sql`).

- **Grano:** una fila por estudiante evaluado. El estudiante se identifica con
  `bk_estu_consecutivo` (código de la prueba), que tiene restricción `UNIQUE`
  (`uq_estu_consecutivo`). El periodo (2019, 2020 o 2025) viene de `dim_tiempo`.
- **Medidas:** `punt_global`, `punt_matematicas`, `punt_lectura_critica`, `punt_naturales`,
  `punt_sociales_ciudadanas`, `punt_ingles` y `estu_inse_individual`.
- **Llaves a dimensiones:** `id_colegio`, `id_socioeconomico`, `id_tiempo`, `id_ubicacion`.
- **Identificador de evento:** `id_resultado` (clave primaria, autoincremental).

**Cargas duplicadas y reejecuciones del DAG.** La carga es idempotente: el `UNIQUE` sobre
`bk_estu_consecutivo` junto con `INSERT IGNORE` evita duplicar estudiantes si el DAG se
vuelve a ejecutar. Los registros ya cargados conservan su `id_resultado`, por eso el
`event_id` de un mismo estudiante no cambia entre ejecuciones. (El contador
`AUTO_INCREMENT` puede dejar huecos al reejecutar; no afecta a los ids existentes.)

## 4. Evento publicado: tópico `saber11.estudiantes`

- **Clave del mensaje:** `event_id` como texto.
- **Valor:** JSON en UTF-8.
- **Qué NO viaja:** `punt_global` ni ningún puntaje. El target no se publica como variable.

Ejemplo:

```json
{
  "event_id": 9483,
  "event_ts": "2026-11-22T19:12:27.701+00:00",
  "semestre": 2,
  "fami_estratovivienda": "Estrato 1",
  "fami_educacionpadre": "Primaria incompleta",
  "fami_educacionmadre": "Primaria incompleta",
  "fami_tieneinternet": "No",
  "fami_tienecomputador": "Si",
  "fami_personashogar": "7 a 8",
  "cole_naturaleza": "OFICIAL",
  "cole_calendario": "A",
  "cole_area_ubicacion": "URBANA",
  "estu_mcpio_reside": "CALI",
  "cole_cod_icfes": "162958",
  "cole_nombre_establecimiento": "INSTITUCION EDUCATIVA MONSEÑOR RAMON ARCILA"
}
```

| Campo | Descripción |
|---|---|
| `event_id` | `id_resultado` de la fact; identifica el evento y permite descartar repetidos |
| `event_ts` | Momento en que el productor emitió el evento (UTC) |
| `semestre` | 1 o 2, **entero** (si va como texto el modelo predice distinto) |
| Variables del hogar (`fami_*`) y del colegio (`cole_*`), `estu_mcpio_reside` | Las 11 variables de entrada del modelo (junto con `semestre`) |
| `cole_cod_icfes` | Código del colegio; el modelo 2.0 lo usa para buscar el historial del colegio |
| `cole_nombre_establecimiento` | Solo para mostrar en el dashboard; el modelo no lo usa |

## 5. Productor (`streaming/productor.py`)

- Lee los estudiantes del modelo dimensional uniendo la fact con las cuatro dimensiones
  (solo Valle del Cauca, con `punt_global` no nulo).
- Por defecto solo envía estudiantes del **conjunto de prueba** (`models/ids_test.csv`),
  que el modelo no vio al entrenar. Con `--todos` envía a todos.
- Ordena por `id_resultado` y mezcla con semilla fija, así la muestra es reproducible.
- Publica uno a uno con una pequeña pausa (`--delay`, por defecto 0,3 s; sin CDC).
- Se ejecuta de dos formas:
  - Dentro del DAG: tarea `publicar_kafka` (después de `load`, en paralelo con
    `cleanup_staging`). Publica `KAFKA_EVENTOS_POR_EJECUCION` eventos (200 por defecto)
    con pausa `KAFKA_PAUSA_SEGUNDOS` (0,05 por defecto).
  - A mano: `docker compose --profile streaming run --rm productor`.

## 6. Consumidor e inferencia (`streaming/consumidor.py`)

1. Al arrancar carga **una sola vez** el artefacto `models/modelo_riesgo.joblib`
   (pipeline con preprocesamiento + modelo, y su metadata). No se reentrena por mensaje.
2. Por cada evento arma una fila con las variables de entrada, calcula la probabilidad
   de riesgo y la convierte en etiqueta con el umbral guardado en
   `metadata['decision_threshold']` (no con 0,5).
3. Si el modelo trae `metadata['historial_colegios']` (modelo 2.0), añade la columna
   `colegio_riesgo_hist` buscando el colegio por `cole_cod_icfes`; si el colegio es nuevo
   usa la media global.
4. Escribe la predicción en `data/streaming/predicciones.jsonl` (una línea por evento) y la
   publica en el tópico `saber11.predicciones`.
5. Descarta los `event_id` que ya procesó (idempotencia ante reejecuciones o reenvíos).

**Mensaje de predicción:**

```json
{
  "event_id": 9483,
  "model_id": "saber11-estudiante-en-riesgo",
  "model_version": "2.0.0",
  "prediction": 1,
  "prediction_label": "en_riesgo",
  "probability_riesgo": 0.6487,
  "event_ts": "2026-11-22T19:12:27.701+00:00",
  "pred_ts": "2026-11-22T19:12:27.739+00:00",
  "semestre": 2,
  "estu_mcpio_reside": "CALI",
  "fami_estratovivienda": "Estrato 1",
  "cole_cod_icfes": "162958",
  "cole_nombre_establecimiento": "INSTITUCION EDUCATIVA MONSEÑOR RAMON ARCILA"
}
```

Los seis primeros campos son el formato acordado con el modelo; los demás se agregan
para que el dashboard pueda agrupar por colegio, municipio y estrato sin consultar la base de datos.

## 7. Dashboard

El dashboard **operativo** (`streaming/dashboard/app.py`) no consulta MySQL: lee
`data/streaming/predicciones.jsonl`, que escribe el consumidor. Simula la reactividad con
un *pull* periódico usando `@st.fragment(run_every="3s")`, que refresca solo la parte viva
sin recargar toda la página; el caché (`@st.cache_data(ttl=60)`) se usa solo en lo que casi
no cambia. Muestra la lista de prioridad de colegios y municipios, alertas con umbral
ajustable, una acción sugerida y apoyo analítico (% en riesgo total y por estrato). Corre en
http://localhost:8501. No se confunde con el dashboard **analítico** (`dashboards/saber11_valle.html`),
que sí consulta el DW y es de punto en el tiempo.

## 8. Cómo ejecutarlo

```bash
docker compose up -d                                           # Airflow, MySQL, Kafka
docker compose --profile streaming up -d consumidor dashboard  # consumidor + dashboard (http://localhost:8501)
docker compose --profile streaming run --rm productor          # publicar eventos (o ejecutar el DAG)
```

Para ver las predicciones: `data/streaming/predicciones.jsonl`. El dashboard queda en
http://localhost:8501. Para una demo sin levantar el stack: `python streaming/dashboard/simular_stream.py`.

## 9. Limitaciones

- Kafka con un solo nodo y una sola partición: suficiente para la demostración, no para producción.
- Los datos son históricos; el productor simula la llegada de estudiantes con una pausa.
- Cada ejecución del DAG publica la misma muestra reproducible; el consumidor descarta los
  repetidos, así que repetir el DAG no hace crecer el dashboard salvo que se aumente el límite.
- El modelo solo usa el perfil del hogar y del colegio: indica probabilidad de riesgo, no causas.
- Las variables socioeconómicas tienen cerca de 25 % de "No informa" en 2019 y 2025.
- **`event_id` depende de `id_resultado`** (autoincremental de MySQL), igual que `models/ids_test.csv`.
  Mientras no se vacíe y recargue el DW entre el entrenamiento y la demo, los ids son estables y
  el productor publica exactamente el conjunto de prueba. Si se recarga el DW, los `id_resultado`
  cambian: habría que reentrenar (regenerar `ids_test.csv`) antes de volver a publicar. Una versión
  más robusta usaría `bk_estu_consecutivo` (la llave de negocio, estable) como identificador del
  evento; se deja anotado como mejora futura.
