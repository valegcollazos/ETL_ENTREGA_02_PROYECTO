# Ficha de la fuente — API datos.gov.co 2020 (rama API, Lauren)

| Campo | Valor |
|---|---|
| Origen | API de datos abiertos datos.gov.co (Socrata, SODA2), Saber 11 año 2020 |
| Conjuntos consultados | `a8xr-en99` (periodo `20201`) y `rnvb-vnyh` (periodo `20204`) |
| Dirección de consulta | `https://www.datos.gov.co/resource/<id>.json` |
| Filtro aplicado en la consulta | `upper(estu_depto_reside) = 'VALLE'`, ordenado por `estu_consecutivo`, páginas de 10.000 filas |
| Autenticación | Sin token (`SOCRATA_APP_TOKEN` es opcional) |
| Filas del país (referencia) | 15.435 en `a8xr-en99` y 504.872 en `rnvb-vnyh` |
| Valores de depto que contienen VALLE | `VALLE` |
| Filas descargadas de Valle | 41.743 (6.301 de `a8xr-en99` y 35.442 de `rnvb-vnyh`) |
| Filas en raw_api.parquet | 41.743 filas, 81 columnas (la API entrega más columnas de las que usa el esquema) |
| Filas en staging_api.parquet | 41.743 filas, 25 columnas |
| Columnas que faltaron | `estu_nse_individual` y `estu_inse_individual` no existen en `a8xr-en99`; se crean en nulo. En `rnvb-vnyh` existen pero vienen vacías en parte de las filas |
| Valores únicos de periodo | En la API: `20201` (6.301 filas) y `20204` (35.442 filas). En staging: `20201` y `20202` |
| Corrección de periodo | `20204` se reasigna a `20202` en `transform_api`, antes de la limpieza compartida. `raw_api.parquet` conserva el valor original |
| Filas sin NSE/INSE | 7.791 (6.301 del conjunto 2020-1 y 1.490 del 2020-2). Quedan en `-1` para NSE y nulo para INSE |
| Estudiantes sin colegio | 0 |
| Filas descartadas por doble inscripción | 0 |
| Valores reparados con ftfy | Ninguno |
| Hallazgos de calidad | Sin llaves nulas, sin filas idénticas repetidas y sin colegios con atributos distintos entre registros. La API omite los campos nulos en el JSON, por eso las columnas que un registro no trae quedan vacías al armar la tabla |

## Por qué esta fuente

El conjunto de 2020 está partido en dos publicaciones de datos.gov.co, una por periodo, y ninguna cubre sola el año. Se usan las dos y se unen en un solo archivo. El filtro de Valle se hace dentro de la consulta para no descargar el país completo (más de 500.000 filas). Se usa la versión SODA2 (`/resource/<id>.json`) porque funciona sin autenticación; la versión `/api/v3/views/...` pide credenciales.

## Decisión sobre el periodo

El conjunto 2020-2 publica el periodo como `20204`. El contrato del proyecto solo admite semestres 1 o 2, y ninguna de las dos publicaciones trae un `20202`. El grupo decidió tratar el `20204` como segundo semestre de 2020 (`20202`). Es un supuesto del equipo: la fuente no explica qué significa el 4. El cambio se hace en `transform_api` y la suite de calidad exige que el periodo termine en 1 o 2.

## Resultado de la rama

`extract_api` y `transform_api` corren sin errores desde la máquina local. El resultado es repetible: cada ejecución sobreescribe los archivos y mantiene 41.743 filas. `staging_api.parquet` queda con 25 columnas, solo `VALLE` y solo año 2020 (`20201` y `20202`).
