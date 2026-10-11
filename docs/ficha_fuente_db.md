# Ficha de la fuente — Base SQL 2025 (rama DB, Valeria)

| Campo | Valor |
|---|---|
| Origen | Tabla `raw_saber11` de la entrega 1 (MySQL), datos abiertos ICFES Saber 11 2025 |
| Filas leídas (tabla completa) | 661.891 |
| Valores de depto que contienen VALLE | `VALLE` (50.875 filas) |
| Filas tras filtro Valle + 2025 | 50.875 |
| Filas en staging_db.parquet | 50.875 filas, 25 columnas |
| Columnas que faltaron | Ninguna (la tabla raw ya tiene las 23 columnas de entrada; `anio` y `semestre` se derivan de `periodo` en la limpieza) |
| Valores únicos de periodo | `20251` (11.947 filas) y `20252` (38.928 filas) |
| Estudiantes sin colegio | 7.856 (inscripción individual) |
| Filas descartadas por doble inscripción | 0 |
| Valores reparados con ftfy | `estu_mcpio_reside`: 9 · `cole_nombre_establecimiento`: 118 · `fami_educacionpadre`: 4 · `fami_educacionmadre`: 4 · `fami_personashogar`: 1 |
| Hallazgos de calidad | Sin filas con llaves nulas (estudiante/periodo), sin filas idénticas repetidas y sin doble inscripción. 376 colegios aparecen con atributos distintos entre registros (aviso informativo de la limpieza, no es un error). |

## Resultado de la rama

`read_db` y `transform_db` corren sin errores, tanto desde la máquina local como dentro de Airflow (`airflow tasks test`). El resultado es repetible: cada ejecución sobreescribe el staging y mantiene 50.875 filas.

El archivo `staging_db.parquet` queda con 25 columnas, solo `VALLE` y solo periodos de 2025 (`20251` y `20252`).