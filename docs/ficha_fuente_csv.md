# Ficha de la fuente — CSV 2019 (rama CSV)

| Campo | Valor |
|---|---|
| Origen | CSV de datos abiertos ICFES Saber 11 2019 (`2019.csv`, nacional, 459,6 MB). Mismas 94 columnas de la tabla `raw_saber11` de la entrega 1 |
| Formato | Separado por comas, **latin-1 (ISO-8859-1)**, no UTF-8. `read_csv` lo detecta y lo lee en latin-1 |
| Filas leídas (archivo completo) | 680.893 |
| Valores de depto que contienen VALLE | `VALLE` (57.341 filas) |
| Filas tras filtro Valle + 2019 | 57.341 |
| Filas en staging_csv.parquet | 57.341 filas, 25 columnas |
| Columnas que faltaron | Ninguna (el CSV trae las 23 columnas de entrada, incluidas `estu_nse_individual` y `estu_inse_individual`; `anio` y `semestre` se derivan de `periodo` en la limpieza) |
| Valores únicos de periodo | `20191` (11.624 filas en Valle; 66.104 en el país) y `20192` (45.717 en Valle; 614.789 en el país) |
| Estudiantes sin colegio | 10.784 (inscripción individual) |
| Filas descartadas por doble inscripción | 0 |
| Valores reparados con ftfy | 0 (leído en latin-1, el texto no tiene caracteres corruptos) |
| Hallazgos de calidad | Ver la lista debajo de esta tabla. |

Hallazgos de calidad:

- **Archivo guardado desde Excel.** Los códigos DANE (`cole_cod_dane_*`) quedaron en notación científica (`3.05001E+11`) y perdieron dígitos. No son columnas del esquema, así que no afectan el staging.
- **Las columnas que usamos están intactas.** Se compararon las 66.104 filas de 20191 con una versión UTF-8 del mismo periodo, y las 23 columnas del esquema coinciden en todas.
- **Llaves y duplicados.** No hay filas con llaves nulas, filas repetidas, dobles inscripciones ni colegios con atributos distintos entre registros.
- **Datos socioeconómicos faltantes.** Los inscritos individuales casi nunca los traen: solo 5 de 10.784 tienen estrato. Entre los escolarizados faltan el 8,2 % de los estratos. En total hay 12.899 filas con NSE `-1` e INSE nulo.
- **Puntajes.**
  - `punt_ingles` tiene 152 nulos; las demás pruebas no tienen nulos.
  - Todos los puntajes están dentro de su rango.
  - `punt_global` coincide con la fórmula ponderada del ICFES en las 57.189 filas que tienen los cinco puntajes.
  - Un estudiante de 20192 tiene 0 en todas las pruebas. Se conserva; GX decide si el lote pasa.
- **Colegio fuera del departamento.** 236 escolarizados residen en el Valle pero su colegio está en otro departamento. Se conservan, porque el filtro es por residencia.

## Resultado de la rama

`read_csv` y `transform_csv` corren sin errores desde la máquina local sobre el archivo completo. Toman unos 8 segundos y usan un máximo de 216 MB de memoria, porque leen por bloques y solo las 23 columnas. El resultado es repetible: cada ejecución sobreescribe el staging y mantiene 57.341 filas.

El archivo `staging_csv.parquet` queda con 25 columnas, solo `VALLE` y solo los periodos `20191` y `20192`. `tests/test_rama_csv.py` prueba el filtro, la lectura de varios archivos, los errores, el esquema de salida y la repetibilidad.

## Nota para el análisis: los dos semestres son poblaciones distintas

| Periodo | Calendario | Escolarizados en Valle | Oficiales | Puntaje global medio |
|---|---|---|---|---|
| `20191` | B (97 %) | 7.484 | 22 | 284,4 |
| `20192` | A (98 %) | 39.073 | 30.774 | 245,1 |

Al comparar 2019 con 2020 y 2025 conviene usar el año completo, que es lo que trae el staging, o comparar por periodo y naturaleza del colegio. Comparar un solo semestre llevaría a conclusiones distintas.
