# Imagen de Airflow del proyecto. CAMBIAR la versión por la que usamos en clase
# (en las dos líneas: FROM y la URL de constraints).
FROM apache/airflow:2.10.5-python3.11

COPY requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt"
