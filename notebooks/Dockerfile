#docker compose --profile analisis up -d jupyter
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends pandoc && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    "mistune<3.0.0" \
    jupyterlab==4.2.5 nbconvert==7.16.4 \
    pandas==2.2.3 numpy==1.26.4 scipy==1.13.1 \
    matplotlib==3.9.2 seaborn==0.13.2 \
    sqlalchemy==2.0.36 pymysql==1.1.1

WORKDIR /work/notebooks