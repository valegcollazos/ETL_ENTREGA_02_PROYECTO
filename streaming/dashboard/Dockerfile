FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
# El código y los datos se montan por volumen en docker-compose (ver servicio "dashboard").
EXPOSE 8501
CMD ["streamlit", "run", "streaming/dashboard/app.py", \
     "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true"]
