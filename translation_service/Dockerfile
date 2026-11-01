FROM python:3.12-slim
WORKDIR /app
COPY requirements-common.txt /app/requirements-common.txt
COPY translation_service/requirements.txt /app/translation_service/requirements.txt
RUN pip install --no-cache-dir -r /app/translation_service/requirements.txt
COPY common /app/common
COPY translation_service /app/translation_service
ENV PYTHONPATH=/app
CMD ["python", "-m", "translation_service.main"]
