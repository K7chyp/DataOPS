# DataOps Final Project

## Структура проекта

- `mlflow/` – MLflow tracking server
- `airflow/` – Apache Airflow
- `lakefs/` – LakeFS с MinIO
- `jupyterhub/` – JupyterHub
- `ml_service/` – ML-сервис на FastAPI
- `monitoring/` – Prometheus + Grafana
- `k8s/` – Kubernetes манифесты
- `helm/ml-service/` – Helm chart
- `create_prompts.py` – скрипт для создания промптов в MLflow

## Запуск

1. Скопируйте `.env.example` в `.env` в каждой директории и заполните своими значениями.
2. Запустите каждый сервис через `docker-compose up -d` в соответствующей папке.
3. Для мониторинга убедитесь, что ML-сервис доступен для Prometheus.
4. Для Kubernetes примените манифесты из `k8s/` или установите Helm chart.

## Проверка

- MLflow: http://localhost:5000
- Airflow: http://localhost:8080
- LakeFS: http://localhost:8000
- JupyterHub: http://localhost:8000
- ML-сервис: http://localhost:8001/docs
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin/admin)

## Примечания

- Для работы Ingress в Kubernetes необходим контроллер ingress.
- В Helm chart можно настроить параметры через `values.yaml`.
