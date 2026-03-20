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

## Проверка

- MLflow: http://localhost:5000
- Airflow: http://localhost:8080
- LakeFS: http://localhost:8000
- JupyterHub: http://localhost:8000
- ML-сервис: http://localhost:8001/docs
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin/admin)

