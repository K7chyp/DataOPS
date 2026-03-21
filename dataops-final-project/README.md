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

## Картинки
<img width="1568" height="813" alt="image" src="https://github.com/user-attachments/assets/aa002465-f9c0-44ec-86a3-f18ab5825c01" />
<img width="1493" height="857" alt="image" src="https://github.com/user-attachments/assets/77d1785f-1015-4135-bfca-31dd39155604" />
<img width="798" height="97" alt="image" src="https://github.com/user-attachments/assets/b4a36d52-eef1-4ab2-b1c1-0885e2c37e4d" />
<img width="1461" height="909" alt="image" src="https://github.com/user-attachments/assets/74348f15-9f91-4bb7-99ce-ebcfbe444547" />




