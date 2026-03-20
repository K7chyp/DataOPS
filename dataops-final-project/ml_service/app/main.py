from fastapi import FastAPI, Request
import time
import json
from prometheus_client import Counter, Histogram, generate_latest, REGISTRY
from .schemas import PredictionRequest, PredictionResponse
from .models import model
from .database import SessionLocal, PredictionLog
from .logger import logger

app = FastAPI(title="ML Service")

request_count = Counter("http_requests_total", "Total HTTP requests", ["method", "endpoint"])
request_latency = Histogram("http_request_duration_seconds", "HTTP request latency", ["method", "endpoint"])

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/metrics")
async def metrics():
    return generate_latest(REGISTRY)

@app.post("/api/v1/predict", response_model=PredictionResponse)
async def predict(request: PredictionRequest, http_request: Request):
    start = time.time()
    request_count.labels(method="POST", endpoint="/api/v1/predict").inc()

    features = request.features
    logger.info("Prediction request", extra={"features": features})

    pred = model.predict(features)

    latency = time.time() - start
    request_latency.labels(method="POST", endpoint="/api/v1/predict").observe(latency)

    db = SessionLocal()
    log_entry = PredictionLog(
        input_features=json.dumps(features),
        prediction=pred,
        model_version="dummy_v1",
        latency_ms=latency * 1000
    )
    db.add(log_entry)
    db.commit()
    db.close()

    logger.info("Prediction response", extra={"prediction": pred, "latency_ms": latency*1000})

    return PredictionResponse(prediction=pred)
