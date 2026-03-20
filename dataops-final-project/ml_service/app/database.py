from sqlalchemy import create_engine, Column, Integer, Float, DateTime, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os
from datetime import datetime

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@postgres:5432/ml_logs")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class PredictionLog(Base):
    __tablename__ = "predictions"

    id = Column(Integer, primary_key=True, index=True)
    input_features = Column(String)
    prediction = Column(Integer)
    model_version = Column(String)
    timestamp = Column(DateTime, default=datetime.utcnow)
    latency_ms = Column(Float)

Base.metadata.create_all(bind=engine)
