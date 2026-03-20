from pydantic import BaseModel, Field
from typing import List

class PredictionRequest(BaseModel):
    features: List[float] = Field(..., example=[5.1, 3.5, 1.4, 0.2])

class PredictionResponse(BaseModel):
    prediction: int
