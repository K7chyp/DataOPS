import mlflow
from mlflow import prompts

with mlflow.start_run():
    prompts.create_prompt(
        name="sentiment_analysis",
        template="Classify the sentiment of the following text: {text}",
        tags={"version": "v1"},
        description="Basic sentiment classification"
    )
    prompts.create_prompt_version(
        name="sentiment_analysis",
        template="Determine if the following text is positive, negative, or neutral: {text}",
        tags={"version": "v2"}
    )
    prompts.create_prompt_version(
        name="sentiment_analysis",
        template="Analyze the emotion of this text: {text}",
        tags={"version": "v3"}
    )
