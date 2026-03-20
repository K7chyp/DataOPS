FROM python:3.11-slim

RUN apt-get update && apt-get install -y nodejs npm && \
    pip install jupyterhub jupyterlab configurable-http-proxy && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    jupyterhub \
    jupyterlab \
    configurable-http-proxy \
    dockerspawner

COPY jupyterhub_config.py /srv/jupyterhub/jupyterhub_config.py

WORKDIR /srv/jupyterhub

CMD ["jupyterhub", "-f", "/srv/jupyterhub/jupyterhub_config.py"]

