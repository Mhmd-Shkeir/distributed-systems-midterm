FROM mirror.gcr.io/library/python:3.12-slim
WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY node ./node
COPY metadata ./metadata
CMD ["uvicorn", "node.app:app", "--host", "0.0.0.0", "--port", "8080"]
