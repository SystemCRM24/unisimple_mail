FROM python:3.13-slim
WORKDIR /app

COPY . /app/

RUN pip install --no-cache-dir --upgrade -r requirements.txt
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "11002"]