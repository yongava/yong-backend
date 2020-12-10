FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8

RUN pip install --upgrade pip && \
    pip install --no-cache-dir pandas && \
    pip install --no-cache-dir BeautifulSoup4 && \
    pip install --no-cache-dir requests \ 
    pip install --no-cache-dir SQLAlchemy && \
    pip install --no-cache-dir pymssql && \
    pip install --no-cache-dir pathlib && \
    pip install --no-cache-dir azure.storage.blob

COPY ./app /app/app