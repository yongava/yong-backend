FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8

RUN pip install --no-cache-dir pandas && \
    pip install --no-cache-dir BeautifulSoup4 && \
    pip install --no-cache-dir requests \ 
    pip install SQLAlchemy && \
    pip install pymssql

COPY ./app /app/app