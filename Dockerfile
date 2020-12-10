FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8

RUN pip install SQLAlchemy && \
    pip install pymssql

COPY ./app /app/app