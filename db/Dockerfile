FROM postgres:13

ENV POSTGRES_USER='postgres'
ENV POSTGRES_PASSWORD='admin'
ENV POSTGRES_DB='avito_api_db'

COPY create_db.sql /docker-entrypoint-initdb.d/
