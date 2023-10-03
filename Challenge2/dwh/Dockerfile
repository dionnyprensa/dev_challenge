FROM postgres as db
WORKDIR /app
COPY ./init.sh /docker-entrypoint-initdb.d
COPY ./init.sql ./init.sql