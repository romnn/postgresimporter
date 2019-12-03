FROM golang:latest AS build

RUN go get -v \
    github.com/romnnn/pgfutter

ENV APP_DIR=$GOPATH/src/github.com/romnnn/pgfutter/
RUN mkdir -p $APP_DIR
WORKDIR $APP_DIR

RUN go build -o /bin/pgfutter

FROM python:3.7-alpine

MAINTAINER roman <contact@romnn.com>

WORKDIR /app

ENV DB_HOST postgres
ENV DB_USER postgres
ENV DB_PASSWORD password
ENV DB_PORT 5432

COPY requirements.txt /app/
COPY postgresimporter/ /app/postgresimporter
COPY deployment/wait-for-postgres.sh /app/
COPY --from=build /bin/pgfutter /usr/bin/pgfutter
RUN chmod a+x /usr/bin/pgfutter
RUN dos2unix wait-for-postgres.sh
RUN ls -la /app

RUN apk update && apk add --no-cache ca-certificates postgresql-client libc6-compat && update-ca-certificates
RUN pip install --upgrade pip && pip install -r requirements.txt
# RUN wget -O /usr/bin/pgfutter https://github.com/lukasmartinelli/pgfutter/releases/download/v1.2/pgfutter_linux_amd64 && chmod a+x /usr/bin/pgfutter

ENTRYPOINT ["/bin/sh", "./wait-for-postgres.sh", "python", "-m", "postgresimporter.import"]
