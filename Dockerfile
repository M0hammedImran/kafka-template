## Build
FROM golang:1.19-buster AS build

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY *.go ./

RUN go build -o /app/grandline

## Deploy
FROM gcr.io/distroless/base-debian10

WORKDIR /

COPY --from=build  /app/grandline /grandline

USER nonroot:nonroot

ENTRYPOINT ["/grandline"]