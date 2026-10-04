# syntax=docker/dockerfile:1
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGET=api
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/relayflow ./cmd/${TARGET}

FROM alpine:3.21
RUN addgroup -S relayflow && adduser -S -G relayflow relayflow
WORKDIR /app
COPY --from=build /out/relayflow /usr/local/bin/relayflow
COPY db/migrations ./db/migrations
USER relayflow
ENTRYPOINT ["/usr/local/bin/relayflow"]
