# Stage 1: Build binary
FROM golang:1.26-alpine AS builder

WORKDIR /app

RUN apk --no-cache add build-base

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=1 GOOS=linux go build -ldflags="-s -w" -o servergo main.go

# Stage 2: Minimal runtime image
FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

COPY --from=builder /app/servergo /app/servergo

# Expose HTTP Web Dashboard (8090) first for PaaS HTTP router (Coolify / Traefik), then gRPC (50051) and UDP (50052/udp)
EXPOSE 8090 50051 50052/udp

ENV SERVER_IP=209.126.81.68
ENV GRPC_PORT=50051
ENV WEB_PORT=8090

CMD ["/app/servergo"]
