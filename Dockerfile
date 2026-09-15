# Stage 1: Build binary
FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o servergo main.go

# Stage 2: Minimal runtime image
FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

COPY --from=builder /app/servergo /app/servergo

EXPOSE 50051 8080

ENV SERVER_IP=209.126.81.68
ENV GRPC_PORT=50051
ENV WEB_PORT=8080

CMD ["/app/servergo"]
