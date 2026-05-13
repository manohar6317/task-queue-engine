# ---- Build Stage ----
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Download dependencies first (cached layer — only re-runs if go.mod changes)
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build a statically linked binary
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /task-queue-api ./cmd/api

# ---- Runtime Stage ----
# Use minimal alpine image — final image is ~15MB instead of ~800MB
FROM alpine:3.19

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app
COPY --from=builder /task-queue-api .

EXPOSE 8080

# Run as non-root user for security
RUN adduser -D -g '' appuser
USER appuser

CMD ["./task-queue-api"]
