FROM golang:1.26-alpine AS build

WORKDIR /src

COPY services/payment-api/go.mod services/payment-api/go.sum ./
RUN go mod download

COPY services/payment-api/ ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/payment-api ./cmd/api && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/outbox-publisher ./cmd/outbox-publisher

FROM alpine:3.22 AS runtime

RUN apk add --no-cache ca-certificates
RUN addgroup -S riskflow && adduser -S -G riskflow riskflow

USER riskflow

FROM runtime AS outbox-publisher

COPY --from=build /out/outbox-publisher /usr/local/bin/outbox-publisher

ENTRYPOINT ["/usr/local/bin/outbox-publisher"]

FROM runtime AS payment-api

COPY --from=build /out/payment-api /usr/local/bin/payment-api

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/payment-api"]
