FROM golang:1.27 AS builder
WORKDIR /app
COPY go.* ./
RUN go mod download
COPY . .
RUN make build

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /app/s3-logs-shipper /s3-logs-shipper
ENTRYPOINT ["/s3-logs-shipper"]
