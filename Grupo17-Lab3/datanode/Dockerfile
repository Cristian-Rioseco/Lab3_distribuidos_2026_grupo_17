FROM golang:1.27-alpine AS builder

WORKDIR /app


COPY ../go.mod ../go.sum ./
RUN go mod download


COPY . .

COPY ../proto ./proto


RUN go build -o app_ejecutable ./datanode


FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/app_ejecutable .


CMD ["./app_ejecutable"]