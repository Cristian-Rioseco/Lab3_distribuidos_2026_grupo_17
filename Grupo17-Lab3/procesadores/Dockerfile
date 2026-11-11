FROM golang:1.27-alpine AS builder

WORKDIR /app

# Copiar archivos de dependencias desde la raíz
COPY go.mod go.sum ./
RUN go mod download

# Copiar todo el código del proyecto (esto incluye todas las carpetas)
COPY . .

# Compilamos el ejecutable ESPECIFICANDO SU CARPETA
RUN go build -o app_ejecutable ./procesadores

# --- Etapa de Producción ---
FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/app_ejecutable .

# Comando por defecto para ejecutar la aplicación
CMD ["./app_ejecutable"]