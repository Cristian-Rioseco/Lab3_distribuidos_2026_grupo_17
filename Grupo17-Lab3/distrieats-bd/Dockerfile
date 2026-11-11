FROM golang:1.27-alpine AS builder

WORKDIR /app

#Primero copiamos los archivos de dependencias
#(Asumimos que el go.mod está en la carpeta raíz del proyecto)
COPY ../go.mod ../go.sum ./
RUN go mod download

#Copiamos el código fuente de la entidad actual y la carpeta proto
COPY . .
#También necesitamos los contratos protobuf (jaime debes ajustar la ruta si están en otro lado)
COPY ../proto ./proto

#Compilamos el ejecutable
RUN go build -o app_ejecutable .

#Etapa de Producción
FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/app_ejecutable .

#Comando por defecto para ejecutar la aplicación
CMD ["./app_ejecutable"]