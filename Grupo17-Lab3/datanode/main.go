package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	pb "distrieats/proto" //

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// leer variables de entorno con un valor por defecto.
func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

type datanodeServer struct {
	pb.UnimplementedAlmacenamientoServer
	mu              sync.Mutex
	lotesProcesados map[string]bool // Para evitar duplicados (idempotencia)
}

// aca se recibe los sub-lotes procesados desde las Procesadoras y ez.
func (s *datanodeServer) GuardarLoteProcesado(ctx context.Context, req *pb.GuardarReq) (*pb.GuardarRes, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	//Verificar si el lote ya fue procesado
	if s.lotesProcesados[req.SubloteId] {
		log.Printf("[DATANODE] Lote duplicado descartado: %s\n", req.SubloteId)
		return &pb.GuardarRes{Exito: true}, nil
	}

	//crear el archivo CSV
	archivo, err := os.OpenFile("registro_maestro.csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("[DATANODE] Error al abrir archivo: %v", err)
		return &pb.GuardarRes{Exito: false}, err
	}
	defer archivo.Close()
	escritor := csv.NewWriter(archivo)
	info, _ := archivo.Stat()
	if info.Size() == 0 {
		cabecera := []string{"ID_Pedido", "Cadena", "Estado", "Timestamp_Emision", "Timestamp_Procesamiento", "Origen", "Procesadora"}
		escritor.Write(cabecera)
	}

	//Escribir los registros
	for _, p := range req.PedidosProcesados {
		fila := []string{
			p.IdPedido, p.Cadena, p.Estado, p.TimestampEmision,
			p.TimestampProcesamiento, p.Origen, p.Procesadora,
		}
		escritor.Write(fila)
	}
	escritor.Flush()

	//Marcamos el lote como procesado
	s.lotesProcesados[req.SubloteId] = true
	log.Printf("[DATANODE] Sub-lote guardado con éxito: %s (%d pedidos)\n", req.SubloteId, len(req.PedidosProcesados))

	return &pb.GuardarRes{Exito: true}, nil
}

// esta es la funcion para mantener vivo el registro en el NameServer
func enviarHeartbeats(clienteNameServer pb.NameServerClient, nombre string) {
	for {
		time.Sleep(3 * time.Second)
		_, err := clienteNameServer.RenovarLease(context.Background(), &pb.HeartbeatReq{Nombre: nombre})
		if err != nil {
			log.Printf("[DATANODE] Error al enviar heartbeat al NameServer: %v", err)
		}
	}
}

func main() {
	puerto := getEnv("DATANODE_PORT", "50070")
	hostAnunciado := getEnv("DATANODE_HOST", "localhost")
	nsAddr := getEnv("NAMESERVER_ADDR", "localhost:50050")

	direccionFisica := fmt.Sprintf("%s:%s", hostAnunciado, puerto)

	// nos conectamos al servidor de clientes
	conn, err := grpc.Dial(nsAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[DATANODE] No se pudo conectar al NameServer en %s: %v", nsAddr, err)
	}
	defer conn.Close()

	clienteNS := pb.NewNameServerClient(conn)
	nombreEntidad := "distrieats.almacenamiento.datanode"

	//Registrarse en el NameServer (le puse un pequeño loop, en caso de que tire error que el dan lo modifique)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, err = clienteNS.Registrar(ctx, &pb.RegistroReq{
			Nombre:    nombreEntidad,
			Direccion: direccionFisica,
		})
		cancel()

		if err == nil {
			log.Printf("[DATANODE] Registrado exitosamente en NameServer como %s -> %s\n", nombreEntidad, direccionFisica)
			break
		}
		log.Printf("[DATANODE] Esperando al NameServer (%s)...", nsAddr)
		time.Sleep(2 * time.Second)
	}

	go enviarHeartbeats(clienteNS, nombreEntidad)

	lis, err := net.Listen("tcp", ":"+puerto)
	if err != nil {
		log.Fatalf("[DATANODE] Error al escuchar en el puerto %s: %v", puerto, err)
	}

	s := grpc.NewServer()
	pb.RegisterAlmacenamientoServer(s, &datanodeServer{
		lotesProcesados: make(map[string]bool),
	})

	log.Printf("[DATANODE] Servidor de Almacenamiento escuchando en el puerto %s...", puerto)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("[DATANODE] Error en el servidor: %v", err)
	}
}
