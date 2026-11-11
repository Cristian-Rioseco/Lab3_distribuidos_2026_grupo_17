package main

import (
	"context"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	pb "distrieats/proto"

	"google.golang.org/grpc"
)

// Estructura principal del Servidor de Nombres
type server struct {
	pb.UnimplementedNameServerServer
	mu        sync.Mutex
	registros map[string]RegistroInfo
}

// Almacena la dirección IP:Puerto y cuándo fue la última vez que reportó estar vivo
type RegistroInfo struct {
	Direccion           string
	UltimaActualizacion time.Time
}

// aca se guarda o actualiza la ubicación de una entidad
func (s *server) Registrar(ctx context.Context, req *pb.RegistroReq) (*pb.RegistroRes, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.registros[req.Nombre] = RegistroInfo{
		Direccion:           req.Direccion,
		UltimaActualizacion: time.Now(),
	}
	log.Printf("[NAMESERVER] Registrado: %s -> %s\n", req.Nombre, req.Direccion)
	return &pb.RegistroRes{Exito: true}, nil
}

// aca se busca y devuelve la dirección física de un nombre lógico
func (s *server) Resolver(ctx context.Context, req *pb.ResolverReq) (*pb.ResolverRes, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	info, existe := s.registros[req.Nombre]
	if !existe {
		return &pb.ResolverRes{Encontrado: false}, nil
	}
	return &pb.ResolverRes{Direccion: info.Direccion, Encontrado: true}, nil
}

// aca se retorna todos los nombres que coincidan con un prefijo (usado por el algoritmo Bully)
func (s *server) Listar(ctx context.Context, req *pb.ListarReq) (*pb.ListarRes, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	coincidencias := make(map[string]string)
	for nombre, info := range s.registros {
		if strings.HasPrefix(nombre, req.Prefijo) {
			coincidencias[nombre] = info.Direccion
		}
	}
	return &pb.ListarRes{NombresDirecciones: coincidencias}, nil
}

// aca se actualiza el temporizador de vida de una entidad (Heartbeat)
func (s *server) RenovarLease(ctx context.Context, req *pb.HeartbeatReq) (*pb.HeartbeatRes, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	info, existe := s.registros[req.Nombre]
	if existe {
		info.UltimaActualizacion = time.Now()
		s.registros[req.Nombre] = info
		return &pb.HeartbeatRes{Renovado: true}, nil
	}
	return &pb.HeartbeatRes{Renovado: false}, nil
}

// aca es la rutina en segundo plano que elimina entidades si no envían heartbeat en 10 segundos
func (s *server) limpiador() {
	for {
		time.Sleep(3 * time.Second) // Revisa cada 3 segundos
		s.mu.Lock()
		ahora := time.Now()
		for nombre, info := range s.registros {
			// Si pasaron más de 10 segundos desde la última actualización, expiró
			if ahora.Sub(info.UltimaActualizacion) > 10*time.Second {
				log.Printf("[NAMESERVER] Registro expirado y eliminado: %s\n", nombre)
				delete(s.registros, nombre)
			}
		}
		s.mu.Unlock()
	}
}

func main() {
	lis, err := net.Listen("tcp", ":50050")
	if err != nil {
		log.Fatalf("Error al escuchar: %v", err)
	}

	s := grpc.NewServer()
	ns := &server{
		registros: make(map[string]RegistroInfo),
	}

	pb.RegisterNameServerServer(s, ns)
	go ns.limpiador()

	log.Println("[NAMESERVER] Servidor de Nombres esta escuchando en el puerto 50050")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("!! ERROR EN EL SERVIDOR (FUNÓ) !!: %v", err)
	}
}
