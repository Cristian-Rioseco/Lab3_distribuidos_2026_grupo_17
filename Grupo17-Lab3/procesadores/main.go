package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "distrieats/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	aliasLider    = "distrieats.procesamiento.lider"
	aliasDatanode = "distrieats.almacenamiento.datanode"
	prefijoProc   = "distrieats.procesamiento.procesadora"
)

type procesadoraServer struct {
	pb.UnimplementedCoordinacionBullyServer
	pb.UnimplementedProcesamientoLiderServer
	pb.UnimplementedProcesamientoTrabajadoraServer

	id          int
	miDireccion string
	nsAddr      string
	liderID     int
	soyLider    bool
	enEleccion  bool
	mu          sync.Mutex
	clienteNS   pb.NameServerClient
}

// saca la variable de entorno o usa un default si no existe
func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

// el algoritmo bully y la coordinacion que necesitamos

// responde OK al mensaje de elección y lanza la suya propia
func (s *procesadoraServer) Eleccion(ctx context.Context, req *pb.MensajeBully) (*pb.RespuestaBully, error) {
	log.Printf("[PROCESADORA-%d] Recibí mensaje ELECCIÓN de Procesadora-%d. Respondo OK.", s.id, req.GetIdOrigen())
	go s.iniciarEleccion()
	return &pb.RespuestaBully{Ok: true}, nil
}

// actualiza los datos internos cuando alguien más avisa que ganó la elección
func (s *procesadoraServer) Coordinador(ctx context.Context, req *pb.MensajeBully) (*pb.RespuestaBully, error) {
	nuevoLider := int(req.GetIdOrigen())
	s.mu.Lock()
	s.liderID = nuevoLider
	s.soyLider = (s.id == nuevoLider)
	s.enEleccion = false
	s.mu.Unlock()

	log.Printf("[PROCESADORA-%d] Reconozco como nuevo LÍDER a la Procesadora-%d", s.id, nuevoLider)
	return &pb.RespuestaBully{Ok: true}, nil
}

// responde al latido para confirmar que no se ha caído
func (s *procesadoraServer) PingLider(ctx context.Context, req *pb.PingReq) (*pb.PingRes, error) {
	return &pb.PingRes{Activo: true}, nil
}

// corazón del bully: busca procesadoras con id mayor y les tira la pelota
func (s *procesadoraServer) iniciarEleccion() {
	s.mu.Lock()
	if s.enEleccion {
		s.mu.Unlock()
		return
	}
	s.enEleccion = true
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.enEleccion = false
		s.mu.Unlock()
	}()

	log.Printf("[PROCESADORA-%d] Iniciando proceso de ELECCIÓN (Bully)...", s.id)

	ctxNS, cancelNS := context.WithTimeout(context.Background(), 3*time.Second)
	res, err := s.clienteNS.Listar(ctxNS, &pb.ListarReq{Prefijo: prefijoProc})
	cancelNS()
	if err != nil {
		log.Printf("[PROCESADORA-%d] Error al listar procesadoras en NameServer: %v", s.id, err)
		return
	}

	alguienMayorRespondio := false

	for nombre, direccion := range res.GetNombresDirecciones() {
		var idPar int
		_, errScan := fmt.Sscanf(nombre, "distrieats.procesamiento.procesadora%d", &idPar)
		if errScan != nil || idPar <= s.id {
			continue
		}

		conn, errDial := grpc.NewClient(direccion, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if errDial != nil {
			continue
		}

		cliente := pb.NewCoordinacionBullyClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, errElec := cliente.Eleccion(ctx, &pb.MensajeBully{IdOrigen: int32(s.id)})
		cancel()
		conn.Close()

		if errElec == nil && resp.GetOk() {
			alguienMayorRespondio = true
			log.Printf("[PROCESADORA-%d] Procesadora-%d (ID mayor) respondió OK. Cedo la elección.", s.id, idPar)
		}
	}

	// Si ningún nodo con ID superior respondió, me proclamo líder
	if !alguienMayorRespondio {
		s.proclamarLider(res.GetNombresDirecciones())
	}
}

// se registra como líder oficial en el nameserver y le avisa a las demás
func (s *procesadoraServer) proclamarLider(nodos map[string]string) {
	s.mu.Lock()
	s.soyLider = true
	s.liderID = s.id
	s.mu.Unlock()

	log.Printf("[PROCESADORA-%d] ¡GANÉ LA ELECCIÓN! Me proclamo LÍDER del clúster.", s.id)

	// 1. Registrar el alias oficial del líder en el NameServer para que el Broker me encuentre
	ctxReg, cancelReg := context.WithTimeout(context.Background(), 3*time.Second)
	_, err := s.clienteNS.Registrar(ctxReg, &pb.RegistroReq{
		Nombre:    aliasLider,
		Direccion: s.miDireccion,
	})
	cancelReg()
	if err != nil {
		log.Printf("[PROCESADORA-%d] Error registrando alias de líder en NameServer: %v", s.id, err)
	} else {
		log.Printf("[PROCESADORA-%d] Alias '%s' -> %s registrado en NameServer", s.id, aliasLider, s.miDireccion)
	}

	// 2. Anunciar victoria (COORDINADOR) a las demás procesadoras
	for nombre, direccion := range nodos {
		var idPar int
		_, errScan := fmt.Sscanf(nombre, "distrieats.procesamiento.procesadora%d", &idPar)
		if errScan != nil || idPar == s.id {
			continue
		}

		conn, errDial := grpc.NewClient(direccion, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if errDial != nil {
			continue
		}
		cliente := pb.NewCoordinacionBullyClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, _ = cliente.Coordinador(ctx, &pb.MensajeBully{IdOrigen: int32(s.id)})
		cancel()
		conn.Close()
	}
}

// loop infinito que chequea si el líder se cayó para levantar otra elección
func (s *procesadoraServer) vigilarLider() {
	// Pequeña espera inicial para dar tiempo a que se registren al arrancar
	time.Sleep(2 * time.Second)
	s.iniciarEleccion()

	for {
		time.Sleep(3 * time.Second)

		s.mu.Lock()
		liderActual := s.liderID
		soyLider := s.soyLider
		s.mu.Unlock()

		if soyLider {
			continue
		}

		if liderActual == 0 {
			s.iniciarEleccion()
			continue
		}

		nombreLider := fmt.Sprintf("distrieats.procesamiento.procesadora%d", liderActual)
		ctxNS, cancelNS := context.WithTimeout(context.Background(), 2*time.Second)
		resNS, errNS := s.clienteNS.Resolver(ctxNS, &pb.ResolverReq{Nombre: nombreLider})
		cancelNS()

		if errNS != nil || !resNS.GetEncontrado() {
			log.Printf("[PROCESADORA-%d] El Líder (Procesadora-%d) ya no figura en el NameServer. Iniciando elección...", s.id, liderActual)
			s.iniciarEleccion()
			continue
		}

		conn, errDial := grpc.NewClient(resNS.GetDireccion(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if errDial != nil {
			s.iniciarEleccion()
			continue
		}

		cliente := pb.NewCoordinacionBullyClient(conn)
		ctxPing, cancelPing := context.WithTimeout(context.Background(), 2*time.Second)
		_, errPing := cliente.PingLider(ctxPing, &pb.PingReq{IdOrigen: int32(s.id)})
		cancelPing()
		conn.Close()

		if errPing != nil {
			log.Printf("[PROCESADORA-%d] ¡El Líder (Procesadora-%d) no respondió al Ping! Iniciando elección...", s.id, liderActual)
			s.iniciarEleccion()
		}
	}
}

// Rol del procesador lider : recibe lotes y reparte los sub lotes

// recibe el lote gigante del broker, lo pica en partes iguales y lo reparte
func (s *procesadoraServer) EnviarLoteCrudo(ctx context.Context, req *pb.LoteReq) (*pb.LoteRes, error) {
	pedidos := req.GetPedidos()
	log.Printf("[PROCESADORA-%d] [LÍDER] Lote recibido del Broker: '%s' (Origen: %s, Total pedidos: %d)",
		s.id, req.GetLoteId(), req.GetOrigen(), len(pedidos))

	if len(pedidos) == 0 {
		return &pb.LoteRes{Exito: true}, nil
	}

	// 1. Consultar procesadoras activas en el NameServer
	ctxNS, cancelNS := context.WithTimeout(context.Background(), 3*time.Second)
	resNS, err := s.clienteNS.Listar(ctxNS, &pb.ListarReq{Prefijo: prefijoProc})
	cancelNS()

	activas := make(map[string]string)
	if err == nil && len(resNS.GetNombresDirecciones()) > 0 {
		for k, v := range resNS.GetNombresDirecciones() {
			activas[k] = v
		}
	}
	// Asegurar que el líder mismo esté considerado si el listado falló temporalmente
	miNombre := fmt.Sprintf("distrieats.procesamiento.procesadora%d", s.id)
	if len(activas) == 0 {
		activas[miNombre] = s.miDireccion
	}

	// Ordenar nombres para repartir de manera determinista
	var nombresOrdenados []string
	for nombre := range activas {
		nombresOrdenados = append(nombresOrdenados, nombre)
	}
	sort.Strings(nombresOrdenados)

	numTrabajadoras := len(nombresOrdenados)
	subLotes := make([][]*pb.Pedido, numTrabajadoras)

	// 2. Repartir pedidos equitativamente (Round-Robin)
	for i, p := range pedidos {
		idx := i % numTrabajadoras
		subLotes[idx] = append(subLotes[idx], p)
	}

	// 3. Enviar cada sub-lote a su trabajadora correspondiente
	for i, nombreTrabajadora := range nombresOrdenados {
		if len(subLotes[i]) == 0 {
			continue
		}

		direccionTrab := activas[nombreTrabajadora]
		subLoteID := fmt.Sprintf("%s_sub_%d", req.GetLoteId(), i+1)

		subReq := &pb.SubLoteReq{
			SubloteId: subLoteID,
			Origen:    req.GetOrigen(),
			Pedidos:   subLotes[i],
		}

		log.Printf("[PROCESADORA-%d] [LÍDER] Despachando sub-lote '%s' (%d pedidos) -> %s (%s)",
			s.id, subLoteID, len(subLotes[i]), nombreTrabajadora, direccionTrab)

		exito := s.enviarSubLoteATrabajadora(direccionTrab, subReq)
		if !exito {
			// Si una trabajadora se cayó justo en este milisegundo, el Líder procesa ese sub-lote
			// localmente para no abortar ni perder el lote completo.
			log.Printf("[PROCESADORA-%d] [LÍDER] Trabajadora %s no respondió. Procesando '%s' localmente por respaldo.",
				s.id, nombreTrabajadora, subLoteID)
			resLocal, errLocal := s.ProcesarSubLote(context.Background(), subReq)
			if errLocal != nil || !resLocal.GetExito() {
				return &pb.LoteRes{Exito: false}, fmt.Errorf("fallo al procesar sub-lote %s", subLoteID)
			}
		}
	}

	log.Printf("[PROCESADORA-%d] [LÍDER] Lote '%s' completado exitosamente en todas las trabajadoras.", s.id, req.GetLoteId())
	return &pb.LoteRes{Exito: true}, nil
}

// abre conexión grpc con una trabajadora y le manda su sub-lote
func (s *procesadoraServer) enviarSubLoteATrabajadora(direccion string, subReq *pb.SubLoteReq) bool {
	conn, err := grpc.NewClient(direccion, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return false
	}
	defer conn.Close()

	cliente := pb.NewProcesamientoTrabajadoraClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res, errRPC := cliente.ProcesarSubLote(ctx, subReq)
	return errRPC == nil && res != nil && res.GetExito()
}

// aca la trabajadora de encarga de la limpieza del ruido y envio al datanode

// aplica las reglas del negocio: mayúsculas, cadenas válidas y bota los cancelados o nulos
func (s *procesadoraServer) limpiarYValidarPedido(p *pb.Pedido, origen string) (*pb.PedidoProcesado, bool) {
	idPedido := strings.TrimSpace(p.GetIdPedido())
	cadena := strings.TrimSpace(p.GetCadena())
	estado := strings.TrimSpace(p.GetEstado())
	tsEmision := strings.TrimSpace(p.GetTimestampEmision())

	// Regla 1: Descartar registros con campos vacíos o NULL
	if idPedido == "" || cadena == "" || estado == "" || tsEmision == "" {
		return nil, false
	}

	// Regla 2: Normalizar ID del pedido a mayúsculas (ej. "bn-048213" -> "BN-048213")
	idPedidoNorm := strings.ToUpper(idPedido)

	// Regla 3: Validar cadena reconocida
	cadenasValidas := map[string]string{
		"burgernode":  "BurgerNode",
		"dockerpizza": "DockerPizza",
		"sushistream": "SushiStream",
		"gophertacos": "GopherTacos",
	}
	cadenaNorm, existeCadena := cadenasValidas[strings.ToLower(cadena)]
	if !existeCadena {
		return nil, false
	}

	// Regla 4: Normalizar estados y filtrar "Cancelado" o estados no reconocidos
	var estadoNorm string
	switch strings.ToLower(estado) {
	case "recibido":
		estadoNorm = "Recibido"
	case "preparando":
		estadoNorm = "Preparando"
	case "en camino":
		estadoNorm = "En Camino"
	case "entregado":
		estadoNorm = "Entregado"
	case "cancelado":
		// Los pedidos cancelados se filtran y no pasan al registro maestro
		return nil, false
	default:
		// Estado corrupto o no reconocido
		return nil, false
	}

	return &pb.PedidoProcesado{
		IdPedido:               idPedidoNorm,
		Cadena:                 cadenaNorm,
		Estado:                 estadoNorm,
		TimestampEmision:       tsEmision,
		TimestampProcesamiento: time.Now().Format("2006-01-02 15:04:05"),
		Origen:                 origen,
		Procesadora:            fmt.Sprintf("Procesadora-%d", s.id),
	}, true
}

// recibe el sub-lote crudo, lo limpia y se lo manda directo al datanode
func (s *procesadoraServer) ProcesarSubLote(ctx context.Context, req *pb.SubLoteReq) (*pb.SubLoteRes, error) {
	pedidosCrudos := req.GetPedidos()
	log.Printf("[PROCESADORA-%d] [TRABAJADORA] Procesando sub-lote '%s' (%d pedidos crudos)...",
		s.id, req.GetSubloteId(), len(pedidosCrudos))

	var limpios []*pb.PedidoProcesado
	descartados := 0

	for _, p := range pedidosCrudos {
		if limpio, ok := s.limpiarYValidarPedido(p, req.GetOrigen()); ok {
			limpios = append(limpios, limpio)
		} else {
			descartados++
		}
	}

	log.Printf("[PROCESADORA-%d] [TRABAJADORA] Sub-lote '%s' filtrado: %d válidos, %d descartados por ruido/cancelación.",
		s.id, req.GetSubloteId(), len(limpios), descartados)

	// Enviar al Datanode Central (incluso si quedaron 0 válidos o solo los válidos)
	if len(limpios) > 0 {
		if err := s.enviarADatanode(req.GetSubloteId(), limpios); err != nil {
			log.Printf("[PROCESADORA-%d] [TRABAJADORA] Error guardando en Datanode: %v", s.id, err)
			return &pb.SubLoteRes{Exito: false}, err
		}
	}

	return &pb.SubLoteRes{Exito: true}, nil
}

// pregunta al nameserver dónde está el datanode y le manda los pedidos ya procesados
func (s *procesadoraServer) enviarADatanode(subloteID string, pedidos []*pb.PedidoProcesado) error {
	// 1. Resolver dirección del Datanode en el NameServer
	ctxNS, cancelNS := context.WithTimeout(context.Background(), 3*time.Second)
	resNS, err := s.clienteNS.Resolver(ctxNS, &pb.ResolverReq{Nombre: aliasDatanode})
	cancelNS()

	if err != nil || !resNS.GetEncontrado() {
		return fmt.Errorf("datanode '%s' no encontrado en NameServer", aliasDatanode)
	}

	// 2. Conectar al Datanode y guardar el sub-lote procesado
	conn, err := grpc.NewClient(resNS.GetDireccion(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	clienteDN := pb.NewAlmacenamientoClient(conn)
	ctxDN, cancelDN := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDN()

	resDN, errRPC := clienteDN.GuardarLoteProcesado(ctxDN, &pb.GuardarReq{
		SubloteId:         subloteID,
		PedidosProcesados: pedidos,
	})

	if errRPC != nil || !resDN.GetExito() {
		return fmt.Errorf("el Datanode rechazó o falló al guardar el sub-lote: %v", errRPC)
	}

	log.Printf("[PROCESADORA-%d] [TRABAJADORA] Sub-lote '%s' guardado exitosamente en Datanode (%s)",
		s.id, subloteID, resNS.GetDireccion())
	return nil
}

// el main

func main() {
	// Permite pasar el ID por bandera (-id=1) o por variable de entorno (PROCESADORA_ID=1)
	idDefault, _ := strconv.Atoi(getEnv("PROCESADORA_ID", "1"))
	idPtr := flag.Int("id", idDefault, "ID de la procesadora (1, 2 o 3)")
	flag.Parse()

	id := *idPtr
	puertoDefault := fmt.Sprintf("5006%d", id) // 50061, 50062, 50063
	puerto := getEnv("PROCESADORA_PORT", puertoDefault)
	hostAnunciado := getEnv("PROCESADORA_HOST", "localhost")
	miDireccion := fmt.Sprintf("%s:%s", hostAnunciado, puerto)
	nsAddr := getEnv("NAMESERVER_ADDR", "localhost:50050")

	// nos conectar al Servidor de Nombres
	connNS, err := grpc.NewClient(nsAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[PROCESADORA-%d] Error preparando cliente NameServer: %v", id, err)
	}
	defer connNS.Close()
	clienteNS := pb.NewNameServerClient(connNS)

	s := &procesadoraServer{
		id:          id,
		miDireccion: miDireccion,
		nsAddr:      nsAddr,
		clienteNS:   clienteNS,
	}

	nombreEntidad := fmt.Sprintf("distrieats.procesamiento.procesadora%d", s.id)

	// Registro inicial en NameServer con reintentos
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, err := clienteNS.Registrar(ctx, &pb.RegistroReq{
			Nombre:    nombreEntidad,
			Direccion: miDireccion,
		})
		cancel()
		if err == nil {
			log.Printf("[PROCESADORA-%d] Registrada en NameServer como '%s' -> %s", s.id, nombreEntidad, miDireccion)
			break
		}
		log.Printf("[PROCESADORA-%d] Esperando al NameServer (%s)...", s.id, nsAddr)
		time.Sleep(2 * time.Second)
	}

	// Goroutine de Heartbeats (renueva su propio registro y, si es líder, también el alias del líder)
	go func() {
		for {
			time.Sleep(3 * time.Second)
			_, _ = clienteNS.RenovarLease(context.Background(), &pb.HeartbeatReq{Nombre: nombreEntidad})

			s.mu.Lock()
			esLider := s.soyLider
			s.mu.Unlock()

			if esLider {
				_, _ = clienteNS.RenovarLease(context.Background(), &pb.HeartbeatReq{Nombre: aliasLider})
			}
		}
	}()

	// levantar servidor gRPC
	lis, err := net.Listen("tcp", ":"+puerto)
	if err != nil {
		log.Fatalf("[PROCESADORA-%d] Error al escuchar en el puerto %s: %v", s.id, puerto, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterCoordinacionBullyServer(grpcServer, s)
	pb.RegisterProcesamientoLiderServer(grpcServer, s)
	pb.RegisterProcesamientoTrabajadoraServer(grpcServer, s)

	log.Printf("[PROCESADORA-%d] Escuchando peticiones gRPC en %s...", s.id, miDireccion)

	// iniciar monitoreo Bully
	go s.vigilarLider()

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("[PROCESADORA-%d] Error en servidor gRPC: %v", s.id, err)
	}
}
