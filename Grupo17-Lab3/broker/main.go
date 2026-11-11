package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	pb "distrieats/proto"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const aliasLider = "distrieats.procesamiento.lider"

// esto representa lo mandado por el clientey extractor
type EventoJSON struct {
	IdPedido         string `json:"id_pedido"`
	Cadena           string `json:"cadena"`
	Estado           string `json:"estado"`
	TimestampEmision string `json:"timestamp_emision"`
}

// envio de lotes y coordinacion lider
type Broker struct {
	nsAddr        string
	mu            sync.Mutex
	cachedLeader  string
	contadorLotes uint64
	timeoutGRPC   time.Duration
	esperaReenvio time.Duration
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

// conexion a rabbit mq
func conectarRabbitMQ(rabbitURL string) *amqp.Connection {
	for {
		conn, err := amqp.Dial(rabbitURL)
		if err == nil {
			log.Println("[BROKER] Conectado exitosamente a RabbitMQ")
			return conn
		}
		log.Printf("[BROKER] Esperando a RabbitMQ (%v)... reintentando en 3s", err)
		time.Sleep(3 * time.Second)
	}
}

// registrar el Broker en el NameServer y mantenerlo vivo
func registrarBrokerEnNameServer(nsAddr string) {
	conn, err := grpc.NewClient(nsAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[BROKER] No se pudo conectar al NameServer para registro: %v", err)
	}

	client := pb.NewNameServerClient(conn)
	nombreEntidad := "distrieats.logistica.broker"
	direccionFisica := "Broker-MV2 (Solo Cliente)"

	_, err = client.Registrar(context.Background(), &pb.RegistroReq{
		Nombre:    nombreEntidad,
		Direccion: direccionFisica,
	})

	if err != nil {
		log.Fatalf("[BROKER] Error al registrarse en el NameServer: %v", err)
	}
	log.Printf("[BROKER] Registrado exitosamente en el NameServer como '%s'", nombreEntidad)

	// aca indicamos que
	go func() {
		for {
			time.Sleep(3 * time.Second)
			_, err := client.RenovarLease(context.Background(), &pb.HeartbeatReq{Nombre: nombreEntidad})
			if err != nil {
				log.Printf("[BROKER] Advertencia: Fallo enviando heartbeat al NameServer: %v", err)
			}
		}
	}()
}

// obtenerDireccionLider devuelve la dirección en caché o consulta al Servidor de Nombres
func (b *Broker) obtenerDireccionLider() string {
	b.mu.Lock()
	dir := b.cachedLeader
	b.mu.Unlock()

	if dir != "" {
		return dir
	}

	for {
		conn, err := grpc.NewClient(b.nsAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			client := pb.NewNameServerClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), b.timeoutGRPC)
			res, errRPC := client.Resolver(ctx, &pb.ResolverReq{Nombre: aliasLider})
			cancel()
			conn.Close()

			if errRPC == nil && res.GetEncontrado() && res.GetDireccion() != "" {
				nuevaDir := res.GetDireccion()
				b.mu.Lock()
				b.cachedLeader = nuevaDir
				b.mu.Unlock()
				log.Printf("[BROKER] Líder resuelto en NameServer: '%s' -> %s", aliasLider, nuevaDir)
				return nuevaDir
			}
		}

		log.Printf("[BROKER] Alias '%s' aún no disponible en NameServer (%s). Reintentando en %v...",
			aliasLider, b.nsAddr, b.esperaReenvio)
		time.Sleep(b.esperaReenvio)
	}
}

// aca se borra la dirección si coinciden, forzando una nueva resolución
func (b *Broker) invalidarCacheLider(dirFallida string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cachedLeader == dirFallida {
		log.Printf("[BROKER] Invalidando caché del líder caído/inaccesible (%s)", dirFallida)
		b.cachedLeader = ""
	}
}

// aca se garantisa que el lote no se descarte nunca ante fallos del líder
func (b *Broker) enviarLoteConfiable(lote *pb.LoteReq) {
	intento := 1
	for {
		dirLider := b.obtenerDireccionLider()

		conn, err := grpc.NewClient(dirLider, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("[BROKER] [REENVIO] Intento %d fallido al preparar conexión con líder %s para lote %s: %v",
				intento, dirLider, lote.GetLoteId(), err)
			b.invalidarCacheLider(dirLider)
			time.Sleep(b.esperaReenvio)
			intento++
			continue
		}

		client := pb.NewProcesamientoLiderClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), b.timeoutGRPC)

		if intento == 1 {
			log.Printf("[BROKER] Enviando lote '%s' (Origen: %s, Pedidos: %d) al líder %s",
				lote.GetLoteId(), lote.GetOrigen(), len(lote.GetPedidos()), dirLider)
		} else {
			log.Printf("[BROKER] [REENVIO] Reenviando lote '%s' (Intento %d, Origen: %s, Pedidos: %d) al líder %s",
				lote.GetLoteId(), intento, lote.GetOrigen(), len(lote.GetPedidos()), dirLider)
		}

		res, errRPC := client.EnviarLoteCrudo(ctx, lote)
		cancel()
		conn.Close()

		if errRPC == nil && res != nil && res.GetExito() {
			log.Printf("[BROKER] Lote '%s' confirmado exitosamente por el líder %s", lote.GetLoteId(), dirLider)
			return
		}

		log.Printf("[BROKER] [FALLO LIDER] Error o rechazo al enviar lote '%s' a %s (err: %v). Esperando reelección...",
			lote.GetLoteId(), dirLider, errRPC)
		b.invalidarCacheLider(dirLider)
		time.Sleep(b.esperaReenvio)
		intento++
	}
}

// aca con esta funcion lo que hacemos es agrupar los eventos en un intervalo de tiempo o tamaño y luego confirma a nuestro rabbitmq
func (b *Broker) consumirCola(conn *amqp.Connection, nombreCola, origen string, tamanoLote int, timeoutLote time.Duration) {
	for {
		ch, err := conn.Channel()
		if err != nil {
			log.Printf("[BROKER] Error abriendo canal para '%s': %v. Reintentando en 3s...", nombreCola, err)
			time.Sleep(3 * time.Second)
			continue
		}

		_, err = ch.QueueDeclare(
			nombreCola,
			true,  // durable
			false, // delete when unused
			false, // exclusive
			false, // no-wait
			nil,   // arguments
		)
		if err != nil {
			log.Printf("[BROKER] Error declarando cola '%s': %v. Reintentando en 3s...", nombreCola, err)
			ch.Close()
			time.Sleep(3 * time.Second)
			continue
		}

		// Limitar mensajes sin ACK en vuelo a 2 lotes para control de memoria
		_ = ch.Qos(tamanoLote*2, 0, false)

		msgs, err := ch.Consume(
			nombreCola,
			"",    // consumer tag
			false, // auto-ack = false (Confirmación manual después del éxito del líder)
			false, // exclusive
			false, // no-local
			false, // no-wait
			nil,   // args
		)
		if err != nil {
			log.Printf("[BROKER] Error suscribiéndose a '%s': %v. Reintentando en 3s...", nombreCola, err)
			ch.Close()
			time.Sleep(3 * time.Second)
			continue
		}

		log.Printf("[BROKER] Escuchando cola '%s' (Origen: %s | Umbral: %d msgs o %v)",
			nombreCola, origen, tamanoLote, timeoutLote)

		var bufferPedidos []*pb.Pedido
		var bufferDeliveries []amqp.Delivery

		timer := time.NewTimer(timeoutLote)
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timerActivo := false

		despacharLote := func() {
			if len(bufferPedidos) == 0 {
				return
			}

			numLote := atomic.AddUint64(&b.contadorLotes, 1)
			loteID := fmt.Sprintf("lote_%s_%d", origen, numLote)

			loteReq := &pb.LoteReq{
				LoteId:  loteID,
				Origen:  origen,
				Pedidos: bufferPedidos,
			}

			// Bloquea hasta que el líder (actual o nuevo tras reelección) confirme persistencia en Datanode
			b.enviarLoteConfiable(loteReq)

			// Confirmar manualmente en RabbitMQ cada mensaje del lote aceptado
			for _, d := range bufferDeliveries {
				if errAck := d.Ack(false); errAck != nil {
					log.Printf("[BROKER] Advertencia al hacer ACK en '%s': %v", nombreCola, errAck)
				}
			}

			bufferPedidos = nil
			bufferDeliveries = nil
			if timerActivo {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timerActivo = false
			}
		}

		canalCerrado := false
		for !canalCerrado {
			select {
			case d, ok := <-msgs:
				if !ok {
					log.Printf("[BROKER] Canal de la cola '%s' cerrado. Reabriendo...", nombreCola)
					canalCerrado = true
					break
				}

				var ev EventoJSON
				if err := json.Unmarshal(d.Body, &ev); err != nil {
					log.Printf("[BROKER] Mensaje malformado en '%s' (%v), descartando con Ack", nombreCola, err)
					_ = d.Ack(false)
					continue
				}

				// NUEVO: Mostramos en consola cada vez que el Broker extrae un mensaje de RabbitMQ
				log.Printf("[BROKER] <- Evento recibido de RabbitMQ (%s) | ID: %s | Estado: %s", origen, ev.IdPedido, ev.Estado)

				bufferPedidos = append(bufferPedidos, &pb.Pedido{
					IdPedido:         ev.IdPedido,
					Cadena:           ev.Cadena,
					Estado:           ev.Estado,
					TimestampEmision: ev.TimestampEmision,
				})
				bufferDeliveries = append(bufferDeliveries, d)

				// Iniciar el temporizador cuando llega el primer mensaje del lote
				if len(bufferPedidos) == 1 {
					timer.Reset(timeoutLote)
					timerActivo = true
				}

				// Si alcanzamos el tamaño máximo de lote, enviamos de inmediato
				if len(bufferPedidos) >= tamanoLote {
					despacharLote()
				}

			case <-timer.C:
				timerActivo = false
				if len(bufferPedidos) > 0 {
					log.Printf("[BROKER] Timeout de lote (%v) alcanzado para '%s'. Enviando %d mensajes acumulados.",
						timeoutLote, origen, len(bufferPedidos))
					despacharLote()
				}
			}
		}

		ch.Close()
		time.Sleep(2 * time.Second)
	}
}

// Nuestro main donde comienza todo el flujo del trabajo
func main() {
	rabbitURL := getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
	nsAddr := getEnv("NAMESERVER_ADDR", "localhost:50050")
	colaPedidos := getEnv("RABBITMQ_QUEUE_PEDIDOS", "pedidos_queue")
	colaHistorico := getEnv("RABBITMQ_QUEUE_HISTORICO", "historico_queue")

	tamanoLote, err := strconv.Atoi(getEnv("TAMANO_LOTE", "10"))
	if err != nil || tamanoLote <= 0 {
		tamanoLote = 10
	}

	timeoutLoteSeg, err := strconv.Atoi(getEnv("TIMEOUT_LOTE_SEG", "10"))
	if err != nil || timeoutLoteSeg <= 0 {
		timeoutLoteSeg = 10
	}

	timeoutGRPCSeg, err := strconv.Atoi(getEnv("GRPC_TIMEOUT_SEG", "15"))
	if err != nil || timeoutGRPCSeg <= 0 {
		timeoutGRPCSeg = 15
	}

	esperaReenvioSeg, err := strconv.Atoi(getEnv("RETRY_WAIT_SEG", "3"))
	if err != nil || esperaReenvioSeg <= 0 {
		esperaReenvioSeg = 3
	}

	// Aca registramos nuestro boker a la tabla de nombres
	registrarBrokerEnNameServer(nsAddr)

	broker := &Broker{
		nsAddr:        nsAddr,
		timeoutGRPC:   time.Duration(timeoutGRPCSeg) * time.Second,
		esperaReenvio: time.Duration(esperaReenvioSeg) * time.Second,
	}

	conn := conectarRabbitMQ(rabbitURL)
	defer conn.Close()

	var wg sync.WaitGroup
	wg.Add(2)

	// Consumidor para flujo en Tiempo Real (cleinte)
	go func() {
		defer wg.Done()
		broker.consumirCola(conn, colaPedidos, "TIEMPO_REAL", tamanoLote, time.Duration(timeoutLoteSeg)*time.Second)
	}()

	// Consumidor para flujo Histórico (extractor)
	go func() {
		defer wg.Done()
		broker.consumirCola(conn, colaHistorico, "HISTORICO", tamanoLote, time.Duration(timeoutLoteSeg)*time.Second)
	}()

	wg.Wait()
}
