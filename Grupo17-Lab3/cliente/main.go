package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
)

type EventoPedido struct {
	IdPedido         string `json:"id_pedido"`
	Cadena           string `json:"cadena"`
	Estado           string `json:"estado"`
	TimestampEmision string `json:"timestamp_emision"`
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func conectarPostgres() *sql.DB {
	host := getEnv("DB_HOST", "localhost")
	port := getEnv("DB_PORT", "5432")

	user := getEnv("DB_USER", "distrieats_lector")
	pass := getEnv("DB_PASS", "lector_lab3_2026")
	dbname := getEnv("DB_NAME", "distrieats")

	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, pass, dbname)

	for {
		db, err := sql.Open("postgres", dsn)
		if err == nil {
			if err = db.Ping(); err == nil {
				log.Println("[CLIENTE] Conectado exitosamente a PostgreSQL")
				return db
			}
			db.Close()
		}
		log.Printf("[CLIENTE] Esperando a PostgreSQL (%v)... reintentando en 3s", err)
		time.Sleep(3 * time.Second)
	}
}

// aca conectamos a rabbitmq
func conectarRabbitMQ() (*amqp.Connection, *amqp.Channel) {
	rabbitURL := getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")

	for {
		conn, err := amqp.Dial(rabbitURL)
		if err == nil {
			ch, err := conn.Channel()
			if err == nil {
				log.Println("[CLIENTE] Conectado exitosamente a RabbitMQ")
				return conn, ch
			}
			conn.Close()
		}
		log.Printf("[CLIENTE] Esperando a RabbitMQ (%v)... reintentando en 3s", err)
		time.Sleep(3 * time.Second)
	}
}

func main() {
	tabla := getEnv("DB_TABLE_TIEMPO_REAL", "pedidos_tiempo_real")
	cola := getEnv("RABBITMQ_QUEUE_PEDIDOS", "pedidos_queue")
	intervaloSeg, err := strconv.Atoi(getEnv("INTERVALO_EMISION_SEG", "10"))
	if err != nil || intervaloSeg <= 0 {
		intervaloSeg = 10
	}

	db := conectarPostgres()
	defer db.Close()

	conn, ch := conectarRabbitMQ()
	defer conn.Close()
	defer ch.Close()

	_, err = ch.QueueDeclare(
		cola,  // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		log.Fatalf("[CLIENTE] Error declarando cola %s: %v", cola, err)
	}

	ultimoID := 0
	loteLectura := 100

	log.Printf("[CLIENTE] Iniciando emisión desde '%s' hacia '%s' cada %ds", tabla, cola, intervaloSeg)

	for {
		query := fmt.Sprintf(`
			SELECT id, id_pedido, cadena, estado, timestamp_emision::text
			FROM %s
			WHERE id > $1
			ORDER BY id ASC
			LIMIT $2`, tabla)

		rows, err := db.Query(query, ultimoID, loteLectura)
		if err != nil {
			log.Printf("[CLIENTE] Error consultando BD: %v. Reintentando en 3s...", err)
			time.Sleep(3 * time.Second)
			continue
		}

		registrosEncontrados := 0
		for rows.Next() {
			var id int
			var idPedido, cadena, estado, tsEmision sql.NullString

			if err := rows.Scan(&id, &idPedido, &cadena, &estado, &tsEmision); err != nil {
				log.Printf("[CLIENTE] Error escaneando fila: %v", err)
				continue
			}

			evento := EventoPedido{
				IdPedido:         idPedido.String,
				Cadena:           cadena.String,
				Estado:           estado.String,
				TimestampEmision: tsEmision.String,
			}

			body, err := json.Marshal(evento)
			if err != nil {
				log.Printf("[CLIENTE] Error serializando evento ID %d: %v", id, err)
				continue
			}

			for {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err = ch.PublishWithContext(
					ctx,
					"",    // exchange
					cola,  // routing key
					false, // mandatory
					false, // immediate
					amqp.Publishing{
						ContentType:  "application/json",
						DeliveryMode: amqp.Persistent,
						Body:         body,
					},
				)
				cancel()

				if err == nil {
					break
				}
				log.Printf("[CLIENTE] Error publicando en RabbitMQ (%v). Reconectando...", err)
				ch.Close()
				conn.Close()
				conn, ch = conectarRabbitMQ()
			}

			log.Printf("[CLIENTE] Evento publicado (ID BD: %d) -> Pedido: '%s' | Cadena: '%s' | Estado: '%s'",
				id, evento.IdPedido, evento.Cadena, evento.Estado)

			ultimoID = id
			registrosEncontrados++
			time.Sleep(time.Duration(intervaloSeg) * time.Second)
		}
		rows.Close()

		if registrosEncontrados == 0 {
			log.Println("[CLIENTE] Se alcanzó el final de la tabla pedidos_tiempo_real. Finalizando Cliente Emisor.")
			break
		}
	}
}
