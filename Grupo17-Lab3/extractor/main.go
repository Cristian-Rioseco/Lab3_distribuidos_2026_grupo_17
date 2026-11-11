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

// saca la variable de entorno o usa un default si no existe
func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

// conecta a postgres, si falla se queda en loop intentando
func conectarPostgres() *sql.DB {
	host := getEnv("DB_HOST", "localhost")
	port := getEnv("DB_PORT", "5432")
	//Credenciales
	user := getEnv("DB_USER", "distrieats_lector")
	pass := getEnv("DB_PASS", "lector_lab3_2026")
	dbname := getEnv("DB_NAME", "distrieats")

	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, pass, dbname)

	for {
		db, err := sql.Open("postgres", dsn)
		if err == nil {
			if err = db.Ping(); err == nil {
				log.Println("[Extractor] conectado exitosamente a postgreSQL")
				return db
			}
			db.Close()
		}
		log.Printf("[Extractor] Esperando a postgreSQL (%v)... reintentando", err)
		time.Sleep(3 * time.Second)
	}
}

// levanta la conexion a rabbitmq y devuelve el canal para publicar
func conectarRabbitMQ() (*amqp.Connection, *amqp.Channel) {
	rabbitURL := getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")

	for {
		conn, err := amqp.Dial(rabbitURL)
		if err == nil {
			ch, err := conn.Channel()
			if err == nil {
				log.Println("[Extractor] conectado exitosamente a rabbitmq")
				return conn, ch
			}
			conn.Close()
		}
		log.Printf("[Extractor] esperando a rabbitmq (%v)... reintentando", err)
		time.Sleep(3 * time.Second)
	}
}

func main() {
	tabla := getEnv("DB_TABLE_HISTORICO", "pedidos_historicos")
	cola := getEnv("RABBITMQ_QUEUE_HISTORICO", "historico_queue")

	tamanoPagina, err := strconv.Atoi(getEnv("TAMANO_PAGINA", "1000"))
	if err != nil || tamanoPagina <= 0 {
		tamanoPagina = 1000
	}

	pausaMilisegundos, err := strconv.Atoi(getEnv("PAUSA_PAGINA_MS", "500"))
	if err != nil || pausaMilisegundos < 0 {
		pausaMilisegundos = 500
	}

	//conecto servicios
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
		log.Fatalf("[Extractor] error declarando cola %s: %v", cola, err)
	}

	ultimoID := 0
	paginasProcesadas := 0
	totalExtraido := 0

	log.Printf("[Extractor] iniciando extraccion desde '%s' hacia '%s' (bloques de %d)", tabla, cola, tamanoPagina)

	for {
		query := fmt.Sprintf(`
            SELECT id, id_pedido, cadena, estado, timestamp_emision::text
            FROM %s
            WHERE id > $1
            ORDER BY id ASC
            LIMIT $2`, tabla)

		rows, err := db.Query(query, ultimoID, tamanoPagina)
		if err != nil {
			log.Printf("[Extractor] error consultando base de datos: %v. volviendo a intentar", err)
			time.Sleep(3 * time.Second)
			continue
		}

		registrosEnBloque := 0

		for rows.Next() {
			var id int
			var idPedido, cadena, estado, tsEmision sql.NullString

			if err := rows.Scan(&id, &idPedido, &cadena, &estado, &tsEmision); err != nil {
				log.Printf("[Extractor] error escaneando fila: %v", err)
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
				log.Printf("[Extractor] error serializando evento id %d: %v", id, err)
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

				log.Printf("[Extractor] error publicando en rabbitmq (%v). reconectando...", err)
				ch.Close()
				conn.Close()
				conn, ch = conectarRabbitMQ()
			}

			ultimoID = id
			registrosEnBloque++
			totalExtraido++
		}
		rows.Close()

		if registrosEnBloque == 0 {
			log.Printf("[Extractor] fin de tabla alcanzadom, total de registros historicos: %d", totalExtraido)
			break
		}

		paginasProcesadas++
		log.Printf("[Extractor] bloque %d procesado (%d registros), ultimo id procesado: %d. Pausando %d ms...",
			paginasProcesadas, registrosEnBloque, ultimoID, pausaMilisegundos)

		// pa que no se colapse el rabbitmq
		time.Sleep(time.Duration(pausaMilisegundos) * time.Millisecond)
	}
}
