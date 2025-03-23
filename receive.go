package main

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/streadway/amqp"
)

func main() {
	rabbitMQURL := "amqp://admin:admin@52.7.35.94:5672/"
	conn, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		log.Fatalf("❌ Error al conectar con RabbitMQ: %s", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("❌ Error al abrir un canal: %s", err)
	}
	defer ch.Close()

	_, err = ch.QueueDeclare("pedidos_queue", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("❌ Error al declarar la cola 'pedidos_queue': %s", err)
	}

	msgs, err := ch.Consume("pedidos_queue", "", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("❌ Error al consumir mensajes de 'pedidos_queue': %s", err)
	}

	go func() {
		for msg := range msgs {
			log.Printf("📩 Pedido recibido: %s", msg.Body)

			if err := enviarNotificacionAPI(msg.Body); err != nil {
				log.Printf("❌ Error al enviar la transacción a API1: %s", err)
			} else {
				log.Println("✅ Transacción enviada correctamente a API1")
			}
		}
	}()

	select {}
}

func enviarNotificacionAPI(data []byte) error {
	type Notificacion struct {
		PedidoID  int    `json:"pedido_id"`
		Cliente   string `json:"cliente"`
		Producto  string `json:"producto"`
		Cantidad  int    `json:"cantidad"`
		Estado    string `json:"estado"`
		Fecha     string `json:"fecha"`
	}

	var pedido Notificacion
	if err := json.Unmarshal(data, &pedido); err != nil {
		log.Println("❌ Error al convertir mensaje a notificación:", err)
		return err
	}

	// 🔍 Validar PedidoID
	if pedido.PedidoID <= 0 {
		log.Println("⚠️ PedidoID inválido, asignando valor por defecto")
		pedido.PedidoID = 1
	}

	// 🔍 Validar Cliente
	if pedido.Cliente == "" {
		log.Println("⚠️ Cliente vacío, asignando 'Desconocido'")
		pedido.Cliente = "Desconocido"
	}

	pedido.Fecha = time.Now().Format("2006-01-02 15:04:05")

	jsonData, err := json.Marshal(pedido)
	if err != nil {
		log.Println("❌ Error al convertir notificación a JSON:", err)
		return err
	}

	api1NotificacionURL := "http://34.199.34.207:8080/notificaciones"

	// 🔍 Imprimir datos antes de enviarlos
	log.Println("📤 Datos enviados a API1:", string(jsonData))

	resp, err := http.Post(api1NotificacionURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Println("❌ Error en la solicitud HTTP:", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		log.Printf("⚠️ API1 respondió con código %d\n", resp.StatusCode)
		return err
	}

	return nil
}
