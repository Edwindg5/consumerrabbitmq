package main

import (
	"bytes"
	"log"
	"net/http"
	"time"

	"github.com/streadway/amqp"
)

func main() {

	rabbitMQURL := "amqp://admin:admin@52.7.35.94:5672/"

	// Conectar a RabbitMQ
	conn, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		log.Fatalf("❌ Error al conectar con RabbitMQ: %s", err)
	}
	defer conn.Close()

	// Crear un canal
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("❌ Error al abrir un canal: %s", err)
	}
	defer ch.Close()

	
	_, err = ch.QueueDeclare(
		"pedidos_queue",
		true, 
		false, 
		false, 
		false, 
		nil,   
	)
	if err != nil {
		log.Fatalf("❌ Error al declarar la cola 'pedidos_queue': %s", err)
	}

	// Consumir mensajes de `pedidos_queue`
	msgs, err := ch.Consume(
		"pedidos_queue",
		"",    
		true,  
		false, 
		false, 
		false, 
		nil,   
	)
	if err != nil {
		log.Fatalf("❌ Error al consumir mensajes de 'pedidos_queue': %s", err)
	}

	log.Println("📡 Esperando mensajes de RabbitMQ...")


	for msg := range msgs {
		log.Printf("📩 Pedido recibido: %s", msg.Body)

		// Simulación de proceso de seguridad
		log.Println("🔍 Procesando seguridad...")
		time.Sleep(2 * time.Second) // Simula un chequeo de seguridad

		
		sendToNotificationsAPI(msg.Body)
	}
}


func sendToNotificationsAPI(data []byte) {
	apiURL := "http://localhost:8081"

	resp, err := http.Post(apiURL+"/procesar", "application/json", bytes.NewBuffer(data))
	if err != nil {
		log.Println("❌ Error al enviar mensaje a api-notifications:", err)
		return
	}
	defer resp.Body.Close()

	log.Println("✅ Notificación enviada correctamente a api-notifications")
}
