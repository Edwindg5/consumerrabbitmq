// consumer/consumer.go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"


	"github.com/streadway/amqp"
)

type Pedido struct {
	ID       int    `json:"id"`
	Cliente  string `json:"cliente"`
	Producto string `json:"producto"`
	Cantidad int    `json:"cantidad"`
	Estado   string `json:"estado"`
}

func main() {
	rabbitMQURL := "amqp://admin:admin@52.7.35.94:5672/"
	conn, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		log.Fatalf("❌ Error al conectar con RabbitMQ: %s", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("❌ Error al abrir canal: %s", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"pedido_enviado",
		true,  
		false, 
		false, 
		false, 
		nil,   
	)
	if err != nil {
		log.Fatalf("❌ Error al declarar la cola: %s", err)
	}

	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("❌ Error al consumir mensajes: %s", err)
	}

	log.Println("📥 Esperando pedidos en la cola 'pedido_enviado'...")

	for d := range msgs {
		var pedido Pedido
		err := json.Unmarshal(d.Body, &pedido)
		if err != nil {
			log.Println("❌ Error al deserializar el mensaje:", err)
			continue
		}

		log.Printf("📦 Pedido recibido: %+v", pedido)

		// **Actualizar estado en API 1**
		err = actualizarEstadoEnAPI1(pedido)
		if err != nil {
			log.Println("❌ Error al actualizar en API 1:", err)
		}
	}
}

func actualizarEstadoEnAPI1(pedido Pedido) error {
    api1URL := "http://localhost:8080/pedidos/actualizar" // Nueva ruta para actualizar sin ID
    data, _ := json.Marshal(map[string]interface{}{
        "producto": pedido.Producto,
        "cantidad": pedido.Cantidad,
        "estado":   "procesado",
    })

    req, err := http.NewRequest("PUT", api1URL, bytes.NewBuffer(data))
    if err != nil {
        return err
    }

    req.Header.Set("Content-Type", "application/json")
    client := &http.Client{}
    resp, err := client.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        return fmt.Errorf("error en API 1: %s", resp.Status)
    }

    log.Printf("✅ Pedido con producto '%s' y cantidad %d actualizado en API 1 a estado 'procesado'", pedido.Producto, pedido.Cantidad)
    return nil
}
