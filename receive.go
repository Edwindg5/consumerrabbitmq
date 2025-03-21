// receive.go
package main

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/streadway/amqp"
)

var (
	clients   = make(map[*websocket.Conn]bool)
	clientsMu sync.Mutex
	upgrader  = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
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

	http.HandleFunc("/ws", handleConnections)
	go func() {
		log.Println("📡 Servidor WebSocket escuchando en :8081")
		log.Fatal(http.ListenAndServe(":8081", nil))
	}()

	go func() {
		for msg := range msgs {
			log.Printf("📩 Pedido recibido: %s", msg.Body)

			broadcastMessage(msg.Body)

			if err := enviarNotificacionAPI(msg.Body); err != nil {
				log.Printf("❌ Error al enviar la transacción a API1: %s", err)
			} else {
				log.Println("✅ Transacción enviada correctamente a API1")
			}
		}
	}()

	select {}
}

func handleConnections(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("❌ Error al actualizar a WebSocket:", err)
		return
	}

	clientsMu.Lock()
	clients[ws] = true
	clientsMu.Unlock()

	log.Println("✅ Cliente conectado")

	for {
		if _, _, err := ws.ReadMessage(); err != nil {
			log.Println("❌ Cliente desconectado")

			clientsMu.Lock()
			delete(clients, ws)
			clientsMu.Unlock()

			ws.Close()
			break
		}
	}
}

func broadcastMessage(message []byte) {
	clientsMu.Lock()
	defer clientsMu.Unlock()

	for client := range clients {
		err := client.WriteMessage(websocket.TextMessage, message)
		if err != nil {
			log.Println("❌ Error al enviar mensaje:", err)
			client.Close()
			delete(clients, client)
		}
	}
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
