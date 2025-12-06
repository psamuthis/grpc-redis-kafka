// cmd/client/main.go
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Request/Response structures matching your bridge
type PollRequest struct {
	Name string `json:"name"`
}

type PollResponse struct {
	Stock            int64   `json:"stock"`
	StationLatitude  float64 `json:"station_latitude"`
	StationLongitude float64 `json:"station_longitude"`
}

type NearRequest struct {
	CarLatitude  float64 `json:"car_latitude"`
	CarLongitude float64 `json:"car_longitude"`
	SearchRadius float64 `json:"search_radius"`
}

type StationInfo struct {
	Name             string  `json:"name"`
	Distance         float32 `json:"distance"`
	Stock            int64   `json:"stock"`
	StationLatitude  float64 `json:"station_latitude"`
	StationLongitude float64 `json:"station_longitude"`
}

type NearResponse struct {
	Stations []StationInfo `json:"stations"`
}

type UpdateRequest struct {
	StationName    string `json:"station_name"`
	StockIncrement int64  `json:"stock_increment"`
}

type UpdateResponse struct {
	Updated bool `json:"updated"`
}

// Global variables for async response handling
var (
	pollResponseChan   = make(chan PollResponse, 1)
	nearResponseChan   = make(chan NearResponse, 1)
	updateResponseChan = make(chan UpdateResponse, 1)
	errorChan          = make(chan string, 1)
)

func main() {
	// Get client ID from env or use default
	clientID := os.Getenv("CLIENT_ID")
	if clientID == "" {
		clientID = "vehicle-" + fmt.Sprintf("%d", time.Now().UnixNano())
	}

	// Connect to EMQX
	opts := mqtt.NewClientOptions()
	opts.AddBroker("tcp://emqx:1883")
	opts.SetClientID(clientID)
	opts.SetDefaultPublishHandler(defaultMessageHandler)
	opts.OnConnect = connectHandler
	opts.OnConnectionLost = connectLostHandler

	client := mqtt.NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("Failed to connect to EMQX: %v", token.Error())
	}
	defer client.Disconnect(250)

	log.Printf("Connected to EMQX broker as %s", clientID)

	// Subscribe to response topics
	subscribeToResponses(client, clientID)

	// Make requests
	makePollRequest(client, clientID, "Downtown Hub")
	makeFindNearestRequest(client, clientID)
	makeUpdateRequest(client, clientID, "Downtown Hub", 5)

	// Wait for responses
	waitForResponses()
}

func subscribeToResponses(client mqtt.Client, clientID string) {
	// Subscribe to all response topics for this client
	responseTopics := map[string]mqtt.MessageHandler{
		"station/poll/response/" + clientID:   pollResponseHandler,
		"station/near/response/" + clientID:   nearResponseHandler,
		"station/update/response/" + clientID: updateResponseHandler,
	}

	for topic, handler := range responseTopics {
		if token := client.Subscribe(topic, 1, handler); token.Wait() && token.Error() != nil {
			log.Printf("Failed to subscribe to %s: %v", topic, token.Error())
		} else {
			log.Printf("Subscribed to %s", topic)
		}
	}
}

func makePollRequest(client mqtt.Client, clientID, stationName string) {
	time.Sleep(1 * time.Second)

	log.Printf("Polling station: %s", stationName)

	request := PollRequest{Name: stationName}
	payload, err := json.Marshal(request)
	if err != nil {
		log.Printf("Failed to marshal poll request: %v", err)
		return
	}

	topic := "station/poll/request/" + clientID
	token := client.Publish(topic, 1, false, payload)
	token.Wait()

	if token.Error() != nil {
		log.Printf("Failed to publish poll request: %v", token.Error())
	} else {
		log.Printf("Published poll request to %s", topic)
	}
}

func makeFindNearestRequest(client mqtt.Client, clientID string) {
	time.Sleep(2 * time.Second)

	log.Printf("Looking for nearest station...")

	request := NearRequest{
		CarLatitude:  47.7062,
		CarLongitude: -122.3421,
		SearchRadius: 200.3,
	}

	payload, err := json.Marshal(request)
	if err != nil {
		log.Printf("Failed to marshal near request: %v", err)
		return
	}

	topic := "station/near/request/" + clientID
	token := client.Publish(topic, 1, false, payload)
	token.Wait()

	if token.Error() != nil {
		log.Printf("Failed to publish near request: %v", token.Error())
	} else {
		log.Printf("Published near request to %s", topic)
	}
}

func makeUpdateRequest(client mqtt.Client, clientID, stationName string, increment int64) {
	time.Sleep(3 * time.Second)

	log.Printf("Updating station %s stock by %d", stationName, increment)

	request := UpdateRequest{
		StationName:    stationName,
		StockIncrement: increment,
	}

	payload, err := json.Marshal(request)
	if err != nil {
		log.Printf("Failed to marshal update request: %v", err)
		return
	}

	topic := "station/update/request/" + clientID
	token := client.Publish(topic, 1, false, payload)
	token.Wait()

	if token.Error() != nil {
		log.Printf("Failed to publish update request: %v", token.Error())
	} else {
		log.Printf("Published update request to %s", topic)
	}
}

// MQTT Message Handlers
func pollResponseHandler(client mqtt.Client, msg mqtt.Message) {
	var resp PollResponse
	if err := json.Unmarshal(msg.Payload(), &resp); err != nil {
		// Check if it's an error response
		var errorResp map[string]string
		if err2 := json.Unmarshal(msg.Payload(), &errorResp); err2 == nil && errorResp["error"] != "" {
			errorChan <- fmt.Sprintf("Poll error: %s", errorResp["error"])
			return
		}
		errorChan <- fmt.Sprintf("Failed to parse poll response: %v", err)
		return
	}

	log.Printf("Poll succeeded - Stock: %d, Location: %f,%f",
		resp.Stock, resp.StationLatitude, resp.StationLongitude)
	pollResponseChan <- resp
}

func nearResponseHandler(client mqtt.Client, msg mqtt.Message) {
	var resp NearResponse
	if err := json.Unmarshal(msg.Payload(), &resp); err != nil {
		// Check if it's an error response
		var errorResp map[string]string
		if err2 := json.Unmarshal(msg.Payload(), &errorResp); err2 == nil && errorResp["error"] != "" {
			errorChan <- fmt.Sprintf("Near error: %s", errorResp["error"])
			return
		}
		errorChan <- fmt.Sprintf("Failed to parse near response: %v", err)
		return
	}

	log.Printf("Found %d nearest stations:", len(resp.Stations))
	for i, station := range resp.Stations {
		log.Printf("  %d. %s (%.1f km away, stock: %d)",
			i+1, station.Name, station.Distance, station.Stock)
	}
	nearResponseChan <- resp
}

func updateResponseHandler(client mqtt.Client, msg mqtt.Message) {
	var resp UpdateResponse
	if err := json.Unmarshal(msg.Payload(), &resp); err != nil {
		// Check if it's an error response
		var errorResp map[string]string
		if err2 := json.Unmarshal(msg.Payload(), &errorResp); err2 == nil && errorResp["error"] != "" {
			errorChan <- fmt.Sprintf("Update error: %s", errorResp["error"])
			return
		}
		errorChan <- fmt.Sprintf("Failed to parse update response: %v", err)
		return
	}

	if resp.Updated {
		log.Printf("Station stock updated successfully")
	} else {
		log.Printf("Station stock update failed")
	}
	updateResponseChan <- resp
}

func defaultMessageHandler(client mqtt.Client, msg mqtt.Message) {
	log.Printf("Received message on unexpected topic: %s", msg.Topic())
}

func connectHandler(client mqtt.Client) {
	log.Println("Connected to MQTT broker")
}

func connectLostHandler(client mqtt.Client, err error) {
	log.Printf("Connection lost: %v", err)
}

func waitForResponses() {
	timeout := time.After(30 * time.Second)
	responsesReceived := 0

	for responsesReceived < 3 {
		select {
		case <-pollResponseChan:
			responsesReceived++
		case <-nearResponseChan:
			responsesReceived++
		case <-updateResponseChan:
			responsesReceived++
		case err := <-errorChan:
			log.Printf("Error: %s", err)
		case <-timeout:
			log.Println("Timeout waiting for responses")
			return
		}
	}

	log.Println("All responses received successfully")
}
