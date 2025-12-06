// cmd/mqtt-grpc-bridge/main.go
package main

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"
	"os"

	"github.com/eclipse/paho.mqtt.golang"
	pb "github.com/psamuthis/grpc-station/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Connect to EMQX
	//time.Sleep(10 * time.Second)
	mqttOpts := mqtt.NewClientOptions()
	mqttOpts.AddBroker(os.Getenv("MQTT_BROKER"))
	mqttOpts.SetClientID(os.Getenv("GRPC_SERVER"))
	
	mqttClient := mqtt.NewClient(mqttOpts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("MQTT connection failed: %v", token.Error())
	}
	
	grpcConn, err := grpc.NewClient(os.Getenv("GRPC_SERVER"),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("gRPC connection failed: %v", err)
	}
	defer grpcConn.Close()
	
	grpcClient := pb.NewStationClient(grpcConn)
	
	// Subscribe to MQTT topics
	token := mqttClient.Subscribe("station/+/request/#", 1, func(client mqtt.Client, msg mqtt.Message) {
		go handleMessage(msg, grpcClient, mqttClient)
	})
	token.Wait()
	
	log.Println("MQTT-gRPC bridge running...")
	select {} // Keep running
}

func handleMessage(msg mqtt.Message, grpcClient pb.StationClient, mqttClient mqtt.Client) {
	topic := msg.Topic()
	parts := strings.Split(topic, "/")
	if len(parts) < 4 {
		log.Printf("Invalid topic format: %s", topic)
		return
	}
	
	operation := parts[1]    // "poll", "near", or "update"
	clientID := parts[3]     // The client ID from the topic
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	var response interface{}
	var err error
	
	switch operation {
	case "poll":
		response, err = handlePoll(ctx, grpcClient, msg.Payload())
	case "near":
		response, err = handleNear(ctx, grpcClient, msg.Payload())
	case "update":
		response, err = handleUpdate(ctx, grpcClient, msg.Payload())
	default:
		log.Printf("Unknown operation: %s", operation)
		return
	}
	
	// Send response back via MQTT
	responseTopic := "station/" + operation + "/response/" + clientID
	if err != nil {
		sendError(mqttClient, responseTopic, err)
	} else {
		sendResponse(mqttClient, responseTopic, response)
	}
}

func handlePoll(ctx context.Context, client pb.StationClient, payload []byte) (interface{}, error) {
	var req struct{ Name string `json:"name"` }
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, err
	}
	
	resp, err := client.PollStation(ctx, &pb.PollStationRequest{Name: req.Name})
	if err != nil {
		return nil, err
	}
	
	return map[string]interface{}{
		"stock":             resp.GetStock(),
		"station_latitude":  resp.GetStationLatitude(),
		"station_longitude": resp.GetStationLongitude(),
	}, nil
}

func handleNear(ctx context.Context, client pb.StationClient, payload []byte) (interface{}, error) {
	var req struct {
		CarLatitude  float64 `json:"car_latitude"`
		CarLongitude float64 `json:"car_longitude"`
		SearchRadius float64 `json:"search_radius"`
	}
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, err
	}
	
	resp, err := client.FindNearestStation(ctx, &pb.NearStationRequest{
		CarLatitude:  req.CarLatitude,
		CarLongitude: req.CarLongitude,
		SearchRadius: req.SearchRadius,
	})
	if err != nil {
		return nil, err
	}
	
	stations := make([]map[string]interface{}, len(resp.GetStations()))
	for i, s := range resp.GetStations() {
		stations[i] = map[string]interface{}{
			"name":              s.GetName(),
			"distance":          s.GetDistance(),
			"stock":             s.GetStock(),
			"station_latitude":  s.GetStationLatitude(),
			"station_longitude": s.GetStationLongitude(),
		}
	}
	
	return map[string]interface{}{"stations": stations}, nil
}

func handleUpdate(ctx context.Context, client pb.StationClient, payload []byte) (interface{}, error) {
	var req struct {
		StationName    string `json:"station_name"`
		StockIncrement int64  `json:"stock_increment"`
	}
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, err
	}
	
	resp, err := client.UpdateStationStock(ctx, &pb.UpdateStationRequest{
		StationName:    req.StationName,
		StockIncrement: req.StockIncrement,
	})
	if err != nil {
		return nil, err
	}
	
	return map[string]interface{}{"updated": resp.GetUpdated()}, nil
}

func sendResponse(client mqtt.Client, topic string, data interface{}) {
	payload, _ := json.Marshal(data)
	token := client.Publish(topic, 1, false, payload)
	token.Wait()
}

func sendError(client mqtt.Client, topic string, err error) {
	payload, _ := json.Marshal(map[string]string{"error": err.Error()})
	token := client.Publish(topic, 1, false, payload)
	token.Wait()
}