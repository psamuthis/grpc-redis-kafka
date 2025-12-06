package main

import (
	"context"
	"log"
	"time"

	pb "github.com/psamuthis/grpc-station/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient("api:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewStationClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	log.Printf("Polling for stock")
	station_data, err := client.PollStation(ctx, &pb.PollStationRequest{Name: "University Campus"})
	if err != nil {
		log.Fatalf("Stock query failed: %v", err)
	}
	log.Printf("University Campus currently holds %v cyliders of hydrogen.", station_data.GetStock())

	log.Printf("Station about to sell 2 cylinders.")
	resp, err := client.UpdateStationStock(ctx, &pb.UpdateStationRequest{StationName: "University Campus", StockIncrement: -2})
	if err != nil {
		log.Fatalf("Stock update failed: %v", err)
	}

	log.Printf("Stock update succeeded? %v", resp.GetUpdated())

	updated_station_data, err := client.PollStation(ctx, &pb.PollStationRequest{Name: "University Campus"})
	if err != nil {
		log.Fatalf("Stock query failed: %v", err)
	}
	log.Printf("Station now holds %v cylinders of hydrogen.", updated_station_data.GetStock())
}