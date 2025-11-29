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

	log.Printf("Vehicle about to poll Downtown Hub station.")
	resp, err := client.PollStation(ctx, &pb.PollStationRequest{Name: "Downtown Hub"})
	if err != nil {
		log.Fatalf("Poll failed: %v", err)
	}
	log.Printf("Polled succeeded stock: %d, location: %f,%f", resp.GetStock(), resp.GetStationLatitude(), resp.GetStationLongitude())

	log.Printf("Vehicle looking for nearest station.")
	near_stations_resp, err := client.FindNearestStation(ctx, &pb.NearStationRequest{
		CarLatitude: 47.7062,
		CarLongitude: -122.3421,
		SearchRadius: 200.3,
	})
	if err != nil {
		log.Fatalf("FindNearest failed: %v", err)
	}

	log.Printf("Nearest Stations %v", near_stations_resp)
}