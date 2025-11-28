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

	resp, err := client.PollStation(ctx, &pb.PollStationRequest{Name: "berlin-hbf"})
	if err != nil {
		log.Fatalf("Poll failed: %v", err)
	}

	log.Printf("Success! Stock: %d, Location: %s", resp.GetStock(), resp.GetStationLatitude(), resp.GetStationLongitude())
}