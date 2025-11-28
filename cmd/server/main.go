package main

import (
	"context"
	"log"
	"net"
	"os"
	"strconv"

	pb "github.com/psamuthis/grpc-station/proto"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var rdb *redis.Client

func init() {
	rdb = redis.NewClient(&redis.Options{
		Addr:os.Getenv("REDIS_ADDR"),
		Password: "",
		DB: 0,
	})

	ctx := context.Background()
	if _, err := rdb.Ping(ctx).Result(); err != nil {
		log.Fatalf("Failed to connect to Redis at startup: %v", err)
	}
	log.Println("Connected to Redis at startup")
}

type stationServer struct {
	pb.UnimplementedStationServer
}

func (s *stationServer) PollStation(ctx context.Context, req *pb.PollStationRequest) (*pb.StationReply, error) {
	station_name := req.GetName()
	if station_name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "station name is required")
	}

	result, err := rdb.HGetAll(ctx, station_name).Result()
	if err != nil {
		log.Printf("Redis HGetAll error %v", err)
		return nil, status.Errorf(codes.Internal, "redis error")
	}

	if len(result) == 0 {
		log.Printf("Station %s not found", station_name)
		return &pb.StationReply{
			Stock: 0,
			StationLatitude: 0.0,
			StationLongitude: 0.0,
		}, nil
	}

	stock := int64(0)
	if raw, ok := result["stock"]; ok {
		stock, _ = strconv.ParseInt(raw, 10, 64)
	}

	latitude := float64(0)
	if raw, ok := result["latitude"]; ok {
		latitude, _ = strconv.ParseFloat(raw, 64)
	}

	longitude := float64(0)
	if raw, ok := result["longitude"]; ok {
		longitude, _ = strconv.ParseFloat(raw, 64)
	}

	log.Printf("Station %s -> stock=%d location=%f,%f", station_name, stock, latitude, longitude)

	return &pb.StationReply{
		Stock: stock,
		StationLatitude: latitude,
		StationLongitude: longitude,
	}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterStationServer(s, &stationServer{})
	log.Println("gRPC server listening on :50051")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}