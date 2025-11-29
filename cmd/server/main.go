// cmd/server/main.go
package main

import (
	"context"
	"log"
	"net"
	"os"
	"strconv"

	pb "github.com/psamuthis/grpc-station/proto"
	"github.com/psamuthis/grpc-station/cmd/server/seed"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var rdb *redis.Client

func init() {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "redis:6379"
	}
	rdb = redis.NewClient(&redis.Options{Addr: addr})

	if _, err := rdb.Ping(context.Background()).Result(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	log.Println("Connected to Redis at startup")
}

type stationServer struct {
	pb.UnimplementedStationServer
}

// ────────────────────── PollStation ──────────────────────
func (s *stationServer) PollStation(ctx context.Context, req *pb.PollStationRequest) (*pb.StationReply, error) {
	key := "station:" + req.GetName()
	log.Printf("PollStation called key=%v", key)
	data, err := rdb.HGetAll(ctx, key).Result()
	if err != nil || len(data) == 0 {
		return &pb.StationReply{Stock: 0}, nil
	}

	stock, _ := strconv.ParseInt(data["stock"], 10, 64)
	lat, _ := strconv.ParseFloat(data["lat"], 64)
	lon, _ := strconv.ParseFloat(data["lon"], 64)

	return &pb.StationReply{
		Stock:            stock,
		StationLatitude:  lat,
		StationLongitude: lon,
	}, nil
}

// ────────────────────── FindNearestStation ──────────────────────
func (s *stationServer) FindNearestStation(ctx context.Context, req *pb.NearStationRequest) (*pb.NearStationResponse, error) {
	lat := req.GetCarLatitude()
	lon := req.GetCarLongitude()
	radius := req.GetSearchRadius()
	if radius <= 0 {
		radius = 50
	}
	log.Printf("FindNearestStation called. Vehicle location %f,%f search radius %f km", lat, lon, radius)

	// This works on ALL go-redis/v9 versions
	results, err := rdb.GeoRadius(ctx, "stations", lon, lat, &redis.GeoRadiusQuery{
		Radius:    radius,
		Unit:      "km",
		WithDist:  true,
		WithCoord: true,
		Count:     10,
	}).Result()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "geo search failed: %v", err)
	}

	var stations []*pb.NearStationReply
	for _, g := range results {
		id := g.Name
		data, err := rdb.HGetAll(ctx, "station:"+id).Result()
		if err != nil || len(data) == 0 {
			continue
		}

		stock, _ := strconv.ParseInt(data["stock"], 10, 64)

		stations = append(stations, &pb.NearStationReply{
			Name:             data["name"],
			Distance:         float32(g.Dist),   // Dist, not Distance
			Stock:            stock,
			StationLatitude:  g.Latitude,             // Lat, not Latitude
			StationLongitude: g.Longitude,             // Lon, not Longitude
		})
	}

	return &pb.NearStationResponse{Stations: stations}, nil
}

// ────────────────────── UpdateStation (optional) ──────────────────────
func (s *stationServer) UpdateStationStock(ctx context.Context, req *pb.UpdateStationRequest) (*pb.UpdateStationReply, error) {
	key := "station:" + req.GetStationName()
	log.Printf("UpdateStationStock called. station %v stock_incr %d", req.GetStationName(), req.GetStockIncrement())
	_, err := rdb.HIncrBy(ctx, key, "stock", req.GetStockIncrement()).Result()
	return &pb.UpdateStationReply{Updated: err == nil}, nil
}

// ────────────────────── main ──────────────────────
func main() {
	seed.SeedStations(rdb)

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