// cmd/server/main.go
package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/psamuthis/grpc-station/cmd/server/seed"
	pb "github.com/psamuthis/grpc-station/proto"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var rdb *redis.Client
var kafka_writer *kafka.Writer
var kafka_reader *kafka.Reader

func init() {
	time.Sleep(15 * time.Second)
	init_redis()
	init_kafka_writer()
	//init_kafka_reader()
}

func init_redis() {
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

func init_kafka_writer() {
	kafka_brokers := os.Getenv("KAFKA_BROKERS")
	topic_name := "stations-updates"

	if kafka_brokers == "" {
		log.Fatalf("Could not retrieve kafka brokers from env.")
	}
	log.Printf("Retrieved kafka brokers env.")

	kafka_writer = kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafka_brokers},
		Topic:    topic_name,
		Balancer: &kafka.LeastBytes{},
	})
	log.Printf("Kafka writer on %v", kafka_brokers)
}

func init_kafka_reader() {
	kafka_brokers := os.Getenv("KAFKA_BROKERS")
	topic_name := "stations-updates"

	kafka_reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{kafka_brokers},
		Topic:       topic_name,
		Partition:   0,
		StartOffset: kafka.LastOffset,
	})
	log.Printf("kafka reader on %v", kafka_brokers)
}

type stationServer struct {
	pb.UnimplementedStationServer
}

func persistToKafka(station string) {
	stock, _ := rdb.HGet(context.Background(), "station:"+station, "stock").Int64()
	lat, _ := rdb.HGet(context.Background(), "station:"+station, "lat").Float64()
	lon, _ := rdb.HGet(context.Background(), "station:"+station, "lon").Float64()
	event := map[string]any{
		"station": station,
		"stock":   stock,
		"lat":     lat,
		"lon":     lon,
	}
	data, _ := json.Marshal(event)

	record := kafka.Message{
		Key:   []byte(station),
		Value: data,
	}
	kafka_writer.WriteMessages(context.Background(), record)
	log.Printf("Wrote %v to kafka", event)
}

func restoreFromKafka() {
	kafka_brokers := os.Getenv("KAFKA_BROKERS")
	topic_name := "stations-updates"

	kafka_reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{kafka_brokers},
		Topic:       topic_name,
		Partition:   0,
		MaxWait:     1 * time.Second,
		StartOffset: kafka.FirstOffset,
	})
	log.Printf("kafka reader on %v", kafka_brokers)

	for {
		log.Printf("Reading messages")
		msg, err := kafka_reader.ReadMessage(context.Background())
		if err != nil {
			break
		}

		var e struct {
			Station string
			Stock   int64
			Lat     float64
			Lon     float64
		}

		if json.Unmarshal(msg.Value, &e) == nil {
			//rdb.HSet(context.Background(), "station:"+e.Station, "stock", e.Stock, "latitude", e.Latitude, "longitude", e.Longitude)
			log.Printf("station %v", e.Station)
			log.Printf("stock %v", e.Stock)
			log.Printf("lat %v", e.Lat)
			log.Printf("lon %v", e.Lon)
		}
	}
}

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

func (s *stationServer) FindNearestStation(ctx context.Context, req *pb.NearStationRequest) (*pb.NearStationResponse, error) {
	lat := req.GetCarLatitude()
	lon := req.GetCarLongitude()
	radius := req.GetSearchRadius()
	if radius <= 0 {
		radius = 50
	}
	log.Printf("FindNearestStation called. Vehicle location %f,%f search radius %f km", lat, lon, radius)

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
			Distance:         float32(g.Dist),
			Stock:            stock,
			StationLatitude:  g.Latitude,
			StationLongitude: g.Longitude,
		})
	}

	return &pb.NearStationResponse{Stations: stations}, nil
}

func (s *stationServer) UpdateStationStock(ctx context.Context, req *pb.UpdateStationRequest) (*pb.UpdateStationReply, error) {
	key := "station:" + req.GetStationName()
	log.Printf("UpdateStationStock called → %s  incr:%d", req.GetStationName(), req.GetStockIncrement())

	_, redisErr := rdb.HIncrBy(ctx, key, "stock", req.GetStockIncrement()).Result()
	persistToKafka(req.GetStationName())

	if redisErr != nil {
		log.Printf("Redis update failed (will be restored from Kafka later): %v", redisErr)
		return &pb.UpdateStationReply{Updated: false}, redisErr
	}

	return &pb.UpdateStationReply{Updated: true}, nil
}

func main() {
	seed.SeedStations(rdb)

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	go func() {
		time.Sleep(8 * time.Second)
		restoreFromKafka()
	}()

	s := grpc.NewServer()
	pb.RegisterStationServer(s, &stationServer{})
	log.Println("gRPC server listening on :50051")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

	defer kafka_writer.Close()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan
}
