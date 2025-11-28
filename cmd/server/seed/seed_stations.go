package seed

import (
	"context"
	"log"

	"github.com/redis/go-redis/v9"
)

type Station struct {
	Name      string
	Lat       float64
	Lon       float64
	Stock 	  int
}

func SeedStations(rdb *redis.Client) {
	ctx := context.Background()

	rdb.Del(ctx, "stations")

	stations := []Station{
		{"Downtown Hub", 37.7749, -122.4194, 8},
		{"Airport FastCharge", 37.6213, -122.3790, 12},
		{"Mall North", 37.8044, -122.2712, 6},
		{"University Campus", 37.8719, -122.2585, 4},
		{"Seattle Central", 47.6062, -122.3321, 10},
	}

	// 1. Add to GEO index
	var geoLocations []*redis.GeoLocation
	for _, s := range stations {
		geoLocations = append(geoLocations, &redis.GeoLocation{
			Longitude: s.Lon,
			Latitude:  s.Lat,
			Name:      s.Name,
		})

		// 2. Store detailed data in a hash
		rdb.HSet(ctx, "station:"+s.Name, map[string]interface{}{
			"name":       s.Name,
			"lat":        s.Lat,
			"lon":        s.Lon,
			"stock":      s.Stock,
		})
	}

	// Bulk add to GEO set
	rdb.GeoAdd(ctx, "stations", geoLocations...)

	log.Println("Seeded", len(stations), "charging stations with GEO data")
}