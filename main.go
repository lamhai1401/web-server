package main

import (
	"context"
	"fmt"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
)

var red *redis.Client

func main() {
	app := gin.Default()

	app.Use(useRateLimit(3, 10))

	app.GET("/", func(c *gin.Context) {
		c.JSON(200, "Hello world")
	})

	red = redis.NewClient(&redis.Options{
		Addr:        "localhost:6379",
		Password:    "",
		DB:          0,
		PoolTimeout: time.Minute, // since we user transaction so it can take a long time
	})

	app.Run(":8088")
}

func useRateLimit(rateLimit int64, second int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		clientIP := c.ClientIP()
		key := "RATE_LIMIT_COUNT_" + clientIP
		err := incRequestCount(key, rateLimit, second)

		if err != nil {
			c.AbortWithStatus(403)
			return
		}
		c.Next()
	}
}

func incRequestCount(key string, rateLimit, second int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := red.Watch(ctx, func(t *redis.Tx) error {
		ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel2()
		t.SetNX(ctx2, key, 0, time.Duration(second)*time.Second)
		ctx3, cancel3 := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel3()
		count, err := t.Incr(ctx3, key).Result()

		if count > rateLimit {
			err = fmt.Errorf("Rate limited")
		}

		if err != nil {
			return err
		}

		return nil
	}, key)
	return err
}
