package main

import (
	"NFSB/Config"
	"fmt"
	"strconv"
	"strings"

	"github.com/go-redis/redis"
)

var (
	redisClients []*redis.Client
)

func clearRedisDB() {
	for _, redisClient := range redisClients {
		fmt.Println("Clean up " + redisClient.Options().Addr)
		sizeCmd := redisClient.DBSize()
		count, _ := sizeCmd.Result()
		fmt.Println("Before cleaning up db's count = " + strconv.FormatInt(count, 10))
		//Clean up the db
		redisClient.FlushAllAsync()
		sizeCmd = redisClient.DBSize()
		count, _ = sizeCmd.Result()
		fmt.Println("After cleaning up db's count = " + strconv.FormatInt(count, 10))
	}
}

func ExampleNewClient() []*redis.Client {
	var clients []*redis.Client

	ipList := Utility.ReadRedisIP()

	for i := 0; i < len(ipList); i++ {
		ip_port := ipList[i]

		l := strings.Fields(ip_port)
		redis_ip := l[0]
		redis_port := l[1]

		client := redis.NewClient(&redis.Options{
			Addr:     redis_ip + ":" + redis_port,
			Password: "", // no password set
			DB:       0,  // use default DB
		})

		clients = append(clients, client)
	}

	return clients
}

// func main() {
// 	redisClients = ExampleNewClient()
// 	redisClients[0].SAdd("abc", 1)
// 	redisClients[1].SAdd("abc", 1)
// 	clearRedisDB()
// }
