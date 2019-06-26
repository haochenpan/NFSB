package main

import (
	"NFSB/Config"
	"fmt"
	"strings"

	"github.com/go-redis/redis"
)

func ExampleNewClient() *redis.Client {
	ipList := Utility.ReadRedisIP()
	ip_port := ipList[0]
	l := strings.Fields(ip_port)
	redis_ip := l[0]
	redis_port := l[1]

	client := redis.NewClient(&redis.Options{
		Addr:     redis_ip + ":" + redis_port,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	pong, err := client.Ping().Result()
	fmt.Println(pong, err)
	return client
}

func main() {
	client := ExampleNewClient()
	client.SAdd("abc", 1)
	sizeCmd := client.DBSize()
	count, _ := sizeCmd.Result()
	fmt.Println(count)

	client.FlushAllAsync()
	sizeCmd = client.DBSize()
	count, _ = sizeCmd.Result()
	fmt.Println(count)

}
