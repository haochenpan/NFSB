package main

import (
	"NFSB/Config"
	gnf "NFSB/GNF"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/go-redis/redis"
	zmq "github.com/pebbe/zmq4"
)

//Subscriber for statss
func initControllerStatsSub(wg *sync.WaitGroup) {
	fileName := "stats.txt"
	Utility.CreateFile(fileName)
	defer wg.Done()
	context, _ := zmq.NewContext()

	subscriber, _ := context.NewSocket(zmq.SUB)
	defer subscriber.Close()

	// Connect to the subscriber to listening for their stats
	defer wg.Done()

	// in the future if we have more than one gnf
	port, _ := portMap["controller_stats_port"]

	//Test Cloud: Uncomment this
	for _, ip := range gnfIPs {
		subscriber.Connect("tcp://" + ip + ":" + port)
	}

	//Test Local: Use this
	subscriber.Connect("tcp://localhost" + ":" + port)

	subscriber.SetSubscribe("stat")
	// TODO: in the future can make this parrallel working
	for {
		for i := 0; i < numServer; i++ {
			subscriber.RecvBytes(0)
			b, _ := subscriber.RecvBytes(0)
			stats := gnf.DecodeBmStat(b)
			Utility.AppendStatsToFile(fileName, stats.String())
			subscriber.RecvBytes(0)
		}

	}
}

func initControllerStatsSubTest(wg *sync.WaitGroup, ch chan bool) {
	fileName := "stats.txt"
	Utility.CreateFile(fileName)
	defer wg.Done()
	context, _ := zmq.NewContext()

	subscriber, _ := context.NewSocket(zmq.SUB)
	defer subscriber.Close()

	// Connect to the subscriber to listening for their stats
	defer wg.Done()

	// in the future if we have more than one gnf
	port, _ := portMap["controller_stats_port"]

	//Test Cloud: Uncomment this
	for _, ip := range gnfIPs {
		subscriber.Connect("tcp://" + ip + ":" + port)
	}

	//Test Local: Use this
	subscriber.Connect("tcp://localhost" + ":" + port)

	subscriber.SetSubscribe("stat")
	// TODO: in the future can make this parrallel working
	for {
		// Wait from all the response from the server
		for i := 0; i < numServer; i++ {
			subscriber.RecvBytes(0)
			b, _ := subscriber.RecvBytes(0)
			stats := gnf.DecodeBmStat(b)
			Utility.AppendStatsToFile(fileName, stats.String())
			subscriber.RecvBytes(0)
			fmt.Println("Receive One Data")
		}
		// Send a message to the send side telling the controller that it can start to do the next command
		ch <- true
	}
}

func clearRedisDB() {
	sizeCmd := redisClient.DBSize()
	count, _ := sizeCmd.Result()
	fmt.Println("Before cleaning up db's count = " + strconv.FormatInt(count, 10))
	//Clean up the db
	redisClient.FlushAllAsync()
	sizeCmd = redisClient.DBSize()
	count, _ = sizeCmd.Result()
	fmt.Println("After cleaning up db's count = " + strconv.FormatInt(count, 10))
}

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
	return client
}
