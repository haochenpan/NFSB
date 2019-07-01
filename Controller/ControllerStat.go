package main

import (
	"NFSB/Config"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/go-redis/redis"
)

//Subscriber for statss
// func initControllerStatsSub(wg *sync.WaitGroup) {
// 	defer wg.Done()
// 	context, _ := zmq.NewContext()

// 	subscriber, _ := context.NewSocket(zmq.SUB)
// 	defer subscriber.Close()

// 	// Connect to the subscriber to listening for their stats
// 	defer wg.Done()

// 	// in the future if we have more than one gnf
// 	port, _ := portMap["controller_stats_port"]

// 	//Test Cloud: Uncomment this
// 	for _, ip := range gnfIPs {
// 		subscriber.Connect("tcp://" + ip + ":" + port)
// 	}

// 	//Test Local: Use this
// 	subscriber.Connect("tcp://localhost" + ":" + port)

// 	subscriber.SetSubscribe("stat")
// 	// TODO: in the future can make this parrallel working
// 	for {
// 		for i := 0; i < numServer; i++ {
// 			subscriber.RecvBytes(0)
// 			b, _ := subscriber.RecvBytes(0)
// 			stats := gnf.DecodeBmStat(b)
// 			Utility.AppendStatsToFile(outputFile, stats.String())
// 			subscriber.RecvBytes(0)
// 		}

// 	}
// }

// func initControllerStatsSubTest(wg *sync.WaitGroup, ch chan bool) {
// 	fileName := "stats.txt"
// 	defer wg.Done()
// 	context, _ := zmq.NewContext()

// 	subscriber, _ := context.NewSocket(zmq.SUB)
// 	defer subscriber.Close()

// 	// Connect to the subscriber to listening for their stats
// 	defer wg.Done()

// 	// in the future if we have more than one gnf
// 	port, _ := portMap["controller_stats_port"]

// 	//Test Cloud: Uncomment this
// 	for _, ip := range gnfIPs {
// 		subscriber.Connect("tcp://" + ip + ":" + port)
// 	}

// 	//Test Local: Use this
// 	subscriber.Connect("tcp://localhost" + ":" + port)

// 	subscriber.SetSubscribe("stat")
// 	// TODO: in the future can make this parrallel working
// 	var round int64
// 	for {
// 		// Wait from all the response from the server
// 		for i := 0; i < numServer; i++ {
// 			subscriber.RecvBytes(0)
// 			b, _ := subscriber.RecvBytes(0)
// 			stats := gnf.DecodeBmStat(b)
// 			Utility.AppendStatsToFile(fileName, stats.String())
// 			subscriber.RecvBytes(0)
// 			fmt.Println("Receive One Data")
// 			round++
// 		}
// 		// Send a message to the send side telling the controller that it can start to do the next command
// 		ch <- true
// 	}
// }

func initControllerTCPStats(wg *sync.WaitGroup) {
	defer wg.Done()
	statsPort := portMap["controller_stats_port"]
	l, err := net.Listen(CONN_TYPE, CONN_HOST+":"+statsPort)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	// Close the listener when the application closes.
	defer l.Close()
	for {
		// Listen for an incoming connection.
		fmt.Println("Listening for incoming request")
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}

		go handleStats(conn)
	}
	fmt.Println("Bye")
}

func handleStats(conn net.Conn) {
	fileName := outputFilePrefix + conn.RemoteAddr().String() + ".txt"
	Utility.CreateFile(fileName)
	for {
		// Make a buffer to hold incoming data.
		buf := make([]byte, 2048)
		// Read the incoming connection into the buffer.
		reqLen, _ := conn.Read(buf)
		stats := string(buf[:reqLen])

		Utility.AppendStatsToFile(fileName, stats)
	}
}

func initControllerTCPStatsBenchmark(wg *sync.WaitGroup, roundChan chan bool) {
	defer wg.Done()
	statsPort := portMap["controller_stats_port"]
	l, err := net.Listen(CONN_TYPE, CONN_HOST+":"+statsPort)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	// Close the listener when the application closes.
	defer l.Close()
	ch := make(chan bool)

	go func(ch chan bool, roundChan chan bool) {
		for {
			// Wait till all the responses come back
			for i := 0; i < numServer; i++ {
				<-ch
			}
			// Send the signal to the main thread to trigger the next round
			roundChan <- true
		}
	}(ch, roundChan)

	for {
		// Listen for an incoming connection.
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}

		go handleStats(conn)
	}
}

func handleStatsBenchmark(conn net.Conn, ch chan bool) {
	fileName := outputFilePrefix + conn.RemoteAddr().String() + ".txt"
	Utility.CreateFile(fileName)
	round := 1
	for {
		Utility.AppendStatsToFile(fileName, "Round"+strconv.Itoa(round)+"\n")
		Utility.AppendStatsToFile(fileName, "Load Phase \n")
		// Load Phase
		// Make a buffer to hold incoming data.
		buf := make([]byte, 2048)
		// Read the incoming connection into the buffer.
		reqLen, _ := conn.Read(buf)
		stats := string(buf[:reqLen])

		Utility.AppendStatsToFile(fileName, stats)
		ch <- true

		// Run Phase
		Utility.AppendStatsToFile(fileName, "Run Phase \n")
		buf = make([]byte, 2048)
		// Read the incoming connection into the buffer.
		reqLen, _ = conn.Read(buf)
		stats = string(buf[:reqLen])

		Utility.AppendStatsToFile(fileName, stats)
		ch <- true
	}
}

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
