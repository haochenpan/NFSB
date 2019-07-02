package main

import (
	"NFSB/Config"
	"NFSB/DataStruct"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"

	"github.com/go-redis/redis"
)

var (
	gnfIPs           []string
	portMap          = make(map[string]string)
	numServer        int
	redisClients     []*redis.Client
	outputFilePrefix string
	CONN_TYPE        = "tcp"
	CONN_HOST        = Utility.ReadControllerIp()
)

func running() {
	var wg sync.WaitGroup
	wg.Add(3)
	ch := make(chan DataStruct.UserData)

	// Port talking with Clients Input
	go initUserListener(&wg, ch)
	// Publish the Clients data to the GNF
	go initControllerPub(&wg, ch)
	// Subscriber listening the stats published by the Gnfs
	go initControllerTCPStats(&wg)
	wg.Wait()
	// Function that handle communication with GNF
}

// For benchmark we will not have users running
// but will make the server runs benchmark itself
func benchmark(rounds int) {
	fmt.Println("Benchmarking")
	var wg sync.WaitGroup
	wg.Add(2)

	statsCh := make(chan bool)
	go initControllerPubTest(&wg, statsCh, rounds)

	//Testing:
	// Subscriber listening the stats published by the Gnfs
	go initControllerTCPStatsBenchmark(&wg, statsCh)
	wg.Wait()
}

func main() {
	// Load port Parameter
	Utility.LoadPortConfig(portMap)
	// Populate GNF IP address
	gnfIPs = Utility.LoadGnfAddress()
	numServer = len(gnfIPs)

	//Initiate redis Client
	redisClients = ExampleNewClient()
	if len(os.Args) == 2 {
		// Create the File under the Main
		outputFilePrefix = os.Args[1]
		//Waiting for GNF to join
		waitGnfJoin()
		running()
	} else if len(os.Args) == 4 {
		if os.Args[1] != "benchmark" {
			fmt.Println("Cannot understand your input")
		} else {
			rounds, err := strconv.Atoi(os.Args[2])
			if err != nil {
				log.Fatal(err)
			}
			// Initialize the output.txt based on User request
			outputFilePrefix = os.Args[3]
			waitGnfJoin()
			benchmark(rounds)
		}
	} else {
		fmt.Println("Invalid Number of Parameter")
	}
}
