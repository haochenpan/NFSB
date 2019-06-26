package main

import (
	"NFSB/Config"
	"NFSB/DataStruct"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
)

var (
	gnfIPs    []string
	portMap   = make(map[string]string)
	numServer int
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
	go initControllerStatsSub(&wg)
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
	go initControllerStatsSubTest(&wg, statsCh)
	wg.Wait()
}

func main() {
	// Load port Parameter
	Utility.LoadPortConfig(portMap)
	// Init their IP address
	gnfIPs = Utility.LoadGnfAddress()
	numServer = len(gnfIPs)
	fmt.Println(numServer)
	if len(os.Args) == 1 {
		running()
	} else if len(os.Args) == 3 {
		if os.Args[1] != "benchmark" {
			fmt.Println("Cannot understand your input")
		} else {
			rounds, err := strconv.Atoi(os.Args[3])
			if err != nil {
				log.Fatal(err)
			}
			benchmark(rounds)
		}
	} else {
		fmt.Println("Invalid Mode")
	}
}
