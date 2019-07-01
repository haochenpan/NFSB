package main

import (
	"NFSB/Config"
	"NFSB/DataStruct"
	"sync"
)

var (
	// array to store all GNF IP
	gnfIPs []string
	// Load the port from the config file
	portMap = make(map[string]string)
)

func main() {
	// Load port Parameter
	Utility.LoadPortConfig(portMap)
	// Init gnf IP address
	gnfIPs = Utility.LoadGnfAddress()

	var wg sync.WaitGroup
	wg.Add(3)
	ch := make(chan DataStruct.UserData)
	// Communication with Clients Input
	go initUserListener(&wg, ch)
	// Publish the Clients data to the GNF
	go initControllerPub(&wg, ch)
	// Subscriber listening the stats published by the Gnfs
	go initControllerStatsSub(&wg)
	wg.Wait()
	// Function that handle communication with GNF
}
