package main

import (
	"NFSB/Config"
	"fmt"
	"os"
	"sync"
)

var (
	controller_user_port  string
	controller_nf_port    string
	controller_stats_port string
	portMap               = make(map[string]string)
	myAddressIP           string
	controllerIP          string
)

func main() {
	args := os.Args
	if len(args) != 2 {
		fmt.Println("Not enough arguments")
	} else {
		myAddressIP = args[1]
		controllerIP = Utility.ReadControllerIp()
		Utility.LoadPortConfig(portMap)
		var wg sync.WaitGroup
		wg.Add(2)
		go initControllerSub(&wg)
		go initStatPub(&wg)
		wg.Wait()
	}
}
