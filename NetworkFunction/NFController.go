package main

import (
	"NFSB/Config"
	"NFSB/DataStruct"
	"fmt"
	"net"
	"sync"

	zmq "github.com/pebbe/zmq4"
)

var (
	keys []string
)

func initControllerSub(wg *sync.WaitGroup) {
	defer wg.Done()
	context, _ := zmq.NewContext()

	subscriber, _ := context.NewSocket(zmq.SUB)
	defer subscriber.Close()
	// Connect to the subscriber

	port, _ := portMap["controller_nf_port"]
	subscriber.Connect("tcp://" + controllerIP + ":" + port)
	//Subscribe to the specific message
	subscriber.SetSubscribe("all")
	//Local Testing: Just need to change the port

	// If migrate to different servers, need to pass in the address
	subscriber.SetSubscribe(myAddressIP)

	for {
		subscriber.RecvBytes(0)
		dataBytes, _ := subscriber.RecvBytes(0)
		fmt.Println(len(dataBytes))
		data := DataStruct.Decode(dataBytes)
		Utility.PrintUserData(data)
		subscriber.RecvBytes(0)
	}
}

func handleRequest(conn net.Conn) {

}
