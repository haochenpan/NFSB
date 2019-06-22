package main

import (
	"NFSB/DataStruct"
	"fmt"
	"sync"

	zmq "github.com/pebbe/zmq4"
)

//Publisher for data: 5556
func initControllerPub(wg *sync.WaitGroup, ch chan DataStruct.UserData) {
	defer wg.Done()
	context, _ := zmq.NewContext()

	publisher, _ := context.NewSocket(zmq.PUB)
	defer publisher.Close()
	port, _ := portMap["controller_nf_port"]
	publisher.Bind("tcp://*:" + port)

	for {
		select {
		case data := <-ch:
			// Get the data from the threads that listening for UserInput
			prepareSendingToGNFs(data, publisher)
		}
	}

	//Need to filter out the specific IP and then send them to All
}

func prepareSendingToGNFs(data DataStruct.UserData, publisher *zmq.Socket) {
	if len(data.GnfIPS) == len(gnfIPs) {
		fmt.Println("Broadcast")
		broadcastToGNF(data, publisher)
	} else {
		// Send to Specific gnfIPS
		ipList := data.GnfIPS
		for _, gnfIP := range ipList {
			fmt.Println(gnfIP)
			sendDataToGNF(gnfIP, data, publisher)
		}
	}
}

func sendDataToGNF(address string, data DataStruct.UserData, publisher *zmq.Socket) {
	publisher.SendMessage(
		[][]byte{
			[]byte(address), //this act as a filter
			DataStruct.Encode(&data)},
		0)
}

func broadcastToGNF(data DataStruct.UserData, publisher *zmq.Socket) {
	publisher.SendMessage(
		[][]byte{
			[]byte("all"), //this act as a filter
			DataStruct.Encode(&data)},
		0)
}
