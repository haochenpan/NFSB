package main

import (
	"NFSB/DataStruct"
	"fmt"
	"sync"

	zmq "github.com/pebbe/zmq4"
)

// Listening from UserInput and Publish the data to GNF
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
}

func prepareSendingToGNFs(data DataStruct.UserData, publisher *zmq.Socket) {
	if len(data.GnfIPS) == len(gnfIPs) {
		broadcastToGNF(data, publisher)
	} else {
		ipList := data.GnfIPS
		for _, gnfIP := range ipList {
			fmt.Println(gnfIP)
			sendDataToGNF(gnfIP, data, publisher)
		}
	}
}

// Send to Specific gnfIPS
func sendDataToGNF(address string, data DataStruct.UserData, publisher *zmq.Socket) {
	publisher.SendMessage(
		[][]byte{
			[]byte(address), //this act as a filter
			DataStruct.Encode(&data)},
		0)
}

// Broadcast to all the GNF
func broadcastToGNF(data DataStruct.UserData, publisher *zmq.Socket) {
	publisher.SendMessage(
		[][]byte{
			[]byte("all"), //this act as a filter
			DataStruct.Encode(&data)},
		0)
}
