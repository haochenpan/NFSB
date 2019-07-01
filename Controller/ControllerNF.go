package main

import (
	"NFSB/Config"
	"NFSB/DataStruct"
	"fmt"
	"strconv"
	"sync"
	"time"

	zmq "github.com/pebbe/zmq4"
)

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

func initControllerPubTest(wg *sync.WaitGroup, ch chan bool, rounds int) {
	defer wg.Done()
	context, _ := zmq.NewContext()

	publisher, _ := context.NewSocket(zmq.PUB)
	defer publisher.Close()
	port, _ := portMap["controller_nf_port"]
	publisher.Bind("tcp://*:" + port)

	//Need some time for server to bind
	time.Sleep(5 * time.Second)
	var data DataStruct.UserData
	// Load once Run once
	for i := 0; i < rounds; i++ {
		seperator := "****************** round " + strconv.Itoa(i+1) + " ************************************\n"
		Utility.AppendStatsToFile(fileName, seperator)

		fmt.Println("**************************** New round of Load and Run***********************")

		//Load Phase
		Utility.AppendStatsToFile(fileName, "Load Phase\n")
		fmt.Println("Load_Phase")
		data.Action = "load"
		prepareSendingToGNFsTest(data, publisher)

		// time.Sleep(100 * time.Second)
		// Wait until the stats channel
		<-ch
		// fmt.Println(ok)

		// //Run Phase
		fmt.Println("Run_Phase")
		Utility.AppendStatsToFile(fileName, "Run Phase\n")
		data.Action = "run"
		//Asssuming all are going to perform the same task
		data.GnfIPS = gnfIPs
		prepareSendingToGNFsTest(data, publisher)

		// Wait the result thread
		<-ch

		//clear redis db
		clearRedisDB()
		Utility.AppendStatsToFile(fileName, "\n")
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

func prepareSendingToGNFsTest(data DataStruct.UserData, publisher *zmq.Socket) {
	data.GnfIPS = gnfIPs
	loadNamePrefix := "workload"
	if data.Action == "load" || data.Action == "run" {
		for i, gnfIP := range gnfIPs {
			// Put the default file path to the field
			// each workload will assign to one gnf
			data.WorkLoadFile = loadNamePrefix + strconv.Itoa(i) + ".txt"
			sendDataToGNF(gnfIP, data, publisher)
		}
	}
}

func sendDataToGNF(address string, data DataStruct.UserData, publisher *zmq.Socket) {
	fmt.Println("Send to" + address)
	Utility.PrintUserData(data)
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
