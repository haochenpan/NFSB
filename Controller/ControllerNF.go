package main

import (
	"NFSB/Config"
	"NFSB/DataStruct"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	zmq "github.com/pebbe/zmq4"
)

func waitGnfJoin() {
	fmt.Println("Waiting for GNF to join...")
	l, err := net.Listen(CONN_TYPE, CONN_HOST+":"+"6669")
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	// Close the listener when the application closes.
	defer l.Close()

	for i := 0; i < numServer; i++ {
		// Listen for an incoming connection.
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		go handlePingResponse(conn)

	}
	fmt.Println("Every GNF is ALive")
}

func handlePingResponse(conn net.Conn) {
	defer conn.Close()
	buf := make([]byte, 64)
	// Read the incoming connection into the buffer.
	conn.Read(buf)
	fmt.Println(conn.RemoteAddr().String() + "is alive")
}

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

// Note this function will also auto generate user Request to send to gnf
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
		fmt.Println("**************************** New round of Load and Run***********************")

		fmt.Println("Load_Phase")
		data.Action = "load"
		prepareSendingToGNFsTest(data, publisher)
		// Wait for the Load Phase Result
		<-ch

		// //Run Phase
		fmt.Println("Run_Phase")
		data.Action = "run"
		//Asssuming all are going to perform the same task
		data.GnfIPS = gnfIPs
		prepareSendingToGNFsTest(data, publisher)

		// Wait for the Run Phase Result
		<-ch

		//clear redis db
		clearRedisDB()
	}
}

// Normal User interaction
func prepareSendingToGNFs(data DataStruct.UserData, publisher *zmq.Socket) {
	if len(data.GnfIPS) == len(gnfIPs) {
		broadcastToGNF(data, publisher)
	} else {
		ipList := data.GnfIPS
		for _, gnfIP := range ipList {
			sendDataToGNF(gnfIP, data, publisher)
		}
	}
}

// Benchmarking purpose
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
