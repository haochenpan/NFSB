package main

import (
	"NFSB/DataStruct"
	"log"
	"sync"

	zmq "github.com/pebbe/zmq4"
)

//Listening at 5555
func initUserListener(wg *sync.WaitGroup, ch chan DataStruct.UserData) {
	defer wg.Done()
	// Can add concurrency in the future for handling users request
	go userWorker(ch)

	context, _ := zmq.NewContext()

	clients, _ := context.NewSocket(zmq.ROUTER)
	defer clients.Close()

	// Port Listening for Clients request on port 5555
	port, _ := portMap["controller_user_port"]
	clients.Bind("tcp://*:" + port)

	// Sockets that talk to our background worker threads
	// Basically this backend does nothing but forward the incoming
	// data to the worker threads
	backend, _ := zmq.NewSocket(zmq.DEALER)
	defer backend.Close()
	backend.Bind("ipc://backend")

	// Bind the clients and backedn through proxy
	err := zmq.Proxy(clients, backend, nil)
	log.Fatalln("Proxy interrupted", err)
}

func userWorker(ch chan DataStruct.UserData) {
	worker, _ := zmq.NewSocket(zmq.DEALER)
	defer worker.Close()
	worker.Connect("ipc://backend")

	//Now this worker threads will listen to the value passed in from the client
	// flag := false
	var userData DataStruct.UserData
	for {
		data, _ := worker.RecvMessageBytes(zmq.DONTWAIT)
		for i, b := range data {
			// Since index 0 is an indicator
			if i != 0 {
				userData = DataStruct.Decode(b)
				// Send the data you get from User to the thread that prepares to send to GNFs
				ch <- userData
				worker.SendBytes([]byte("done"), 0)
			}
		}
	}

}
