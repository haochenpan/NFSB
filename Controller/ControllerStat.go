package main

import (
	"NFSB/Config"
	"NFSB/DataStruct"
	"sync"

	zmq "github.com/pebbe/zmq4"
)

//Subscriber for statss
func initControllerStatsSub(wg *sync.WaitGroup) {
	fileName := "stats.txt"
	Utility.CreateFile(fileName)
	defer wg.Done()
	context, _ := zmq.NewContext()

	subscriber, _ := context.NewSocket(zmq.SUB)
	defer subscriber.Close()

	// Connect to the subscriber to listening for their stats
	defer wg.Done()

	// in the future if we have more than one gnf
	port, _ := portMap["controller_stats_port"]

	//Test Cloud: Uncomment this
	for _, ip := range gnfIPs {
		subscriber.Connect("tcp://" + ip + ":" + port)
	}

	//Test Local: Use this
	subscriber.Connect("tcp://localhost" + ":" + port)

	subscriber.SetSubscribe("stat")
	// TODO: in the future can make this parrallel working
	for {
		subscriber.RecvBytes(0)
		b, _ := subscriber.RecvBytes(0)
		stats := DataStruct.DecodeStat(b)
		Utility.AppendStatsToFile(fileName, stats)
		subscriber.RecvBytes(0)
	}
}
