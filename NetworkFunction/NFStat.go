package main

import (
	"NFSB/DataStruct"
	"sync"
	"time"

	zmq "github.com/pebbe/zmq4"
)

// TODO: Roger: you need to pass a channel argument here, so this thread can hear the result :)
func initStatPub(wg *sync.WaitGroup) {
	defer wg.Done()
	context, _ := zmq.NewContext()

	publisher, _ := context.NewSocket(zmq.PUB)
	defer publisher.Close()
	port, _ := portMap["controller_stats_port"]
	publisher.Bind("tcp://*:" + port)

	// This is just a mock test
	var stat DataStruct.Stats
	for i := 0; i < 3; i++ {
		time.Sleep(5 * time.Second)
		stat.Latency = "1111"
		stat.Throughput = "2222"
		publisher.SendMessage(
			[][]byte{
				[]byte("stat"), //this act as a filter
				DataStruct.EncodeStat(&stat)},
			0)
	}

	// Uncoment out this to get the value from channel
	// for {
	// 	select {
	// 	case data := <-ch:

	// 	}
	// }
}
