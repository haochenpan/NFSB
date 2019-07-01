package main

import (
	"log"
	"net"
)

func main() {
	var conn net.Conn
	var err error
	controllerIP := "10.150.0.6"
	port := "6668"
	if conn, err = net.Dial("tcp", controllerIP+":"+port); err != nil {
		log.Fatal(err)
		return
	}
	for i := 0; i < 5; i++ {
		conn.Write([]byte("Hey\n"))
	}
}
