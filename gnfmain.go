package main

import (
	"NFSB/Config"
	gnf "NFSB/GNF"
	"fmt"
	"net"
)

// Send Pong to Controller
func sendPingToController() {
	var conn net.Conn
	var err error

	controllerIP := Utility.ReadControllerIp()
	port := "6669"

	if conn, err = net.Dial("tcp", controllerIP+":"+port); err != nil {
		fmt.Println("err2=", err)
	} else {
		conn.Write([]byte("Alive"))
	}
}

func main() {
	if err := gnf.GnfMain(); err != nil {
		fmt.Println(err)
	} else {
		// Every thing is setup Correctly
		sendPingToController()
	}
}
