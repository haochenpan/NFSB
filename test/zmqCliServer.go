package main

import (
	"fmt"
	"net"
	"os"
	"time"
)

const (
	CONN_HOST = "localhost"
	CONN_PORT = "3333"
	CONN_TYPE = "tcp"
)

func client() {
	// connect to this socket
	conn, _ := net.Dial("tcp", "127.0.0.1:3333")
	for {
		text := "wut up"
		// send to socket
		fmt.Fprintf(conn, text+"\n")
		//   // listen for reply
		//   message, _ := bufio.NewReader(conn).ReadString('\n')
		//   fmt.Print("Message from server: "+message)
		time.Sleep(3 * time.Second)

		// listen for reply
		// message, _ := bufio.NewReader(conn).ReadString('\n')
		// fmt.Print("Message from server: " + message)
	}

}
func server() {
	l, err := net.Listen(CONN_TYPE, CONN_HOST+":"+CONN_PORT)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	// Close the listener when the application closes.
	defer l.Close()
	fmt.Println("Listening on " + CONN_HOST + ":" + CONN_PORT)
	for {
		// Listen for an incoming connection.
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		// Handle connections in a new goroutine.
		go handleRequest(conn)
	}
}

// Handles incoming requests.
func handleRequest(conn net.Conn) {
	for {
		// Make a buffer to hold incoming data.
		buf := make([]byte, 1024)
		// Read the incoming connection into the buffer.
		reqLen, err := conn.Read(buf)
		fmt.Println(conn.RemoteAddr().String())
		if err != nil {
			fmt.Println("Error reading:", err.Error())
		} else {
			fmt.Println(string(buf[:reqLen]))
		}

		// // send new string back to client
		// conn.Write([]byte(string(buf[:reqLen]) + "\n"))
	}

}

func main() {
	// server()
	client()
}
