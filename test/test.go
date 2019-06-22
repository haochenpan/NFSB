package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"os"
)

func getOutboundIP() {
	ifaces, _ := net.Interfaces()
	// handle err
	for _, i := range ifaces {
		addrs, _ := i.Addrs()
		// handle err
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
				fmt.Println(ip.String())
			case *net.IPAddr:
				ip = v.IP
				fmt.Println(ip.String())
			}
			// process IP address
		}
	}
}

func main() {
	fmt.Println(getOutboundIP)
}

func loadText() {
	dir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(dir)
	fmt.Println(string(convertFiletoBytes("workload1.txt")))
}

func getMyIp() {

}

func convertFiletoBytes(fileName string) []byte {
	buf := bytes.NewBuffer(nil)
	f, err := os.Open(fileName)
	if err != nil {
		fmt.Println("Cannot find this file")
	}
	io.Copy(buf, f)
	f.Close()
	return buf.Bytes()
}

func writeToFile() {

}
