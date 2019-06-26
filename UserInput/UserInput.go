package main

import (
	"NFSB/Config"
	"NFSB/DataStruct"
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	zmq "github.com/pebbe/zmq4"
)

var (
	gnfAddress    = make(map[string]int)
	gnfAdressList []string
	portMap       = make(map[string]string)
	controllerIP  string
)

func printInvalidInput() {
	fmt.Println("Please specify your parameter correctly or the GNF address you passed in the Controller does not know")
}

func printInvalidGNFAddress() {
	fmt.Println("One of your GNF Address is invalid")
}

func printFileNotExist() {
	fmt.Println("The provided workload File does not exist")
}

func checkValidGNFAddress(gnfIPs []string) bool {
	for _, gnfIP := range gnfIPs {
		_, ok := gnfAddress[gnfIP]
		if !ok {
			return false
		}
	}
	return true
}

func printInstruction() {
	fmt.Println("Following are the instruction you can perform")
	fmt.Println("***********************************************")
	fmt.Println("load")
	fmt.Println("Syntax: [load] [new or default] [fileName] [all or ip1 ip2 ip3...]")
	fmt.Println("***********************************************")
	fmt.Println("run")
	fmt.Println("Syntax: [run] [new or default] [fileName] [all or ip1 ip2 ip3...]")
	fmt.Println("***********************************************")
	fmt.Println("interrupt")
	fmt.Println("Syntax: [interrupt] [all or ip1 ip2 ip3 ...]")
	fmt.Println("***********************************************")
	fmt.Println("quit")
	fmt.Println("Syntax: [quit]")
	fmt.Println("***********************************************")
}

func readFromUser(ch chan DataStruct.UserData, wg *sync.WaitGroup) {
	defer wg.Done()
	printInstruction()
	reader := bufio.NewReader(os.Stdin)
	var data DataStruct.UserData
	for {

		text, _ := reader.ReadString('\n')
		text = strings.Replace(text, "\n", "", -1)
		paras := strings.Fields(text)

		if len(paras) == 0 {
			printInvalidInput()
		}

		action := paras[0]
		//fmt.Println(paras)
		// load run:
		// [action] [new || default] [workLoadFilePath] [IPList]
		if action == "quit" {
			data.Action = action
			data.GnfIPS = gnfAdressList
			ch <- data

		} else if action == "interrupt" {

			if len(paras) < 2 {
				printInvalidInput()
			} else if len(paras) == 2 && paras[1] == "all" {
				data.Action = action
				data.GnfIPS = gnfAdressList
				ch <- data
			} else if checkValidGNFAddress(paras[1:]) {
				data.GnfIPS = paras[1:]
				data.Action = action
				ch <- data

			} else {
				printInvalidInput()
			}

		} else if action == "load" || action == "run" {
			data.Action = action

			if len(paras) < 4 {
				printInvalidInput()
			} else {
				flag := paras[1]
				filePath := paras[2]
				all := paras[3]

				//Check fileExists
				if Utility.IsFileExist(filePath) {
					if flag == "new" {
						data.NewWorkLoad = true
						//will be the bytes of the config content
						data.WorkLoadFile = Utility.LoadWorkLoadFile(filePath)
					} else if flag == "default" {
						data.NewWorkLoad = false
						data.WorkLoadFile = filePath
					} else {
						printInvalidInput()
						continue
					}
				} else {
					printFileNotExist()
					continue
				}

				if all == "all" {
					//Send to all the GNF
					//fmt.Println("Send to all")
					data.GnfIPS = gnfAdressList
					ch <- data
				} else {
					ips := paras[3:]
					//fmt.Println(ips)
					if checkValidGNFAddress(ips) {
						data.GnfIPS = ips
						ch <- data
					} else {
						printInvalidInput()
					}
				}
			}
		} else {
			printInvalidInput()
		}
	}
}

func sendDataToController(userChan chan DataStruct.UserData, wg *sync.WaitGroup) {
	defer wg.Done()
	client, _ := zmq.NewSocket(zmq.DEALER)
	defer client.Close()

	port, _ := portMap["controller_user_port"]
	client.Connect("tcp://" + controllerIP + ":" + port)

	for {
		data := <-userChan
		//Utility.PrintUserData(data)
		b := DataStruct.Encode(&data)
		client.SendMessage(b)
		returnByte, _ := client.RecvBytes(0)
		fmt.Println(string(returnByte))
	}
}

func test(userChan chan DataStruct.UserData, wg *sync.WaitGroup) {
	defer wg.Done()
	reader := bufio.NewReader(os.Stdin)
	var data DataStruct.UserData
	for {
		text, _ := reader.ReadString('\n')
		text = strings.Replace(text, "\n", "", -1)
		data.Action = "load"
		data.GnfIPS = gnfAdressList[1:]
		fmt.Printf("%v", gnfAdressList)
		userChan <- data
	}
}

func loadGnfMap() {
	for _, ip := range gnfAdressList {
		gnfAddress[ip] = 1
	}
}

func convertFiletoBytes(fileName string) []byte {
	buf := bytes.NewBuffer(nil)
	f, _ := os.Open(fileName)
	io.Copy(buf, f)
	f.Close()
	return buf.Bytes()
}

func main() {
	controllerIP = Utility.ReadControllerIp()
	// Put the port needs to be used into a map
	Utility.LoadPortConfig(portMap)

	// Get the List of GNF lists from the config
	gnfAdressList = Utility.LoadGnfAddress()
	//load the gnf to Map so we can check whether the user input a valid address
	loadGnfMap()

	// This channel will be connected between the following two threads
	// 1. Thread to listen userInput
	// 2. Thread to send to the GNFs
	userChan := make(chan DataStruct.UserData)

	var wg sync.WaitGroup
	wg.Add(2)
	go readFromUser(userChan, &wg)
	go sendDataToController(userChan, &wg)
	wg.Wait()

}
