package gnf

/*
   Copyright 2019 NFSB Research Team & Developers

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

import (
	"NFSB/DataStruct"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	zmq "github.com/pebbe/zmq4"
)

func mockSendController() {
	context, _ := zmq.NewContext()

	publisher, _ := context.NewSocket(zmq.PUB)
	defer publisher.Close()

	_ = publisher.Bind("tcp://*:6667")
	fmt.Println("sleeping....")
	time.Sleep(1 * time.Second)

	_, _ = publisher.SendMessage([][]byte{[]byte("all"),
		DataStruct.Encode(&DataStruct.UserData{Action: "interrupt"})}, 0)
	_, _ = publisher.SendMessage([][]byte{[]byte("all"),
		DataStruct.Encode(&DataStruct.UserData{
			Action:       "load",
			NewWorkLoad:  false,
			WorkLoadFile: "Config/workload_template"})}, 0)
	time.Sleep(10 * time.Second)
	_, _ = publisher.SendMessage([][]byte{[]byte("all"),
		DataStruct.Encode(&DataStruct.UserData{
			Action:       "run",
			NewWorkLoad:  false,
			WorkLoadFile: "Config/workload_template"})}, 0)
	time.Sleep(3 * time.Second)
	_, _ = publisher.SendMessage([][]byte{[]byte("all"),
		DataStruct.Encode(&DataStruct.UserData{Action: "interrupt"})}, 0)
	time.Sleep(3 * time.Second)
	_, _ = publisher.SendMessage([][]byte{[]byte("all"),
		DataStruct.Encode(&DataStruct.UserData{Action: "quit"})}, 0)

}

func mockRecvController() {
	var cxt *zmq.Context
	var sub *zmq.Socket
	var err error

	if cxt, err = zmq.NewContext(); err != nil {
		fmt.Println("err1=", err)
		return
	}
	if sub, err = cxt.NewSocket(zmq.SUB); err != nil {
		fmt.Println("err2=", err)
		return
	}
	defer sub.Close()

	if err = sub.Connect("tcp://127.0.0.1:6668"); err != nil {
		fmt.Println("err3=", err)
		return
	}
	if err = sub.SetSubscribe("stat"); err != nil {
		fmt.Println("err4=", err)
		return
	}

	for i := 0; i < 1; i++ {
		tuple, _ := sub.RecvMessageBytes(0)
		//fmt.Println("mockRecvController got !!!")
		data := DecodeBmStat(tuple[1])
		fmt.Println(data.String())
	}
	_, _ = fmt.Fprintln(os.Stdout, "mockRecvController exit")
}

/*
	exit condition: when isDone is closed or upon exception
	upon exit: does not close any channel
	upon exception: may send exeCmd{ErrorExit, "exeRecvThread exception"}
*/
func exeRecvThread(allToExe chan<- exeCmd, isDone <-chan bool, ip string, port string) {

	var cxt *zmq.Context
	var sub *zmq.Socket
	var err error

	if cxt, err = zmq.NewContext(); err != nil {
		allToExe <- exeCmd{ErrorExit, "exeRecvThread exception"}
		fmt.Println("err1=", err)
		return
	}
	if sub, err = cxt.NewSocket(zmq.SUB); err != nil {
		allToExe <- exeCmd{ErrorExit, "exeRecvThread exception"}
		fmt.Println("err2=", err)
		return
	}
	defer sub.Close()

	if err = sub.Connect("tcp://" + ip + ":" + port); err != nil {
		allToExe <- exeCmd{ErrorExit, "exeRecvThread exception"}
		fmt.Println("err3=", err)
		return
	}
	if err = sub.SetSubscribe("all"); err != nil {
		allToExe <- exeCmd{ErrorExit, "exeRecvThread exception"}
		fmt.Println("err4=", err)
		return
	}

	myIp, _ := getIp() // If migrate to different servers, need to pass in the address
	if err = sub.SetSubscribe(myIp); err != nil {
		allToExe <- exeCmd{ErrorExit, "exeRecvThread exception"}
		fmt.Println("err5=", err)
		return
	}

	// a thread that listens to the controller
	recvChan := make(chan []byte)
	go func() {
		for {
			if tuple, err := sub.RecvMessageBytes(0); err == nil {
				recvChan <- tuple[1]
			} else {
				fmt.Println("recvChan error,", err)
			}
		}
	}()

	allToExe <- exeCmd{Ready, "exeRecvThread is ready"}
	for {

		select {

		case <-isDone:
			//_, _ = fmt.Fprintln(os.Stdout, "exeRecvThread try to exit - 5")
			allToExe <- exeCmd{NormalExit, "exeRecvThread"}
			return

		case tuple := <-recvChan:
			data := DataStruct.Decode(tuple)

			switch data.Action {

			case "quit":
				allToExe <- exeCmd{GnfStop, ""}

			case "interrupt":
				allToExe <- exeCmd{BmkStop, ""}

			case "load", "run":

				var sig exeSig

				if data.Action == "load" {
					sig = CtrlLoad
				} else {
					sig = CtrlRun

				}

				if data.NewWorkLoad {
					fname := "workload_temp"
					if ret := writeWorkloadFile(data.WorkLoadFile, fname); ret != -1 {
						allToExe <- exeCmd{sig, fname}
					} else {
						fmt.Println("error in writing ")
					}
				} else {
					allToExe <- exeCmd{sig, data.WorkLoadFile}
				}

			}
		}

	}
}

/*
	exit condition: when isDone is closed or upon exception
	upon exit: does not close any channel
	upon exception: may send exeCmd{ErrorExit, "exeSendThread exception"}
*/
func exeSendThread(allToExe chan<- exeCmd, isDone <-chan bool, exeToCtl <-chan BmStats, port string) {

	var cxt *zmq.Context
	var pub *zmq.Socket
	var err error

	if cxt, err = zmq.NewContext(); err != nil {
		allToExe <- exeCmd{ErrorExit, "exeSendThread exception"}
		fmt.Println("err1=", err)
		return
	}

	if pub, err = cxt.NewSocket(zmq.PUB); err != nil {
		allToExe <- exeCmd{ErrorExit, "exeSendThread exception"}
		fmt.Println("err2=", err)
		return
	}
	defer pub.Close()

	if err = pub.Bind("tcp://*:" + port); err != nil {
		allToExe <- exeCmd{ErrorExit, "exeSendThread exception"}
		fmt.Println("err2=", err)
		return
	}

	allToExe <- exeCmd{Ready, "exeSendThread is ready"}
	for {
		select {
		case <-isDone:
			//_, _ = fmt.Fprintln(os.Stdout, "exeSendThread try to exit - 4")
			allToExe <- exeCmd{NormalExit, "exeSendThread"}
			return
		case stat := <-exeToCtl:
			fmt.Println("looping...")
			bytes := EncodeBmStat(&stat)
			//fmt.Println(bytes)
			if _, err := pub.SendMessage([][]byte{[]byte("stat"), bytes}, 0); err != nil {
				//fmt.Println("err3=", err)
			} else {
				//fmt.Println("send,", ret)
			}
		}
	}
}

/*
	exit condition: when isDone is closed
	upon exit: does not close any channel
	upon exception: no known possible exception, no exception signal
*/
func exeSignThread(allToExe chan<- exeCmd, isDone <-chan bool) {
	sigToExe := make(chan os.Signal)
	signal.Notify(sigToExe, syscall.SIGINT, syscall.SIGTERM)

	for {

		select {

		case <-isDone:
			signal.Stop(sigToExe)
			close(sigToExe)
			//_, _ = fmt.Fprintln(os.Stdout, "exeSignThread try to exit - 3")
			allToExe <- exeCmd{NormalExit, "exeSignThread"}
			return

		case <-sigToExe:
			//fmt.Println("GnfStop sending")
			allToExe <- exeCmd{GnfStop, "exeSignThread"}
			//fmt.Println("GnfStop sent")

		}
	}

}
