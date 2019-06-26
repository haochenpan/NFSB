package gnf

import (
	"NFSB/DataStruct"
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func mockSendController() {
	context, _ := zmq.NewContext()

	publisher, _ := context.NewSocket(zmq.PUB)
	defer publisher.Close()

	_ = publisher.Bind("tcp://*:6667")
	fmt.Println("sleeping....")
	time.Sleep(1 * time.Second)

	//for i := 0; i < 5; i++ {
	//	fmt.Println("sleeping....")
	//	time.Sleep(1 * time.Second)
	//	fmt.Println("sending....")
	//
	//
	//	fmt.Println("sent!")
	//
	//}

	//_, _ = publisher.SendMessage([][]byte{[]byte("all"),
	//	DataStruct.Encode(&DataStruct.UserData{Action: "interrupt"})}, 0)
	//_, _ = publisher.SendMessage([][]byte{[]byte("all"),
	//	DataStruct.Encode(&DataStruct.UserData{
	//		Action:       "load",
	//		NewWorkLoad:  true,
	//		WorkLoadFile: ""})}, 0)
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
	upon exception: may send exeCmd{EExit, "exeRecvThread exception"}
*/
func exeRecvThread(allToExe chan<- exeCmd, isDone <-chan bool, ip string, port string) {

	var cxt *zmq.Context
	var sub *zmq.Socket
	var err error

	if cxt, err = zmq.NewContext(); err != nil {
		allToExe <- exeCmd{EExit, "exeRecvThread exception"}
		fmt.Println("err1=", err)
		return
	}
	if sub, err = cxt.NewSocket(zmq.SUB); err != nil {
		allToExe <- exeCmd{EExit, "exeRecvThread exception"}
		fmt.Println("err2=", err)
		return
	}
	defer sub.Close()

	if err = sub.Connect("tcp://" + ip + ":" + port); err != nil {
		allToExe <- exeCmd{EExit, "exeRecvThread exception"}
		fmt.Println("err3=", err)
		return
	}
	if err = sub.SetSubscribe("all"); err != nil {
		allToExe <- exeCmd{EExit, "exeRecvThread exception"}
		fmt.Println("err4=", err)
		return
	}

	myIp, _ := getIp() // If migrate to different servers, need to pass in the address
	if err = sub.SetSubscribe(myIp); err != nil {
		allToExe <- exeCmd{EExit, "exeRecvThread exception"}
		fmt.Println("err5=", err)
		return
	}

	allToExe <- exeCmd{Ready, "exeRecvThread is ready"}
	for {
		select {
		case <-isDone:
			_, _ = fmt.Fprintln(os.Stdout, "exeRecvThread try to exit - 5")
			allToExe <- exeCmd{NExit, "exeRecvThread"}
			return
			// gracefully exit
		default:
			var tuple [][] byte // three tuple: filter/flag, msg, msg_section
			var err error
			if tuple, err = sub.RecvMessageBytes(zmq.DONTWAIT); err != nil {
				//fmt.Println("err6=", err)
				//fmt.Println("sleep for one sec")
				time.Sleep(1 * time.Second)
				continue
			}
			data := DataStruct.Decode(tuple[1])
			switch data.Action {
			case "quit":
				allToExe <- exeCmd{GnfStop, ""}
			case "interrupt":
				allToExe <- exeCmd{BmStop, ""}
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
	upon exception: may send exeCmd{EExit, "exeSendThread exception"}
*/
func exeSendThread(allToExe chan<- exeCmd, isDone <-chan bool, exeToCtl <-chan BmStats, port string) {

	var cxt *zmq.Context
	var pub *zmq.Socket
	var err error

	//fmt.Println("port,", port)
	if cxt, err = zmq.NewContext(); err != nil {
		allToExe <- exeCmd{EExit, "exeSendThread exception"}
		fmt.Println("err1=", err)
		return
	}

	if pub, err = cxt.NewSocket(zmq.PUB); err != nil {
		allToExe <- exeCmd{EExit, "exeSendThread exception"}
		fmt.Println("err2=", err)
		return
	}
	defer pub.Close()

	if err = pub.Bind("tcp://*:" + port); err != nil {
		allToExe <- exeCmd{EExit, "exeSendThread exception"}
		fmt.Println("err2=", err)
		return
	}

	//time.Sleep(1*time.Second)
	//allToExe <- exeCmd{EExit, "exeRecvThread exception"}
	//return
	//fmt.Println("before for loop")

	allToExe <- exeCmd{Ready, "exeSendThread is ready"}
	for {
		select {
		case <-isDone:
			_, _ = fmt.Fprintln(os.Stdout, "exeSendThread try to exit - 4")
			allToExe <- exeCmd{NExit, "exeSendThread"}
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
			_, _ = fmt.Fprintln(os.Stdout, "exeSignThread try to exit - 3")
			allToExe <- exeCmd{NExit, "exeSignThread"}
			return

		case <-sigToExe:
			//fmt.Println("GnfStop sending")
			allToExe <- exeCmd{GnfStop, "exeSignThread"}
			//fmt.Println("GnfStop sent")

		}
	}

}
