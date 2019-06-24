package gnf

import (
	Utility "NFSB/Config"
	"NFSB/DataStruct"
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func mockSendController() {
	context, _ := zmq.NewContext()

	publisher, _ := context.NewSocket(zmq.PUB)
	defer publisher.Close()

	_ = publisher.Bind("tcp://*:6667")
	fmt.Println("sleeping....")
	time.Sleep(3 * time.Second)

	for i := 0; i < 5; i++ {
		fmt.Println("sleeping....")
		time.Sleep(1 * time.Second)
		fmt.Println("sending....")
		_, _ = publisher.SendMessage(
			[][]byte{
				[]byte("all!!"), //this act as a filter
				DataStruct.Encode(&DataStruct.UserData{Action: strconv.Itoa(i)})},
			0)
		fmt.Println("sent!")

	}
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
		fmt.Println("got !!!")
		data := DecodeBmStat(tuple[1])
		fmt.Println(data.String())
	}
	fmt.Println("mockRecvController exit")
}

/*
	exit condition: when something is sent through channel isDone (by executor)
	does not close any channel
*/
func exeRecvThread(allToExe chan<- exeCmd, isDone <-chan bool, ip string, port string) {

	var cxt *zmq.Context
	var sub *zmq.Socket
	var err error

	if cxt, err = zmq.NewContext(); err != nil {
		allToExe <- exeCmd{EExit, "exeRecvThread exception"}
		//fmt.Println("err1=", err)
		return
	}
	if sub, err = cxt.NewSocket(zmq.SUB); err != nil {
		allToExe <- exeCmd{EExit, "exeRecvThread exception"}
		//fmt.Println("err2=", err)
		return
	}
	defer sub.Close()

	if err = sub.Connect("tcp://" + ip + ":" + port); err != nil {
		allToExe <- exeCmd{EExit, "exeRecvThread exception"}
		//fmt.Println("err3=", err)
		return
	}
	if err = sub.SetSubscribe("all"); err != nil {
		allToExe <- exeCmd{EExit, "exeRecvThread exception"}
		//fmt.Println("err4=", err)
		return
	}

	myIp, _ := getIp() // If migrate to different servers, need to pass in the address
	if err = sub.SetSubscribe(myIp); err != nil {
		allToExe <- exeCmd{EExit, "exeRecvThread exception"}
		//fmt.Println("err5=", err)
		return
	}


	for {
		select {
		case <-isDone:
			fmt.Println("exeRecvThread try to exit")
			allToExe <- exeCmd{NExit, "exeRecvThread"}
			return
			// gracefully exit
		default:
			var tuple [][] byte // two tuple: filter and msg
			var err error
			if tuple, err = sub.RecvMessageBytes(zmq.DONTWAIT); err != nil {
				//fmt.Println("err6=", err)
				//fmt.Println("sleep for one sec")
				time.Sleep(1 * time.Second)
				continue
			}
			data := DataStruct.Decode(tuple[1])
			Utility.PrintUserData(data)
			// test ip field
			// switch on data action
		}

	}
}

/*
	exit condition: wait on isDone
	does not close any channel
*/
func exeSendThread(allToExe chan<- exeCmd, isDone chan bool, exeToCtl <-chan BmStats, port string) {

	var cxt *zmq.Context
	var pub *zmq.Socket
	var err error

	//fmt.Println("port,", port)
	if cxt, err = zmq.NewContext(); err != nil {
		allToExe <- exeCmd{EExit, "exeSendThread exception"}
		//fmt.Println("err1=", err)
		return
	}

	if pub, err = cxt.NewSocket(zmq.PUB); err != nil {
		allToExe <- exeCmd{EExit, "exeSendThread exception"}
		//fmt.Println("err2=", err)
		return
	}
	defer pub.Close()

	if err = pub.Bind("tcp://*:" + port); err != nil {
		allToExe <- exeCmd{EExit, "exeSendThread exception"}
		//fmt.Println("err2=", err)
		return
	}

	//time.Sleep(1*time.Second)
	//allToExe <- exeCmd{EExit, "exeRecvThread exception"}
	//return
	//fmt.Println("before for loop")
	for {
		select {
		case <-isDone:
			fmt.Println("exeSendThread try to exit")
			allToExe <- exeCmd{NExit, "exeSendThread"}
			return
		case stat := <-exeToCtl:
			//fmt.Println("looping...")
			bytes := EncodeBmStat(&stat)
			//fmt.Println(bytes)
			if ret, err := pub.SendMessage([][]byte{[]byte("stat"), bytes}, 0); err != nil {
				fmt.Println("err3=", err)
			} else {
				fmt.Println("send,", ret)
			}
		}
	}
}

func exeSignThread(allToExe chan<- exeCmd, isDone <-chan bool) {
	sigToExe := make(chan os.Signal)
	signal.Notify(sigToExe, syscall.SIGINT, syscall.SIGTERM)

	for {

		select {

		case <-isDone:
			signal.Stop(sigToExe)
			close(sigToExe)
			fmt.Println("exeSignThread try to exit")
			allToExe <- exeCmd{NExit, "exeSignThread"}
			return

		case <-sigToExe:
			fmt.Println("GnfStop sending")
			allToExe <- exeCmd{GnfStop, "exeSignThread"}
			fmt.Println("GnfStop sent")

		}
	}

}
