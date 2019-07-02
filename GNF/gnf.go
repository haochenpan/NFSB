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

/*
	GNF core components - goroutines and concurrent logic
	goroutines: client thread, statistics thread
	concurrent logic: during benchmark (benchmarkRoutine), listen to controller (mainRoutine)
*/

import (
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"time"
)

/*
	a client thread is the thread that performs DB operations
	there could be multiple client threads during benchmarking, depends on workload parameters

	exit condition: when genToCli is closed by generator or encountered an exception in the load phase
	upon exit: does not close any channel
	upon exception: may send exeCmd{ErrorExit, "cliThread exception"}

	genToCli: a channel from operation generator to client threads
	cliToSta: a channel from client threads to the statistics thread
	allToExe: a channel from the sender thread to the executor thread

*/
func cliThread(genToCli <-chan genCmd, cliToSta chan<- stats,
	allToExe chan<- exeCmd, threadIndex int, db DBClient, phase exePhase) {

	var cliSrc = rand.NewSource(time.Now().UTC().UnixNano())
	var cliRan = rand.New(cliSrc)
	var err error
	var time1, time2 time.Time

	// for every R/W command received from the operation generator thread
	for cmd := range genToCli {
		op, cnt, key := cmd.Sig, cmd.Arg1, cmd.Arg2

		// perform a R/W operation and measure the start and end time
		switch op {
		case DoRead:
			time1 = time.Now()
			_, err = db.DBRead(key)

		case DoWrite:
			str := randString(cliRan, cnt) // generate a random string as
			time1 = time.Now()
			err = db.DBWrite(key, str)
		}
		time2 = time.Now()

		// if there's an error in the load phase
		if err != nil && phase == LoadPhase {
			//_, _ = fmt.Fprintln(os.Stdout, "cliThread tries to return early, idx=", threadIndex)
			cliToSta <- stats{threadIndex, false, cmd, time1, time2}
			allToExe <- exeCmd{ErrorExit, "cliThread exception"}
			return

			// if there's an error in the run phase
		} else if err != nil {
			cliToSta <- stats{threadIndex, false, cmd, time1, time2}

			// if operation succeed
		} else {
			cliToSta <- stats{threadIndex, true, cmd, time1, time2}
		}
	}
	//_, _ = fmt.Fprintln(os.Stdout, "cliThread tries to return normally, idx=", threadIndex)
	allToExe <- exeCmd{NormalExit, "cliThread"}

}

/*
	a statistics thread converges stats objects from client threads and produces one benchmark stats
	there's only one statistics thread per benchmark phase (load/run)

	exit condition: when cliToSta is closed by benchmarkRoutine()
	upon exit: close staToExe
	upon exception: no known possible exception, no exception signal

	cliToSta: a channel from client threads to the statistics thread
	allToExe: a channel from the sender thread to the executor thread
	staToExe: a channel from the statistics thread to the executor thread

*/
func staThread(cliToSta <-chan stats, allToExe chan<- exeCmd, staToExe chan<- BmStats) {

	// successful read, successful write, failed read, failed write
	var sRead, sWrite, fRead, fWrite int
	// latencies of those operations
	var sReadLat, sWriteLat = make(latency, 0), make(latency, 0)
	var fReadLat, fWriteLat = make(latency, 0), make(latency, 0)

	bmStart, bmEnd := time.Now(), time.Now()

	// for every statistics object received from client threads
	for stat := range cliToSta {

		if sRead == 0 && sWrite == 0 && fRead == 0 && fWrite == 0 {
			bmStart = stat.start
		}
		bmEnd = stat.end
		duration := stat.end.Sub(stat.start)

		if stat.succeed && stat.genCmd.Sig == DoRead {
			sRead++
			sReadLat = append(sReadLat, duration)
		} else if stat.succeed && stat.genCmd.Sig == DoWrite {
			sWrite++
			sWriteLat = append(sWriteLat, duration)
		} else if !stat.succeed && stat.genCmd.Sig == DoRead {
			fRead++
			fReadLat = append(fReadLat, duration)
		} else {
			fWrite++
			fWriteLat = append(fWriteLat, duration)
		}
	}

	// sort 4 arrays to get percentile information
	sort.Sort(sReadLat)
	sort.Sort(sWriteLat)
	sort.Sort(fReadLat)
	sort.Sort(fWriteLat)

	runTime := bmEnd.Sub(bmStart).Seconds()
	myIp, _ := getIp()
	bmStats := BmStats{
		IP:         myIp,
		Timestamp:  time.Now().String(),
		Runtime:    runTime,
		Throughput: float64(sRead+sWrite+fRead+fWrite) / runTime,

		SRead:       sRead,
		SReadAvgLat: sReadLat.getAvgLat(),
		SRead95pLat: sReadLat.get95pLat(),

		SWrite:       sWrite,
		SWriteAvgLat: sWriteLat.getAvgLat(),
		SWrite95pLat: sWriteLat.get95pLat(),

		FRead:       fRead,
		FReadAvgLat: fReadLat.getAvgLat(),
		FRead95pLat: fReadLat.get95pLat(),

		FWrite:       fWrite,
		FWriteAvgLat: fWriteLat.getAvgLat(),
		FWrite95pLat: fWriteLat.get95pLat(),
	}

	//_, _ = fmt.Fprintln(os.Stdout, "staThread tries to return normally - 2")
	//allToExe <- exeCmd{NormalExit, "staThread"}
	staToExe <- bmStats
	close(staToExe)

}

/*
	executor's concurrent logic during benchmarking

	exit condition: receives GnfStop or BmkStop or an EEXIT signal from client threads
	upon exit: return a bool indicates whether the outer method mainRoutine() should exit the loop
				b/c it has received a GnfStop signal or encountered some exception
	upon exception: return (true, BmStats)

	need to make sure pub sub threads will not produce EEXIT signal
	during the execution of this method (mainRoutine handles that)

	wl: a pointer to a workload object
	phase: benchmark phase: load or run
	allToExe: a channel from the sender thread to the executor thread


*/
func benchmarkRoutine(wl *Workload, phase exePhase, allToExe chan exeCmd) (bool, BmStats) {

	exeToGen := make(chan bool)         // close by benchmarkRoutine
	genToCli := make(chan genCmd, 1000) // close by gen, 1000: as a buffer to smooth message passing
	cliToSta := make(chan stats, 1000)  // close by benchmarkRoutine
	staToExe := make(chan BmStats)      // close by sta, 1000: as a buffer to smooth message passing

	// starts the generator thread
	gen := getOpGenerator(wl)
	go gen.GenThread(allToExe, exeToGen, genToCli, wl, phase)

	// starts client threads
	clients := getRemoteDBClients(wl, phase)
	for i, cli := range clients {
		go cliThread(genToCli, cliToSta, allToExe, i, cli, phase)
	}

	// stats statistics thread
	go staThread(cliToSta, allToExe, staToExe)

	// has BmkStop signal received;
	// should GNF stop b/c an exception or GnfStop signal
	var bmStop, shouldGnfStop bool
	var doBmExit = func() {
		if !bmStop {
			close(exeToGen)
			bmStop = true
		} else {
			//_, _ = fmt.Fprintln(os.Stdout, "bm already exiting!")
		}
	}

	needToWait := len(clients) + 1 // only needs to wait the generator and client threads here
	for needToWait > 0 {

		cmd := <-allToExe

		switch cmd.sig {

		case NormalExit:
			fmt.Println("received exit=", cmd.arg)
			needToWait--

		case ErrorExit:
			fmt.Println("received exception exit=", cmd.arg)
			needToWait--
			doBmExit()
			shouldGnfStop = true

		case GnfStop:
			fmt.Println("received interrupt sig=", cmd.arg)
			doBmExit()
			shouldGnfStop = true

		case CtrlLoad, CtrlRun:
			fmt.Println("already doing so", cmd.arg)

		case BmkStop:
			fmt.Println("bmstop received, ", cmd.arg)
			doBmExit()

		}
	}

	doBmExit()

	// at this point, only sta thread and threads declared outside are running
	// notify sta thread the benchmark is done to get a BmStats
	close(cliToSta)

	bmStat := <-staToExe

	return shouldGnfStop, bmStat
}

/*
	executor's concurrent logic while not benchmarking (waiting for controller's signals)

	exit condition: receives GnfStop or an EEXIT signal from pub sub threads
	upon exit: hopefully release all resources without incurring a deadlock

*/
func mainRoutine(controllerIp string, subPort, pubPort int) {

	// allToExe: a channel from the sender thread to the executor thread
	// exeToCtl: a channel that sends benchmark statistics to the pub channel
	// isDone: used to signal exit
	allToExe := make(chan exeCmd)
	exeToCtl := make(chan BmStats)
	isDone := make(chan bool)

	go exeSignThread(allToExe, isDone)                                                // exit when isDone is closed
	go exeRecvThread(allToExe, isDone, controllerIp, strconv.Itoa(subPort))           // exit when isDone is closed
	go exeSendThread(allToExe, isDone, exeToCtl, controllerIp, strconv.Itoa(pubPort)) // exit when isDone is closed

	// wait for exeRecvThread and exeSendThread,
	// make sure they won't produce EEXIT signal from here on
	for i := 0; i < 2; i++ {
		sig := <-allToExe
		if sig.sig != Ready {
			fmt.Println("communication threads are not ready", sig.sig, sig.arg)
			return
		}
	}

	wl := InitWorkload()

	var doingExit bool
	var doExit = func() {
		if !doingExit {
			close(isDone)
			doingExit = true
		} else {
			//fmt.Println("already exiting!")
		}
	}

	// need to wait thread threads spawned above
	for needToWait := 3; needToWait > 0; {
		cmd := <-allToExe
		switch cmd.sig {
		case NormalExit:
			fmt.Println("exit received, ", cmd.arg)
			needToWait--

		case ErrorExit:
			fmt.Println("exception received, ", cmd.arg)
			needToWait--
			doExit()
			// ch close ?

		case GnfStop:
			fmt.Println("interrupt received, ", cmd.arg)
			doExit()

		case BmkStop:
			fmt.Println("bm is not going, signal ignored")

		case CtrlLoad, CtrlRun:
			if ret := wl.UpdateWorkloadByFile(cmd.arg); ret < 0 {
				fmt.Println("wl file error")
				continue
			}
			shouldExit, bmStat := benchmarkRoutine(wl, exePhase(cmd.sig), allToExe)
			exeToCtl <- bmStat
			if shouldExit {
				doExit()
			}
		}
	}
	//fmt.Println("mainRoutine exits")
}

/*
	gnf main method
	parses command line argument and spawns one of the two gnfs:
	gnf: gnf with controller, need to specify controller ip, nf port and stat port
	gnf-cli: gnf without controller, need to specify load/run phase and workload file

	returns some_error_with_msg if there's an exception
	or nil if everything works as intended
*/
func GnfMain() error {
	gnf := flag.NewFlagSet("gnf", flag.ExitOnError)
	ip := gnf.String("ip", "127.0.0.1", "controller ip")
	port := gnf.Int("port", 6667, "controller sub port")
	stat := gnf.Int("stat", 6668, "stats pub port")

	cli := flag.NewFlagSet("gnf-cli", flag.ExitOnError)
	phase := cli.String("phase", "load", " \"load\" or \"run\" or \"loadrun\" ")
	fPath := cli.String("wl", "./Config/workload_template", "some_workload_file_path")

	if len(os.Args) < 2 {
		fmt.Println()
		fmt.Printf("Please specify %q or %q as the first argument\n", "gnf", "gnf-cli")
		fmt.Println()
		fmt.Println("Usage of gnf:")
		gnf.PrintDefaults()
		fmt.Println()
		fmt.Println("Usage of cli:")
		cli.PrintDefaults()
		fmt.Println()
		return errors.New("not enough arguments")
	}

	switch os.Args[1] {
	case "gnf":
		if err := gnf.Parse(os.Args[2:]); err != nil {
			gnf.PrintDefaults()
			return errors.New("error in parsing gnf flags")
		}
	case "gnf-cli":
		if err := cli.Parse(os.Args[2:]); err != nil {
			gnf.PrintDefaults()
			return errors.New("error in parsing cli flags")
		}
	default:
		err := fmt.Sprintf("%q is not a valid argument", os.Args[1])
		return errors.New(err)
	}

	// gnf-cli
	if cli.Parsed() {

		wl := InitWorkload()
		if ret := wl.UpdateWorkloadByFile(*fPath); ret < 0 {
			err := fmt.Sprintf("no workload file found on path %q", *fPath)
			return errors.New(err)
		}

		var bmStat BmStats
		allToExe := make(chan exeCmd)
		isDone := make(chan bool)

		go exeSignThread(allToExe, isDone) // needToWait, exit when isDone is closed

		if *phase == "load" {
			_, bmStat = benchmarkRoutine(wl, LoadPhase, allToExe)
		} else if *phase == "run" {
			_, bmStat = benchmarkRoutine(wl, RunPhase, allToExe)
		} else if *phase == "loadrun" {
			_, bmStat = benchmarkRoutine(wl, LoadPhase, allToExe)
			fmt.Println(bmStat.String())
			_, bmStat = benchmarkRoutine(wl, RunPhase, allToExe)
		} else {
			err := fmt.Sprintf("%q is not a valid argument", *phase)
			return errors.New(err)
		}

		fmt.Println(bmStat.String())
		close(isDone) // signal os signal thread
		<-allToExe    // wait for os signal thread to exit
		//fmt.Println("from allToExe", e.arg)
		//fmt.Println("main exits")

		// gnf with a controller
	} else {

		if myIp, err := getIp(); err != nil {
			return errors.New("error in getting my ip")
		} else if myIp == "" {
			return errors.New("error in getting my ip")
		} else {
			fmt.Println("my ip=", myIp)
		}

		fmt.Println("controller ip=", *ip)
		fmt.Println("controller nf port=", *port)
		fmt.Println("controller stat port=", *stat)
		mainRoutine(*ip, *port, *stat)
		sendPingToController(*ip)
	}
	return nil
}

// Send Pong to Controller
func sendPingToController(controllerIP string) {
	var conn net.Conn
	var err error

	port := "6669"

	if conn, err = net.Dial("tcp", controllerIP+":"+port); err != nil {
		fmt.Println("err2=", err)
	} else {
		conn.Write([]byte("Alive"))
	}
}
