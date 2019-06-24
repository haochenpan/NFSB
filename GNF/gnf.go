package gnf

/*
	GNF core components - threads and concurrent logic
*/

import (
	"flag"
	"fmt"
	"github.com/pkg/errors"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"time"
)

/*
	exit condition: genToCli is closed (by generator) OR one err in load phase
	it does not close any channel
*/
func cliThread(genToCli <-chan genCmd, cliToSta chan<- stats, allToExe chan<- exeCmd,
	threadIndex int, db DBClient, phase exePhase) {

	var cliSrc = rand.NewSource(int64(threadIndex + 714))
	var cliRan = rand.New(cliSrc)
	for cmd := range genToCli {
		op, cnt, key := cmd.Sig, cmd.Arg1, cmd.Arg2
		var err error

		time1 := time.Now()
		switch op {
		case ReadSig:
			_, err = db.DBRead(key)
		case WriteSig:
			str := randString(cliRan, cnt)
			time1 = time.Now()
			err = db.DBWrite(key, str)
		}

		time2 := time.Now()
		if err != nil && phase == LoadSig {
			_, _ = fmt.Fprintln(os.Stderr, "cliThread return early", threadIndex)
			cliToSta <- stats{threadIndex, false, cmd, time1, time2}
			allToExe <- exeCmd{EExit, "a db exception in load phase"}
			return
		} else if err != nil {
			cliToSta <- stats{threadIndex, false, cmd, time1, time2}
		} else {
			cliToSta <- stats{threadIndex, true, cmd, time1, time2}
		}
	}
	_, _ = fmt.Fprintln(os.Stderr, "cliThread return normally", threadIndex)
	allToExe <- exeCmd{NExit, "cliThread"}

}

/*
	exit condition: cliToSta is closed (by bmkMain)
	it does not close any channel
*/
func staThread(cliToSta <-chan stats, allToExe chan<- exeCmd, staToCtl chan<- BmStats) {

	var sRead, sWrite, fRead, fWrite int
	var sReadLat, sWriteLat = make(latency, 0), make(latency, 0)
	var fReadLat, fWriteLat = make(latency, 0), make(latency, 0)

	bmStart, bmEnd := time.Now(), time.Now()

	for stat := range cliToSta {

		if sRead == 0 && sWrite == 0 && fRead == 0 && fWrite == 0 {
			bmStart = stat.start
		}

		bmEnd = stat.end
		dur := stat.end.Sub(stat.start)
		if stat.succeed && stat.genCmd.Sig == ReadSig {
			sRead++
			sReadLat = append(sReadLat, dur)
		} else if stat.succeed && stat.genCmd.Sig == WriteSig {
			sWrite++
			sWriteLat = append(sWriteLat, dur)
		} else if ! stat.succeed && stat.genCmd.Sig == ReadSig {
			fRead++
			fReadLat = append(fReadLat, dur)
		} else {
			fWrite++
			fWriteLat = append(fWriteLat, dur)
		}
	}

	sort.Sort(sReadLat)
	sort.Sort(sWriteLat)
	sort.Sort(fReadLat)
	sort.Sort(fWriteLat)

	runTime := bmEnd.Sub(bmStart).Seconds()
	bmStats := BmStats{
		Runtime:      runTime,
		Throughput:   float64(sRead+sWrite+fRead+fWrite) / runTime,
		SRead:        sRead,
		SReadAvgLat:  sReadLat.getAvgLat(),
		SRead95pLat:  sReadLat.get95pLat(),
		SWrite:       sWrite,
		SWriteAvgLat: sWriteLat.getAvgLat(),
		SWrite95pLat: sWriteLat.get95pLat(),
		FRead:        fRead,
		FReadAvgLat:  fReadLat.getAvgLat(),
		FRead95pLat:  fReadLat.get95pLat(),
		FWrite:       fWrite,
		FWriteAvgLat: fWriteLat.getAvgLat(),
		FWrite95pLat: fWriteLat.get95pLat(),
	}

	allToExe <- exeCmd{NExit, "staThread"}
	staToCtl <- bmStats

}

/*
	BmStop: return 3
	GnfStop, return 0
	exception: return 0 for now
*/
func executorRoutine(wl *Workload, phase exePhase, allToExe chan exeCmd, isDone chan bool, exeToCtl chan BmStats) int {

	exeToGen := make(chan bool)
	genToCli := make(chan genCmd) // close by gen
	cliToSta := make(chan stats, 1000)
	staToExe := make(chan BmStats)

	gen := getOpGenerator(wl)
	go gen.GenThread(exeToGen, genToCli, allToExe, wl, phase) // need to wait

	clients := getRemoteDBClients(wl, phase)
	for i, cli := range clients {
		go cliThread(genToCli, cliToSta, allToExe, i, cli, phase)
	}

	go staThread(cliToSta, allToExe, staToExe)

	var bmStop, gnfStop bool               // has the bm stopped
	returnWait := 3                // from outside, 3 threads
	needToWait := len(clients) + 1 // waits generator but does not wait stat thread here
	//var doExit = func() {
	//	if !hasStop {
	//		needToWait, returnWait = needToWait+returnWait, 0
	//		close(exeToGen)
	//		close(isDone)
	//		hasStop = true
	//	} else {
	//		fmt.Println("bm already exiting!")
	//	}
	//}

	var doBmExit = func () {
		if !bmStop {
			close(exeToGen)
			bmStop = true
		} else {
			fmt.Println("bm already exiting!")
		}
	}

	var doGnfExit = func() {
		doBmExit()
		if !gnfStop {
			needToWait, returnWait = needToWait+returnWait, 0
			close(isDone)
			gnfStop = true
		} else {
			fmt.Println("gnf already exiting!")

		}
	}

	for needToWait > 0 {

		select {

		case cmd := <-allToExe:

			switch cmd.sig {

			case NExit:
				fmt.Println("received exit=", cmd.arg)
				needToWait--

			case EExit:
				fmt.Println("received exception exit=", cmd.arg)
				needToWait--
				doBmExit()

			case GnfStop:
				fmt.Println("received interrupt sig=", cmd.arg)
				doGnfExit()

			case ctlLoad, ctlRun:
				fmt.Println("already doing so", cmd.arg)

			case BmStop:
				fmt.Println("bmstop received, ", cmd.arg)
				doBmExit()
			}
		}
	}
	close(cliToSta)
	fmt.Println("after cli to sta is closed, wait stat,", <-allToExe)
	bmStat := <-staToExe
	//fmt.Println(bmStat.String())
	if returnWait > 0 {
		exeToCtl <- bmStat
	}

	return returnWait

}

/*
	exit condition: receives GnfStop from executor main ? OR
					receives enough NExit from other threads OR
					receives an EExit
	whether or not there's an exception,
	this function shall exit gracefully and release all resources,
	without incurring an deadlock

*/
//func bmkMain(wl *Workload, phase exePhase, exeToCtl chan BmStats) {
//
//	var gen OpGenerator
//	switch wl.RemoteDBOperationDistribution {
//	case "uniform":
//		gen = &UniformOpGenerator{}
//	default:
//		panic(wl)
//	}
//
//	exeToGen := make(chan bool)
//	genToCli := make(chan genCmd)
//	allToExe := make(chan exeCmd)
//	cliToSta := make(chan stats, 1000)
//	staToExe := make(chan BmStats, 1)
//
//	sigToExe := make(chan os.Signal)
//	signal.Notify(sigToExe, syscall.SIGINT, syscall.SIGTERM)
//
//	go gen.GenThread(exeToGen, genToCli, allToExe, wl, phase)
//
//	clients := getRemoteDBClients(wl, phase)
//
//	for i, cli := range clients {
//		go cliThread(genToCli, cliToSta, allToExe, i, cli, phase)
//	}
//
//	go staThread(cliToSta, allToExe, staToExe)
//
//	needToWait := len(clients) + 1
//	for needToWait > 0 {
//		select {
//		case cmd := <-allToExe:
//			switch cmd.sig {
//			case NExit:
//				needToWait -= 1
//			case EExit:
//				needToWait -= 1
//				exeToGen <- true
//			}
//		case <-sigToExe:
//			_, _ = fmt.Fprintf(os.Stderr, "receive interrupt signal")
//			exeToGen <- true
//		}
//
//	}
//	close(cliToSta)
//	<-allToExe
//	_, _ = fmt.Fprintln(os.Stderr, "main return normally")
//	return <-staToExe
//}

/*
	stop sig: nothing to interrupt, continue
	interrupt sig, shutdown gnf
*/
func executorMain(controllerIp string, subPort, pubPort int) {

	allToExe := make(chan exeCmd)
	isDone := make(chan bool)
	exeToCtl := make(chan BmStats)

	go func() {
		allToExe <- exeCmd{ctlRun, "./Config/workload_template"}
		//allToExe <- exeCmd{ctlLoad, "./Config/workload_template"}
		//allToExe <- exeCmd{ctlLoad, "./Config/workload_template"}
		//allToExe <- exeCmd{BmStop, "./Config/workload_template"}
		//allToExe <- exeCmd{BmStop, "./Config/workload_template"}
		//allToExe <- exeCmd{GnfStop, "./Config/workload_template"}
		//allToExe <- exeCmd{BmStop, "./Config/workload_template"}
		//time.Sleep(4 * time.Second)
		//allToExe <- exeCmd{BmStop, ""}
		//time.Sleep(4 * time.Second)
		//allToExe <- exeCmd{ctlLoad, "./Config/workload_template"}

	}()
	go exeSignThread(allToExe, isDone)                                      // needToWait, exit when isDone is closed
	go exeRecvThread(allToExe, isDone, controllerIp, strconv.Itoa(subPort)) // needToWait, exit when isDone is closed
	go exeSendThread(allToExe, isDone, exeToCtl, strconv.Itoa(pubPort))     // needToWait, exit when isDone is closed

	var wl *Workload
	wl = InitWorkload()

	var hasDone bool
	var needToWait = 3
	var doExit = func() {
		if !hasDone {
			close(isDone)
			hasDone = true
		} else {
			fmt.Println("already exiting!")
		}
	}
	for needToWait > 0 {
		select {
		case cmd := <-allToExe:
			switch cmd.sig {
			case NExit:
				fmt.Println("exit received, ", cmd.arg)
				needToWait--

			case EExit:
				fmt.Println("exception received, ", cmd.arg)
				needToWait--
				doExit()
				// ch close ?

			case GnfStop:
				fmt.Println("interrupt received, ", cmd.arg)
				doExit()

			case BmStop:
				fmt.Println("bm is not going, signal ignored")

			case ctlLoad, ctlRun:
				if ret := wl.UpdateWorkloadByFile(cmd.arg); ret < 0 {
					fmt.Println("wl file error")
					continue
				}
				needToWait = executorRoutine(wl, exePhase(cmd.sig), allToExe, isDone, exeToCtl)
			}
		default:
			time.Sleep(1 * time.Second)
		}
	}

	fmt.Println("executorMain exits")
}

/*
	gnf main method
	parses command line argument and spawns one of the two gnfs:
	gnf: gnf with controller, need to specify controller ip, nf port and stat port
	gnf-cli: gnf without controller, need to specify load/run phase and workload file

	returns some_error_with_msg if there's an exception
	or nil if everything works as intended
*/
func Main() error {

	gnf := flag.NewFlagSet("gnf", flag.ExitOnError)
	ip := gnf.String("ip", "127.0.0.1", "controller ip")
	port := gnf.Int("port", 6667, "controller sub port")
	stat := gnf.Int("stat", 6668, "stats pub port")

	cli := flag.NewFlagSet("gnf-cli", flag.ExitOnError)
	phase := cli.String("phase", "load", " \"load\" or \"run\" ")
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

	if cli.Parsed() {

		wl := InitWorkload()
		if ret := wl.UpdateWorkloadByFile(*fPath); ret < 0 {
			err := fmt.Sprintf("no workload file found on path %q", *fPath)
			return errors.New(err)
		}

		if *phase == "load" {
			panic("not now")
			//bmkMain(wl, LoadSig)
		} else if *phase == "run" {
			panic("not now")
			//bmkMain(wl, RunSig)
		} else {
			err := fmt.Sprintf("%q is not a valid argument", *phase)
			return errors.New(err)
		}

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
		fmt.Println()
		fmt.Println()
		go mockRecvController()
		executorMain(*ip, *port, *stat)
	}

	return nil
}
