package gnf

/*
	GNF core components - goroutines and concurrent logic
*/

import (
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"time"
)

//func mockCliThread(genToCli <-chan genCmd) {
//	idx := 0
//
//	requests := make(map[string]int) // key, idx
//	reqs := make([]int, 0)
//
//	for cmd := range genToCli {
//		if v, ok := requests[cmd.Arg2]; ok {
//			reqs[v] ++
//		} else {
//			requests[cmd.Arg2] = idx
//			reqs = append(reqs, 1)
//			idx++
//		}
//	}
//	sum := 0
//	for i := range reqs {
//		sum += reqs[i]
//	}
//	fmt.Println(sum, len(reqs))
//	sort.Sort(sort.Reverse(sort.IntSlice(reqs)))
//	fmt.Println(reqs)
//}

/*
	exit condition: when genToCli is closed or upon exception in the load phase
	upon exit: does not close any channel
	upon exception: may send exeCmd{EExit, "cliThread exception"}
*/
func cliThread(genToCli <-chan genCmd, cliToSta chan<- stats, allToExe chan<- exeCmd,
	threadIndex int, db DBClient, phase exePhase) {

	var cliSrc = rand.NewSource(time.Now().UTC().UnixNano())

	var cliRan = rand.New(cliSrc)

	for cmd := range genToCli {
		op, cnt, key := cmd.Sig, cmd.Arg1, cmd.Arg2
		var err error

		var time1, time2 time.Time

		switch op {
		case ReadSig:
			time1 = time.Now()
			_, err = db.DBRead(key)

		case WriteSig:
			str := randString(cliRan, cnt)
			time1 = time.Now()
			err = db.DBWrite(key, str)
		}

		time2 = time.Now()
		if err != nil && phase == LoadSig {
			_, _ = fmt.Fprintln(os.Stdout, "cliThread tries to return early, idx=", threadIndex)
			cliToSta <- stats{threadIndex, false, cmd, time1, time2}
			allToExe <- exeCmd{EExit, "cliThread exception"}
			return
		} else if err != nil {
			cliToSta <- stats{threadIndex, false, cmd, time1, time2}
		} else {
			cliToSta <- stats{threadIndex, true, cmd, time1, time2}
		}
	}
	_, _ = fmt.Fprintln(os.Stdout, "cliThread tries to return normally, idx=", threadIndex)
	allToExe <- exeCmd{NExit, "cliThread"}

}

/*
	exit condition: when cliToSta is closed
	upon exit: close staToExe
	upon exception: no known possible exception, no exception signal

*/
func staThread(cliToSta <-chan stats, allToExe chan<- exeCmd, staToExe chan<- BmStats) {

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
	myIp, _ := getIp()
	bmStats := BmStats{
		IP:           myIp,
		Timestamp:    time.Now().String(),
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

	_, _ = fmt.Fprintln(os.Stdout, "staThread tries to return normally - 2")
	allToExe <- exeCmd{NExit, "staThread"}
	staToExe <- bmStats
	close(staToExe)

}

/*
	exit condition: receives GnfStop or BmStop or upon exception
	if the last executed command is BmStop: return returnWait
	if the last executed command is GnfStop: close isDone, return 0
	Upon exception: close isDone, return 0 for now

	if only processes BmStop, if there's a GnfStop signal comes in,
	it will notify executorRoutine to process that signal.

	we assume two pub sub threads and os signal thread are fault free when this method
	start to execute. However, when there's an error in pub thread, bm stats just get dropped

	// bool: has GnfStop been called?
	// BmStats:
*/
func benchmarkRoutine(wl *Workload, phase exePhase, allToExe chan exeCmd) (bool, BmStats) {

	exeToGen := make(chan bool)        // close after for loop
	genToCli := make(chan genCmd, 0)   // close by gen
	cliToSta := make(chan stats, 1000) // close after for loop
	staToExe := make(chan BmStats)     // close by sta

	gen := getOpGenerator(wl)
	go gen.GenThread(allToExe, exeToGen, genToCli, wl, phase) // need to wait

	clients := getRemoteDBClients(wl, phase)
	for i, cli := range clients {
		//for range clients {
		go cliThread(genToCli, cliToSta, allToExe, i, cli, phase)
		//go mockCliThread(genToCli)
	}

	go staThread(cliToSta, allToExe, staToExe)

	var bmStop, shouldGnfStop bool // has the bm stopped
	needToWait := len(clients) + 1 // waits generator but does not wait stat thread here

	var doBmExit = func() {
		if !bmStop {
			close(exeToGen)
			bmStop = true
		} else {
			_, _ = fmt.Fprintln(os.Stdout, "bm already exiting!")
		}
	}

	//var doGnfExit = func() {
	//	doBmExit()
	//	if !shouldGnfStop {
	//		needToWait, returnWait = needToWait+returnWait, 0
	//		close(isDone)
	//		shouldGnfStop = true
	//	} else {
	//		_, _ = fmt.Fprintln(os.Stdout, "gnf already exiting!")
	//
	//	}
	//}

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
				shouldGnfStop = true
				//doGnfExit()

			case GnfStop:
				fmt.Println("received interrupt sig=", cmd.arg)
				doBmExit()
				shouldGnfStop = true
				//doGnfExit()

			case CtrlLoad, CtrlRun:
				fmt.Println("already doing so", cmd.arg)

			case BmStop:
				fmt.Println("bmstop received, ", cmd.arg)
				doBmExit()
			}
		}
	}

	doBmExit()

	// at this point, only sta thread and 3 outside threads are running
	close(cliToSta) // notify sta thread the benchmark is done
	//fmt.Println("before wait stat")
	e := <-allToExe // wait stat's signal
	fmt.Println("from allToExe", e.arg)

	//fmt.Println("after wait stat")

	//fmt.Println("after cli to sta is closed, wait stat,", )
	bmStat := <-staToExe

	//fmt.Println("after wait bmStat")

	//fmt.Println(bmStat.String())
	//fmt.Println("before !bmStop")
	//fmt.Println("!bmStop")
	//exeToCtl <- bmStat // if gnf-cli, receive in GnfMain; if gnf, receive in executorRoutine
	//fmt.Println("returnWait=", returnWait)
	return shouldGnfStop, bmStat

}

/*
	exit condition: receives GnfStop or upon exception
	whether or not there's an exception,
	this function shall exit gracefully and release all resources,
	without incurring an deadlock
*/
func executorRoutine(controllerIp string, subPort, pubPort int) {

	allToExe := make(chan exeCmd)
	isDone := make(chan bool)
	exeToCtl := make(chan BmStats)

	//go func() {
	//	time.Sleep(2 * time.Second)
	//	allToExe <- exeCmd{CtrlLoad, "Config/workload_template"}
	//	time.Sleep(2 * time.Second)
	//	allToExe <- exeCmd{BmStop, ""}
	//	time.Sleep(2 * time.Second)
	//	allToExe <- exeCmd{GnfStop, ""}
	//}()

	go exeSignThread(allToExe, isDone)                                      // needToWait, exit when isDone is closed
	go exeRecvThread(allToExe, isDone, controllerIp, strconv.Itoa(subPort)) // needToWait, exit when isDone is closed
	go exeSendThread(allToExe, isDone, exeToCtl, strconv.Itoa(pubPort))     // needToWait, exit when isDone is closed

	for i := 0; i < 2; i++ {
		sig := <-allToExe
		if sig.sig != Ready {
			fmt.Println(sig.arg)
			return
		}
		fmt.Println(sig.arg)
	}
	var wl *Workload
	wl = InitWorkload()

	var hasGnfStopCalled, alreadyExit bool
	var bmStat BmStats
	var needToWait = 3
	var doExit = func() {
		if !alreadyExit {
			close(isDone)
			alreadyExit = true
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

			case CtrlLoad, CtrlRun:
				if ret := wl.UpdateWorkloadByFile(cmd.arg); ret < 0 {
					fmt.Println("wl file error")
					continue
				}
				hasGnfStopCalled, bmStat = benchmarkRoutine(wl, exePhase(cmd.sig), allToExe)
				exeToCtl <- bmStat
				if hasGnfStopCalled {
					doExit()
				}
			}
			//default:
			//	time.Sleep(1 * time.Second)
		}
	}

	fmt.Println("executorRoutine exits")
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

		var gnfStop bool
		allToExe := make(chan exeCmd)
		isDone := make(chan bool)
		//exeToCtl := make(chan BmStats, 0)

		var bmStat BmStats

		go exeSignThread(allToExe, isDone) // needToWait, exit when isDone is closed

		if *phase == "load" {
			gnfStop, bmStat = benchmarkRoutine(wl, LoadSig, allToExe)
		} else if *phase == "run" {
			gnfStop, bmStat = benchmarkRoutine(wl, RunSig, allToExe)
		} else if *phase == "loadrun" {
			gnfStop, bmStat = benchmarkRoutine(wl, LoadSig, allToExe)
			fmt.Println(bmStat.String())
			gnfStop, bmStat = benchmarkRoutine(wl, RunSig, allToExe)
		} else {
			err := fmt.Sprintf("%q is not a valid argument", *phase)
			return errors.New(err)
		}

		//fmt.Println("ret=", ret)
		//bm := <-exeToCtl
		fmt.Println("gnfStop=", gnfStop)
		fmt.Println(bmStat.String())
		//e := <-allToExe
		close(isDone)
		e := <-allToExe
		fmt.Println("from allToExe", e.arg)
		fmt.Println("main exits")
		//if ret != 0 {

		//}

		//close()

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
		//fmt.Println()
		//fmt.Println()
		//go mockSendController()
		//go mockRecvController()

		executorRoutine(*ip, *port, *stat)
	}

	return nil
}
