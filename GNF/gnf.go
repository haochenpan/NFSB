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

func mockCliThread(genToCli <-chan genCmd) {
	idx := 0

	requests := make(map[string]int) // key, idx
	reqs := make([]int, 0)
	for cmd := range genToCli {
		if v, ok := requests[cmd.Arg2]; ok {
			//fmt.Println(v)
			reqs[v] ++
		} else {
			requests[cmd.Arg2] = idx
			reqs = append(reqs, 1)
			idx++
		}
	}
	sum := 0
	for i := range reqs {
		sum += reqs[i]
	}
	fmt.Println(sum, len(reqs))
	sort.Sort(sort.Reverse(sort.IntSlice(reqs)))
	fmt.Println(reqs)
}

/*
	exit condition: when genToCli is closed or upon exception in the load phase
	upon exit: does not close any channel
	upon exception: may send exeCmd{EExit, "cliThread exception"}
*/
func cliThread(genToCli <-chan genCmd, cliToSta chan<- stats, allToExe chan<- exeCmd,
	threadIndex int, db DBClient, phase exePhase) {

	var cliSrc = rand.NewSource(int64(threadIndex + 714))
	var cliRan = rand.New(cliSrc)
	for cmd := range genToCli {
		op, cnt, key := cmd.Sig, cmd.Arg1, cmd.Arg2
		var err error

		//fmt.Println(key)
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
			_, _ = fmt.Fprintln(os.Stdout, "cliThread return early", threadIndex)
			cliToSta <- stats{threadIndex, false, cmd, time1, time2}
			allToExe <- exeCmd{EExit, "cliThread exception"}
			return
		} else if err != nil {
			cliToSta <- stats{threadIndex, false, cmd, time1, time2}
		} else {
			cliToSta <- stats{threadIndex, true, cmd, time1, time2}
		}
	}
	_, _ = fmt.Fprintln(os.Stdout, "cliThread return normally", threadIndex)
	allToExe <- exeCmd{NExit, "cliThread"}

}

/*
	exit condition: when cliToSta is closed
	upon exit: does not close any channel
	upon exception: no known possible exception, no exception signal

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

	allToExe <- exeCmd{NExit, "staThread"}
	fmt.Println("staThread exits")
	staToCtl <- bmStats

}

/*
	exit condition: receives GnfStop or BmStop or upon exception
	if the last executed command is BmStop: return returnWait
	if the last executed command is GnfStop: close isDone, return 0
	Upon exception: close isDone, return 0 for now
*/
func bmkRoutine(wl *Workload, phase exePhase, allToExe chan exeCmd, isDone chan bool,
	exeToCtl chan BmStats, returnWait int) int {

	exeToGen := make(chan bool)        // close after for loop
	genToCli := make(chan genCmd, 0)   // close by gen
	cliToSta := make(chan stats, 1000) // close after for loop
	staToExe := make(chan BmStats)     // close after for loop

	gen := getOpGenerator(wl)
	go gen.GenThread(allToExe, exeToGen, genToCli, wl, phase) // need to wait

	clients := getRemoteDBClients(wl, phase)
	for i, cli := range clients {
		//for range clients {
		go cliThread(genToCli, cliToSta, allToExe, i, cli, phase)
		//go mockCliThread(genToCli)
	}

	go staThread(cliToSta, allToExe, staToExe)

	//returnWait := 3              // from outside, 3 threads
	var bmStop, gnfStop bool       // has the bm stopped
	needToWait := len(clients) + 1 // waits generator but does not wait stat thread here

	var doBmExit = func() {
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

			case CtrlLoad, CtrlRun:
				fmt.Println("already doing so", cmd.arg)

			case BmStop:
				fmt.Println("bmstop received, ", cmd.arg)
				doBmExit()
			}
		}
	}
	if !bmStop {
		close(exeToGen)
	}
	close(cliToSta)
	<-allToExe // wait stat
	//fmt.Println("after cli to sta is closed, wait stat,", )
	bmStat := <-staToExe
	close(staToExe)
	//fmt.Println(bmStat.String())
	if !bmStop {
		exeToCtl <- bmStat
	}

	return returnWait

}

/*
	exit condition: receives GnfStop or upon exception
	whether or not there's an exception,
	this function shall exit gracefully and release all resources,
	without incurring an deadlock
*/
func exeRoutine(controllerIp string, subPort, pubPort int) {

	allToExe := make(chan exeCmd)
	isDone := make(chan bool)
	exeToCtl := make(chan BmStats)

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

			case CtrlLoad, CtrlRun:
				if ret := wl.UpdateWorkloadByFile(cmd.arg); ret < 0 {
					fmt.Println("wl file error")
					continue
				}
				needToWait = bmkRoutine(wl, exePhase(cmd.sig), allToExe, isDone, exeToCtl, needToWait)
			}
		default:
			time.Sleep(1 * time.Second)
		}
	}

	fmt.Println("exeRoutine exits")
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

		allToExe := make(chan exeCmd)
		isDone := make(chan bool)
		exeToCtl := make(chan BmStats, 1)

		go exeSignThread(allToExe, isDone) // needToWait, exit when isDone is closed

		if *phase == "load" {
			bmkRoutine(wl, LoadSig, allToExe, isDone, exeToCtl, 1)
		} else if *phase == "run" {
			bmkRoutine(wl, RunSig, allToExe, isDone, exeToCtl, 1)
		} else {
			err := fmt.Sprintf("%q is not a valid argument", *phase)
			return errors.New(err)
		}

		bm := <-exeToCtl

		fmt.Println(bm.String())
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
		fmt.Println()
		fmt.Println()
		//go mockSendController()
		//go mockRecvController()
		exeRoutine(*ip, *port, *stat)
	}

	return nil
}
