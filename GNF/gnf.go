package gnf

/*
	GNF core components - threads and concurrent logic
 */

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"
)

/*
	exit condition: genToCli is closed (by generator) OR one err in load phase
	it does not close any channel
*/
func cliThread(genToCli <-chan GenCmd, cliToSta chan<- Stats, allToExe chan<- ExeCmd,
	threadIndex int, db DBClient, phase ExePhase) {

	var cliRan = rand.New(src)
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
			cliToSta <- Stats{threadIndex, false, cmd, time1, time2}
			allToExe <- ExeCmd{ExceptionSig, "a db exception in load phase"}
			return
		} else if err != nil {
			cliToSta <- Stats{threadIndex, false, cmd, time1, time2}
		} else {
			cliToSta <- Stats{threadIndex, true, cmd, time1, time2}
		}
	}
	_, _ = fmt.Fprintln(os.Stderr, "cliThread return normally", threadIndex)
	allToExe <- ExeCmd{ExitSig, "0"}

}

/*
	exit condition: cliToSta is closed (by bmkMain)
	it does not close any channel
*/
func staThread(cliToSta <-chan Stats, allToExe chan<- ExeCmd) {

	var sRead, sWrite, fRead, fWrite int
	var sReadLat, sWriteLat = make(Latency, 0), make(Latency, 0)
	var fReadLat, fWriteLat = make(Latency, 0), make(Latency, 0)

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
		Runtime:    runTime,
		Throughput: float64(sRead+sWrite+fRead+fWrite) / runTime,
		SRead:      sRead,
		SWrite:     sWrite,
		FRead:      fRead,
		FWrite:     fWrite,
	}

	bmStats.SReadAvgLat = sReadLat.getAvgLat()
	bmStats.SRead95pLat = sReadLat.get95pLat()

	bmStats.SWriteAvgLat = sWriteLat.getAvgLat()
	bmStats.SWrite95pLat = sWriteLat.get95pLat()

	bmStats.FReadAvgLat = fReadLat.getAvgLat()
	bmStats.FReadAvgLat = fReadLat.get95pLat()

	bmStats.FWriteAvgLat = fWriteLat.getAvgLat()
	bmStats.FWriteAvgLat = fWriteLat.get95pLat()

	fmt.Print(bmStats.String())

	allToExe <- ExeCmd{ExitSig, "0"}

}

/*
	exit condition: receives InterruptSig from executor main ? OR
					receives enough ExitSig from other threads OR
					receives an ExceptionSig
	whether or not there's an exception,
	this function shall exit gracefully and release all resources,
	without incurring an deadlock

*/
func ExecutorThread(phase ExePhase, path string) {
	wl := InitWorkload()
	if ret := wl.UpdateWorkloadByFile(path); ret < 0 {
		_, _ = fmt.Fprintln(os.Stderr, "no workload file found")
	}

	var gen OpGenerator
	switch wl.RemoteDBOperationDistribution {
	case "uniform":
		gen = &UniformOpGenerator{}
	default:
		panic(wl)
	}

	exeToGen := make(chan bool)
	genToCli := make(chan GenCmd)
	allToExe := make(chan ExeCmd)
	cliToSta := make(chan Stats, 1000)

	sigToExe := make(chan os.Signal)
	signal.Notify(sigToExe, syscall.SIGINT, syscall.SIGTERM)

	go gen.GenThread(exeToGen, genToCli, allToExe, wl, phase)

	clients := getRemoteDBClients(wl, phase)

	for i, cli := range clients {
		go cliThread(genToCli, cliToSta, allToExe, i, cli, phase)
	}

	go staThread(cliToSta, allToExe)

	needToWait := len(clients) + 1
	for needToWait > 0 {
		select {
		case cmd := <-allToExe:
			switch cmd.Sig {
			case ExitSig:
				needToWait -= 1
			case ExceptionSig:
				needToWait -= 1
				exeToGen <- true
			}
		case <-sigToExe:
			_, _ = fmt.Fprintf(os.Stderr, "receive interrupt signal")
			exeToGen <- true
		}

	}
	close(cliToSta)
	<-allToExe
	_, _ = fmt.Fprintln(os.Stderr, "main return normally")

}
