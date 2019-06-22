package gnf

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
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
			str := RandStringBytesRmndr(cliRan, cnt)
			time1 = time.Now()
			err = db.DBWrite(key, str)
		}

		time2 := time.Now()
		if err != nil && phase == LoadSig {
			_, _ = fmt.Fprintln(os.Stderr, "cliThread return early", threadIndex)
			cliToSta <- Stats{threadIndex, false, cmd, time1, time2}
			allToExe <- ExeCmd{Exception, "a db exception in load phase"}
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
	exit condition: statCh is closed
	it does not close any channel
*/
func StaThread(cliToSta <-chan Stats, allToExe chan<- ExeCmd) {
	var successRead, successWrite, failRead, failWrite int
	var successReadLat, successWriteLat, failReadLat, failWriteLat []time.Duration

	bmStart, bmEnd := time.Now(), time.Now()
	for stat := range cliToSta {
		dur := stat.end.Sub(stat.start)

		if successRead == 0 && successWrite == 0 && failRead == 0 && failWrite == 0 {
			//fmt.Println("first op arrived")
			bmStart = stat.start
		}

		bmEnd = stat.end

		if stat.succeed && stat.genCmd.Sig == ReadSig {
			successRead++
			successReadLat = append(successReadLat, dur)
		} else if stat.succeed && stat.genCmd.Sig == WriteSig {
			successWrite++
			successWriteLat = append(successWriteLat, dur)
		} else if ! stat.succeed && stat.genCmd.Sig == ReadSig {
			failRead++
			failReadLat = append(failReadLat, dur)
		} else {
			failWrite++
			failWriteLat = append(failWriteLat, dur)
		}
	}

	bmDur := bmEnd.Sub(bmStart)

	sort.Slice(successReadLat, func(i, j int) bool { return successReadLat[i].Nanoseconds() < successReadLat[j].Nanoseconds() })
	sort.Slice(successWriteLat, func(i, j int) bool { return successWriteLat[i].Nanoseconds() < successWriteLat[j].Nanoseconds() })
	sort.Slice(failReadLat, func(i, j int) bool { return failReadLat[i].Nanoseconds() < failReadLat[j].Nanoseconds() })
	sort.Slice(failWriteLat, func(i, j int) bool { return failWriteLat[i].Nanoseconds() < failWriteLat[j].Nanoseconds() })

	bcStat := BmStats{
		Runtime:    bmDur.Seconds(),
		Throughput: float64(successRead+successWrite+failRead+failWrite) / bmDur.Seconds(),
		SRead:      successRead,
		SWrite:     successWrite,
		FRead:      failRead,
		FWrite:     failWrite,
	}

	if bcStat.SRead > 0 {
		bcStat.SReadAvgLat = float64(successReadLat[int(float64(len(successReadLat))*0.5)]) / float64(time.Microsecond)
		bcStat.SRead95pLat = float64(successReadLat[int(float64(len(successReadLat))*0.95)]) / float64(time.Microsecond)
	}

	if bcStat.SWrite > 0 {
		bcStat.SWriteAvgLat = float64(successWriteLat[int(float64(len(successWriteLat))*0.5)]) / float64(time.Microsecond)
		bcStat.SWrite95pLat = float64(successWriteLat[int(float64(len(successWriteLat))*0.95)]) / float64(time.Microsecond)
	}

	if bcStat.FRead > 0 {
		bcStat.FReadAvgLat = float64(failReadLat[int(float64(len(failReadLat))*0.5)]) / float64(time.Microsecond)
		bcStat.FRead95pLat = float64(failReadLat[int(float64(len(failReadLat))*0.95)]) / float64(time.Microsecond)
	}

	if bcStat.FWrite > 0 {
		bcStat.FWriteAvgLat = float64(failWriteLat[int(float64(len(failWriteLat))*0.5)]) / float64(time.Microsecond)
		bcStat.FWrite95pLat = float64(failWriteLat[int(float64(len(failWriteLat))*0.95)]) / float64(time.Microsecond)
	}

	fmt.Print(bcStat.String())

	allToExe <- ExeCmd{ExitSig, "0"}

}

/*
	exit condition: receives InterruptSig from executor main ? OR
					receives enough ExitSig from other threads OR
					receives an Exception
	whether or not there's an exception,
	this function shall exit gracefully and release all resources,
	without incurring an deadlock

 */
func ExecutorThread(phase ExePhase, path string) {
	wl := InitWorkload()
	wl.UpdateWorkloadByFile(path)

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

	go gen.GenThread(exeToGen, genToCli, allToExe, wl, phase)

	clients := wl.GetRemoteDBClients(phase)

	for i, cli := range clients {
		go cliThread(genToCli, cliToSta, allToExe, i, cli, phase)
	}

	go StaThread(cliToSta, allToExe)

	needToWait := len(clients) + 1
	for needToWait > 0 {
		select {
		case cmd := <-allToExe:
			switch cmd.Sig {
			case InterruptSig:
				exeToGen <- true
			case ExitSig:
				needToWait -= 1
			case Exception:
				needToWait -= 1
				exeToGen <- true
			}
		}

	}
	close(cliToSta)
	<-allToExe
	_, _ = fmt.Fprintln(os.Stderr, "main return normally")

}
