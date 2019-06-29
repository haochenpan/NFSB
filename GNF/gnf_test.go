package gnf

import (
	"fmt"
	_ "net/http/pprof"
	"testing"
)

func TestGnfMain(t *testing.T) {

	fPath := "/Users/roger/go/src/NFSB/Config/workload_template"
	phase := "loadrun"
	wl := InitWorkload()
	if ret := wl.UpdateWorkloadByFile(fPath); ret < 0 {
		err := fmt.Sprintf("no workload file found on path %q", fPath)
		t.Fatal(err)
	}

	var bmStat BmStats
	allToExe := make(chan exeCmd)
	isDone := make(chan bool)

	go exeSignThread(allToExe, isDone) // needToWait, exit when isDone is closed

	if phase == "load" {
		_, bmStat = benchmarkRoutine(wl, LoadPhase, allToExe)
	} else if phase == "run" {
		_, bmStat = benchmarkRoutine(wl, RunPhase, allToExe)
	} else if phase == "loadrun" {
		_, bmStat = benchmarkRoutine(wl, LoadPhase, allToExe)
		fmt.Println(bmStat.String())
		_, bmStat = benchmarkRoutine(wl, RunPhase, allToExe)
	} else {
		err := fmt.Sprintf("%q is not a valid argument", phase)
		t.Error(err)
	}

	fmt.Println(bmStat.String())
	close(isDone) // signal os signal thread
	<-allToExe    // wait for os signal thread to exit

}
