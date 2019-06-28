package gnf

import (
	"fmt"
	_ "net/http/pprof"
	"testing"
)

func TestGnfMain(t *testing.T) {

	fPath := "../workload0.txt"
	phase := "load"
	wl := InitWorkload()
	if ret := wl.UpdateWorkloadByFile(fPath); ret < 0 {
		err := fmt.Sprintf("no workload file found on path %q", fPath)
		t.Fatal(err)
	}

	var gnfStop bool
	allToExe := make(chan exeCmd)
	isDone := make(chan bool)
	//exeToCtl := make(chan BmStats, 0)

	var bmStat BmStats

	go exeSignThread(allToExe, isDone) // needToWait, exit when isDone is closed

	if phase == "load" {
		gnfStop, bmStat = benchmarkRoutine(wl, LoadSig, allToExe)
	} else if phase == "run" {
		gnfStop, bmStat = benchmarkRoutine(wl, RunSig, allToExe)
	} else if phase == "loadrun" {
		gnfStop, bmStat = benchmarkRoutine(wl, LoadSig, allToExe)
		fmt.Println(bmStat.String())
		gnfStop, bmStat = benchmarkRoutine(wl, RunSig, allToExe)
	} else {
		err := fmt.Sprintf("%q is not a valid argument", phase)
		t.Fatal(err)
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
}
