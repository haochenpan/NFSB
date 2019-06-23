package main

/*
	Operation generator interface and implementations
	A generator reads but does not modify workload parameters
	TODO (Roger): Zipf generator
	TODO (Roger): Optimize UniformOpGenerator
*/

import (
	"fmt"
	"os"
	"strconv"
)

type OpGenerator interface {
	GenThread(exeToGen <-chan bool, genToCli chan<- GenCmd,
		allToExe chan<- ExeCmd, wl *Workload, phase ExePhase)
}

type UniformOpGenerator struct {
}

/*
	exit condition: receive from exeToGen OR offered operation keys required by workload
	it closes genToCli channel
*/
func (gen *UniformOpGenerator) GenThread(exeToGen <-chan bool, genToCli chan<- GenCmd,
	allToExe chan<- ExeCmd, wl *Workload, phase ExePhase) {

	defer close(genToCli)
	var krs []int64
	var opCnt int
	var sig GenSig
	var cmd GenCmd

	if phase == LoadSig {
		krs = keyRangesToKeys(wl.RemoteDBInsertKeyRange)
		opCnt = len(krs)
	} else {
		krs = keyRangesToKeys(wl.RemoteDBOperationRange)
		opCnt = wl.RemoteDBOperationCount
	}

	// supports replay
	for i := 0; i < opCnt; i++ {
		if phase == LoadSig {
			cmd = GenCmd{WriteSig, wl.RemoteDBInsertValueSizeInByte, "user" + strconv.Itoa(int(krs[i]))}
		} else {
			//if (float64(ran.Int63()) / MaxIntInFloat) <= (wl.RemoteDBReadRatio) {
			if ran.Int63() <= int64(float64(wl.RemoteDBReadRatio)*MaxIntInFloat) {
				sig = ReadSig
			} else {
				sig = WriteSig
			}

			k := ran.Int63() % int64(len(krs))
			cmd = GenCmd{sig, wl.RemoteDBInsertValueSizeInByte, "user" + strconv.Itoa(int(krs[k]))}
		}

	retry:
		for {
			select {
			case <-exeToGen:
				_, _ = fmt.Fprintln(os.Stderr, "genThread return early")
				allToExe <- ExeCmd{ExitSig, "1"}
				return
			case genToCli <- cmd:
				break retry
			}
		}
	}

	_, _ = fmt.Fprintln(os.Stderr, "genThread return normally")
	allToExe <- ExeCmd{ExitSig, "0"}
}
