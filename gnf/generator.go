package gnf

import (
	"fmt"
	"os"
	"strconv"
)

type OpGenerator interface {
	GenThread(exeToGen <-chan bool, genToCli chan<- GenCmd, allToExe chan<- ExeCmd, wl *Workload, phase ExePhase)
}

type UniformOpGenerator struct {
}

/*
	exit condition: receive from exeToGen OR offered all operation keys
	it closes genToCli
	could be optimized, wait until zipf gen is done
*/
func (gen *UniformOpGenerator) GenThread(exeToGen <-chan bool, genToCli chan<- GenCmd, allToExe chan<- ExeCmd, wl *Workload, phase ExePhase) {

	defer close(genToCli)
	//var krsKeyIdx, randKeyIdx, keyCnt int
	var krs []int64
	var sig GenSig
	var cmd GenCmd
	var opCnt int

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
			if (float64(ran.Int63()) / MaxIntInFloat) <= (wl.RemoteDBReadRatio) {
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
				_, _ = fmt.Fprintln(os.Stderr, "GenThread return early")
				allToExe <- ExeCmd{ExitSig, "1"}
				return
			case genToCli <- cmd:
				//fmt.Println("sent")
				break retry
			default:
				//time.Sleep(5 * time.Millisecond)
				//fmt.Println("wait")
			}
		}
	}

	_, _ = fmt.Fprintln(os.Stderr, "GenThread return normally")
	allToExe <- ExeCmd{ExitSig, "0"}
}
