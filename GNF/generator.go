package gnf

/*
	Operation generator interface and implementations

	A generator reads but does not modify workload parameters
	TODO (Roger): Zipf generator
	TODO (Roger): Optimize UniformOpGenerator
*/

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
)

var src = rand.NewSource(714) // for generator only

var ran = rand.New(src) // for generator only

type OpGenerator interface {
	GenThread(exeToGen <-chan bool, genToCli chan<- genCmd,
		allToExe chan<- exeCmd, wl *Workload, phase exePhase)
}

type UniformOpGenerator struct {
}

/*
	exit condition: receive from exeToGen OR offered operation keys required by workload
	it closes genToCli channel
*/
func (gen *UniformOpGenerator) GenThread(exeToGen <-chan bool, genToCli chan<- genCmd,
	allToExe chan<- exeCmd, wl *Workload, phase exePhase) {

	var krs []int64
	var opCnt int
	var sig genSig
	var cmd genCmd

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
			cmd = genCmd{WriteSig, wl.RemoteDBInsertValueSizeInByte, "user" + strconv.Itoa(int(krs[i]))}
		} else {
			//if (float64(ran.Int63()) / MaxIntInFloat) <= (wl.RemoteDBReadRatio) {
			if ran.Int63() <= int64(float64(wl.RemoteDBReadRatio)*MaxIntInFloat) {
				sig = ReadSig
			} else {
				sig = WriteSig
			}

			k := ran.Int63() % int64(len(krs))
			cmd = genCmd{sig, wl.RemoteDBInsertValueSizeInByte, "user" + strconv.Itoa(int(krs[k]))}
		}

	retry:
		for {
			select {
			case <-exeToGen:
				_, _ = fmt.Fprintln(os.Stderr, "genThread return early")
				close(genToCli)
				allToExe <- exeCmd{NExit, "GenThread"}
				return
			case genToCli <- cmd:
				break retry
			}
		}
	}

	_, _ = fmt.Fprintln(os.Stderr, "genThread return normally")
	close(genToCli)
	allToExe <- exeCmd{NExit, "GenThread"}
}

func getOpGenerator(wl *Workload) OpGenerator {
	var gen OpGenerator
	switch wl.RemoteDBOperationDistribution {
	case "uniform":
		gen = &UniformOpGenerator{}
	default:
		panic(wl)
	}

	return gen
}