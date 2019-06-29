package gnf

/*
	Operation generator interface and implementations

	A generator reads but does not modify workload parameters

	only supports uniform and zipfian distribution for now
*/

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"
)

type OpGenerator interface {
	/*
		exit condition: receive from exeToGen OR offered operation keys required by the workload
		upon exit: it closes genToCli channel
		upon exception: no known possible exception, no exception signal
	*/
	GenThread(allToExe chan<- exeCmd, exeToGen <-chan bool, genToCli chan<- genCmd, wl *Workload, phase exePhase)
}

type UniformOpGenerator struct {
}
type ZipfianOpGenerator struct {
}

func (gen *UniformOpGenerator) GenThread(allToExe chan<- exeCmd, exeToGen <-chan bool, genToCli chan<- genCmd,
	wl *Workload, phase exePhase) {

	var krs []int64
	var sig genSig
	var cmd genCmd
	var src = rand.NewSource(time.Now().UTC().UnixNano())
	var ran = rand.New(src)

	initGenerator(wl, phase, &krs)

	if phase == LoadPhase {
		for _, key := range krs {
			cmd = genCmd{DoWrite, wl.RemoteDBInsertValueSizeInByte, "user" + strconv.Itoa(int(key))}

			// retry send
			if ret := retrySend(allToExe, exeToGen, genToCli, cmd); ret == 1 {
				return
			}

		}
	} else {
		for i := 0; i < wl.RemoteDBOperationCount; i++ {
			// decide read or write
			a := ran.Int63()
			b := int64(float64(wl.RemoteDBReadRatio) * MaxIntInFloat)
			if a <= b {
				sig = DoRead
			} else {
				sig = DoWrite
			}

			// select the key
			k := ran.Int63() % int64(len(krs))
			cmd = genCmd{sig, wl.RemoteDBInsertValueSizeInByte, "user" + strconv.Itoa(int(krs[k]))}

			// retry send
			if ret := retrySend(allToExe, exeToGen, genToCli, cmd); ret == 1 {
				return
			}
		}
	}

	//_, _ = fmt.Fprintln(os.Stdout, "genThread return normally - 1")
	close(genToCli)
	allToExe <- exeCmd{NormalExit, "GenThread"}

}

func (gen *ZipfianOpGenerator) GenThread(allToExe chan<- exeCmd, exeToGen <-chan bool, genToCli chan<- genCmd,
	wl *Workload, phase exePhase) {
	var krs []int64
	var sig genSig
	var cmd genCmd
	var src = rand.NewSource(time.Now().UTC().UnixNano())
	var ran = rand.New(src)
	var zipf = rand.NewZipf(ran, 1.03, 1, uint64(len(krs)))

	initGenerator(wl, phase, &krs)

	if phase == LoadPhase {
		for _, key := range krs {
			cmd = genCmd{DoWrite, wl.RemoteDBInsertValueSizeInByte, "user" + strconv.Itoa(int(key))}
			// retry send
			if ret := retrySend(allToExe, exeToGen, genToCli, cmd); ret == 1 {
				return
			}
		}
	} else {
		for i := 0; i < wl.RemoteDBOperationCount; i++ {
			// decide read or write
			a := ran.Int63()
			b := int64(float64(wl.RemoteDBReadRatio) * MaxIntInFloat)
			if a <= b {
				sig = DoRead
			} else {
				sig = DoWrite
			}

			k := zipf.Uint64() % uint64(len(krs))
			cmd = genCmd{sig, wl.RemoteDBInsertValueSizeInByte, "user" + strconv.Itoa(int(krs[k]))}

			// retry send
			if ret := retrySend(allToExe, exeToGen, genToCli, cmd); ret == 1 {
				return
			}
		}
	}

	_, _ = fmt.Fprintln(os.Stdout, "genThread return normally - 1")
	close(genToCli)
	allToExe <- exeCmd{NormalExit, "GenThread"}

}

func getOpGenerator(wl *Workload) OpGenerator {
	var gen OpGenerator
	switch wl.RemoteDBOperationDistribution {
	case "uniform":
		gen = &UniformOpGenerator{}
	case "zipfian":
		gen = &ZipfianOpGenerator{}
	default:
		panic(wl)
	}

	return gen
}

func initGenerator(wl *Workload, phase exePhase, krs *[]int64) {

	if phase == LoadPhase {
		*krs = keyRangesToKeys(wl.RemoteDBInsertKeyRange)
	} else {
		*krs = keyRangesToKeys(wl.RemoteDBOperationRange)
	}

}

// return 0: sent
// return 1: need return (exit early)
func retrySend(allToExe chan<- exeCmd, exeToGen <-chan bool, genToCli chan<- genCmd, cmd genCmd) int {
	select {
	case <-exeToGen:
		_, _ = fmt.Fprintln(os.Stdout, "genThread return early - 1")
		close(genToCli)
		allToExe <- exeCmd{NormalExit, "GenThread"}
		return 1
	case genToCli <- cmd:
		return 0
	}
}
