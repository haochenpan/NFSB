package gnf

/*
	Types and utilities that are specific to GNF
	TODO (Roger): Move some structs to the main package
	TODO (Roger): Fix upper lower cases
*/

import (
	"fmt"
	"math/rand"
	"time"
)

type (
	ExeSig string
	GenSig string
	ExePhase string
)

type ExeCmd struct {
	Sig ExeSig
	Arg string
}

type GenCmd struct {
	Sig  GenSig
	Arg1 int
	Arg2 string
}

type Stats struct {
	threadIndex int
	succeed     bool
	genCmd      GenCmd
	start       time.Time
	end         time.Time
}

type BmStats struct {
	Runtime    float64
	Throughput float64

	SRead       int
	SReadAvgLat float64
	SRead95pLat float64

	SWrite       int
	SWriteAvgLat float64
	SWrite95pLat float64

	FRead       int
	FReadAvgLat float64
	FRead95pLat float64

	FWrite       int
	FWriteAvgLat float64
	FWrite95pLat float64
}

type KeyRange struct {
	StartIndex int
	EndIndex   int // exclusive
}

type Latency []time.Duration

type KeyRanges []KeyRange

var src = rand.NewSource(714)

var ran = rand.New(src) // for generator only

const Letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

const (
	MaxUint       = ^uint(0)
	MaxInt        = int(MaxUint >> 1) // use it like the max of int64, assume the platform supports
	MaxIntInFloat = float64(MaxInt)
)

const (
	LoadSig ExePhase = "load"
	RunSig  ExePhase = "run"

	ReadSig  GenSig = "read"
	WriteSig GenSig = "write"

	ExitSig      ExeSig = "exit"
	InterruptSig ExeSig = "stop"
	ExceptionSig ExeSig = "exception"
)

// generate a YCSB-like benchmark report
func (bm *BmStats) String() string {
	var str string
	str += fmt.Sprintf("[OVERALL], RunTime(sec), %f\n", bm.Runtime)
	str += fmt.Sprintf("[OVERALL], Throughput(ops/sec), %f\n", bm.Throughput)

	if bm.SRead > 0 {
		str += fmt.Sprintf("[READ], Operations, %.3d\n", bm.SRead)
		str += fmt.Sprintf("[READ], AverageLatency(us), %.3f\n", bm.SReadAvgLat)
		str += fmt.Sprintf("[READ], 95thPercentileLatency(us), %.3f\n", bm.SRead95pLat)
	}

	if bm.SWrite > 0 {
		str += fmt.Sprintf("[WRITE], Operations, %d\n", bm.SWrite)
		str += fmt.Sprintf("[WRITE], AverageLatency(us), %.3f\n", bm.SWriteAvgLat)
		str += fmt.Sprintf("[WRITE], 95thPercentileLatency(us), %.3f\n", bm.SWrite95pLat)
	}

	if bm.FRead > 0 {
		str += fmt.Sprintf("[READ-FAILED], Operations, %d\n", bm.FRead)
		str += fmt.Sprintf("[READ-FAILED], AverageLatency(us), %.3f\n", bm.FReadAvgLat)
		str += fmt.Sprintf("[READ-FAILED], 95thPercentileLatency(us), %.3f\n", bm.FRead95pLat)
	}

	if bm.FWrite > 0 {
		str += fmt.Sprintf("[WRITE-FAILED], Operations, %d\n", bm.FWrite)
		str += fmt.Sprintf("[WRITE-FAILED], AverageLatency(us), %.3f\n", bm.FWriteAvgLat)
		str += fmt.Sprintf("[WRITE-FAILED], 95thPercentileLatency(us), %.3f\n", bm.FWrite95pLat)
	}
	return str
}

func (lat Latency) Len() int {
	return len(lat)
}

func (lat Latency) Less(i, j int) bool {
	return lat[i].Nanoseconds() < lat[j].Nanoseconds()
}

func (lat Latency) Swap(i, j int) {
	lat[i], lat[j] = lat[j], lat[i]
}

func (lat Latency) getAvgLat() float64 {
	if lat == nil || len(lat) == 0 {
		return 0
	}

	idx := int(float64(len(lat)) * 0.5)
	val := float64(lat[idx]) / float64(time.Microsecond)
	return val
}

func (lat Latency) get95pLat() float64 {
	if lat == nil || len(lat) == 0 {
		return 0
	}

	idx := int(float64(len(lat)) * 0.95)
	val := float64(lat[idx]) / float64(time.Microsecond)
	return val
}

func (kr1 KeyRange) equal(kr2 KeyRange) bool {
	return (kr1.StartIndex == kr2.StartIndex) &&
		(kr1.EndIndex == kr2.EndIndex)
}

func (krs1 KeyRanges) equal(krs2 KeyRanges) bool {
	if (krs1 == nil) != (krs2 == nil) {
		return false
	}

	if len(krs1) != len(krs2) {
		return false
	}

	for i := range krs1 {
		if !krs1[i].equal(krs2[i]) {
			return false
		}
	}
	return true
}

func (krs1 KeyRanges) keyCount() int {
	sum := 0
	for _, kr := range krs1 {
		sum += kr.EndIndex - kr.StartIndex
	}
	return sum
}

func isValidRemoteDB(db string) bool {
	switch db {
	case "redis":
		return true
	}
	return false
}

func isValidDistribution(dist string) bool {
	switch dist {
	case "uniform":
		return true
	}
	return false
}

func isValidKeyRange(keyRanges KeyRanges) bool {
	if len(keyRanges) < 1 {
		return false
	}
	maxSoFar := -1
	for _, r := range keyRanges {
		if r.StartIndex <= maxSoFar {
			return false
		}
		maxSoFar = r.StartIndex
		if r.EndIndex <= maxSoFar {
			return false
		}
		maxSoFar = r.EndIndex
	}
	return true
}

func keyRangesToKeys(keyRanges KeyRanges) []int64 {

	var randKeyIdx, krsKeyIdx int
	keyCnt := keyRanges.keyCount()
	krs := make([]int64, keyCnt)
	for _, keyRange := range keyRanges {

		for i := keyRange.StartIndex; i < keyRange.EndIndex; {

			if randKeyIdx < keyRange.StartIndex {
				_ = ran.Int63()
				randKeyIdx++

			} else if randKeyIdx < keyRange.EndIndex {
				krs[krsKeyIdx] = ran.Int63()
				krsKeyIdx++
				//fmt.Println(randKeyIdx, key)
				randKeyIdx++
				i++
			}
		}
	}
	//fmt.Println(krs)

	return krs
}

// r should be a local var in order to be thread safe
func randString(r *rand.Rand, n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = Letters[r.Int63()%int64(len(Letters))]
	}
	return string(b)
}
