package gnf

/*
	Types and utilities of GNF
*/

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"math"
	"math/rand"
	"net"
	"os"
	"time"
)

type (
	exeSig string
	genSig string
	exePhase string
)

type exeCmd struct {
	sig exeSig
	arg string
}

type genCmd struct {
	Sig  genSig
	Arg1 int
	Arg2 string
}

type stats struct {
	threadIndex int
	succeed     bool
	genCmd      genCmd
	start       time.Time
	end         time.Time
}

type BmStats struct {
	IP        string
	Timestamp string // e.g. get from time.Now().String()

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

type keyRange struct {
	StartIndex int
	EndIndex   int // exclusive
}

type keyRanges []keyRange

type latency []time.Duration

const Letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

const (
	MaxIntInFloat  = float64(math.MaxInt64) // assume the platform supports
	MaxUintInFloat = float64(math.MaxUint64)
)

const (
	LoadSig exePhase = "load"
	RunSig  exePhase = "run"

	ReadSig  genSig = "read"
	WriteSig genSig = "write"

	NExit    exeSig = "exit"      // followed by a return
	EExit    exeSig = "exception" // followed by a return
	GnfStop  exeSig = "stop"      // never followed by a return
	BmStop   exeSig = "bmStop"    // never followed by a return
	CtrlLoad exeSig = "load"
	CtrlRun  exeSig = "run"
	Ready    exeSig = "ready" // two comm threads
)

// generates a YCSB-like benchmark report
func (bm *BmStats) String() string {

	var str string

	str += fmt.Sprintf("[OVERALL], IP, %s\n", bm.IP)
	str += fmt.Sprintf("[OVERALL], Timestamp, %v\n", bm.Timestamp)

	str += fmt.Sprintf("[OVERALL], RunTime(sec), %.3f\n", bm.Runtime)
	str += fmt.Sprintf("[OVERALL], Throughput(ops/sec), %.3f\n", bm.Throughput)

	if bm.SRead > 0 {
		str += fmt.Sprintf("[READ], Operations, %d\n", bm.SRead)
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

func EncodeBmStat(data *BmStats) []byte {
	var res bytes.Buffer

	enc := gob.NewEncoder(&res)
	if err := enc.Encode(&data); err != nil {
		return []byte{}
	}
	return res.Bytes()
}

func DecodeBmStat(dataBytes []byte) BmStats {
	var buff bytes.Buffer
	var stats BmStats

	buff.Write(dataBytes)
	dec := gob.NewDecoder(&buff)
	if err := dec.Decode(&stats); err != nil {
		return BmStats{}
	}
	return stats
}

func (lat latency) Len() int {
	return len(lat)
}

func (lat latency) Less(i, j int) bool {
	return lat[i].Nanoseconds() < lat[j].Nanoseconds()
}

func (lat latency) Swap(i, j int) {
	lat[i], lat[j] = lat[j], lat[i]
}

func (lat latency) getAvgLat() float64 {
	if lat == nil || len(lat) == 0 {
		return 0
	}

	//idx := int(float64(len(lat)) * 0.5)
	//val := float64(lat[idx]) / float64(time.Microsecond)

	sum := int64(0)
	for _, l := range lat {
		sum += time.Duration(l).Nanoseconds()
	}
	avg := (float64(sum) / float64(time.Microsecond)) / float64(len(lat))
	return avg
}

func (lat latency) get95pLat() float64 {
	if lat == nil || len(lat) == 0 {
		return 0
	}

	idx := int(float64(len(lat)) * 0.95)
	val := float64(lat[idx]) / float64(time.Microsecond)
	return val
}

func (kr1 keyRange) equal(kr2 keyRange) bool {
	return (kr1.StartIndex == kr2.StartIndex) &&
		(kr1.EndIndex == kr2.EndIndex)
}

func (krs1 keyRanges) equal(krs2 keyRanges) bool {
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

func (krs1 keyRanges) keyCount() int {
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
	case "uniform", "zipfian":
		return true
	}
	return false
}

func isValidKeyRange(keyRanges keyRanges) bool {
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

func keyRangesToKeys(keyRanges keyRanges) []int64 {

	var randKeyIdx, krsKeyIdx int
	var src = rand.NewSource(714) // only 714 here
	var ran = rand.New(src)
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
		//b[i] = Letters[r.Int63()%int64(len(Letters))]
		idx := r.Int63() % int64(len(Letters))
		//fmt.Println(idx)
		b[i] = Letters[idx]
	}
	return string(b)
}

func getIp() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	return "", nil
}

func writeWorkloadFile(wl string, fname string) int {
	f, err := os.Create(fname)
	if err != nil {
		fmt.Println("crease wl file error")
		return -1
	}

	w := bufio.NewWriter(f)
	_, err = w.WriteString(wl)
	if err != nil {
		fmt.Println("write wl file error")
		return -1
	}
	if err = w.Flush(); err != nil {
		fmt.Println("flush wl file error")
		return -1
	}
	f.Close()
	return 0
}
