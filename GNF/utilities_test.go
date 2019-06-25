package gnf

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"
)

func Test_keyRangesToKeys(t *testing.T) {
	krs1 := keyRanges{{100, 200}, {1000, 2000}}
	if krs1.keyCount() != 1100 {
		t.Error("not correct num of keys")
	}
	keys := keyRangesToKeys(krs1)
	if len(keys) != 1100 {
		t.Error("not correct num of keys")
	}

	krs2 := keyRanges{}
	var krs3 keyRanges
	if krs1.equal(krs2) {
		t.Error("not correct comparision")
	}
	if ! krs1.equal(krs1) {
		t.Error("not correct comparision")
	}
	if krs2.equal(krs3) {
		t.Error("not correct comparision")
	}

	if isValidKeyRange(krs2) {
		t.Error("not a valid key range")
	}

	krs4 := keyRanges{{1000, 500}}
	if isValidKeyRange(krs4) {
		t.Error("not a valid key range")
	}
}

func Test_randString(t *testing.T) {
	var ran = rand.New(src)
	if len(randString(ran, 16)) != 16 {
		t.Error("not a correct length")
	}
}

func TestBmStats_String(t *testing.T) {
	myIp, _ := getIp()
	bm1 := BmStats{}
	bm2 := BmStats{myIp, time.Now().String(),2,3,4,5,6,7,8,9,10,11,12,13,14, 15}
	fmt.Println(bm1.String())
	fmt.Println(bm2.String())
}

func TestLatencies(t *testing.T) {
	t1, _ := time.ParseDuration("25us")
	t2, _ := time.ParseDuration("30us")
	t3, _ := time.ParseDuration("35us")
	t4, _ := time.ParseDuration("40us")
	lat1 := latency{t4, t3, t2, t1}
	lat2 := latency{t1, t2, t3, t4}
	var lat3 latency
	lat4 := make(latency, 0)
	sort.Sort(lat1)
	for i := range lat1 {
		if lat1[i] != lat2[i] {
			t.Error("not sorted")
		}
	}

	if lat1.getAvgLat() != float64(lat2[2])/float64(time.Microsecond) {
		t.Error("not correct avg lat")
	}
	if lat1.get95pLat() != float64(lat2[3])/float64(time.Microsecond) {
		t.Error("not correct 95p lat")
	}

	if lat3.getAvgLat() != 0 || lat4.getAvgLat() != 0 {
		t.Error("not correct avg lat")
	}

	if lat3.get95pLat() != 0 || lat4.get95pLat() != 0 {
		t.Error("not correct avg lat")
	}

}

func Test_getIP(t *testing.T) {
	fmt.Println(getIp())
}