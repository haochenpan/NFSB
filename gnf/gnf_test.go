package gnf

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"
)

func TestWorkload_UpdateWorkloadByLine(t *testing.T) {
	var tests = []struct {
		input string
		want  int
	}{
		{"RemoteDB = redis", 0},
		{"#remoteDB = ", 0},
		{"remoteDB = #", 0},
		{"remoteDB = memcached", -1},
		{"remoteBB = redis", -1},


		{" remoteDBPort = redis", -1},
		{" remoteDBPort = -6379", -1},
		{" remoteDBPort = 0", -1},
		{" remoteDBPort = 6380", 1},

		{" remoteDBReadRatio = redis", -1},
		{" remoteDBReadRatio = -0.1", -1},
		{" remoteDBReadRatio = 1.1", -1},
		{" remoteDBReadRatio = 0.1", 1},

		{" remoteDBOperationDistribution = 0.1", -1},
		{" remoteDBOperationDistribution = zipf", -1},
		{" remoteDBOperationDistribution = uniform", 0},

		{"remoteDBHost = 127.0.0.1", 1},

		{" remoteDBInsertKeyRange = 0", -1},
		{" remoteDBInsertKeyRange = 1-12-2000", -1},
		{" remoteDBInsertKeyRange = 1.1-2", -1},
		{" remoteDBInsertKeyRange = 1-1.2", -1},

		{" remoteDBInsertKeyRange = 0-100", 1},
		{" remoteDBInsertKeyRange = 1-1000,1000-2000", -1},
	}
	for _, test := range tests {
		wl := InitWorkload()
		ret, _ := wl.UpdateWorkloadByLine(test.input)
		if ret != test.want {
			t.Errorf("input=%q, ret=%v, want=%v", test.input, ret, test.want)
		}
	}
}

func TestWorkload_UpdateWorkloadByFile(t *testing.T) {
	path1, path3 := "../workloads/workload_for_testing", "workload_not_exist"
	wl := InitWorkload()
	if ret := wl.UpdateWorkloadByFile(path1); ret != 5 {
		t.Errorf("ret=%v, want=5", ret)
	}

	if ret := wl.UpdateWorkloadByFile(path3); ret != -1 {
		t.Errorf("ret=%v, want=-1", ret)
	}
}

func Test_keyRangesToKeys(t *testing.T) {
	krs1 := KeyRanges{{100, 200}, {1000, 2000}}
	if krs1.keyCount() != 1100 {
		t.Error("not correct num of keys")
	}
	keys := keyRangesToKeys(krs1)
	if len(keys) != 1100 {
		t.Error("not correct num of keys")
	}

	krs2 := KeyRanges{}
	var krs3 KeyRanges
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

	krs4 := KeyRanges{{1000, 500}}
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
	bm1 := BmStats{}
	bm2 := BmStats{1,2,3,4,5,6,7,8,9,10,11,12,13,14}
	fmt.Println(bm1.String())
	fmt.Println(bm2.String())
}

func TestLatencies(t *testing.T) {
	t1, _ := time.ParseDuration("25us")
	t2, _ := time.ParseDuration("30us")
	t3, _ := time.ParseDuration("35us")
	t4, _ := time.ParseDuration("40us")
	lat1 := Latency{t4, t3, t2, t1}
	lat2 := Latency{t1, t2, t3, t4}
	var lat3 Latency
	lat4 := make(Latency, 0)
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
