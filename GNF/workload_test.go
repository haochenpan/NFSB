package gnf

import "testing"

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
	path1, path3 := "../Config/workload_for_testing", "workload_not_exist"
	wl := InitWorkload()
	if ret := wl.UpdateWorkloadByFile(path1); ret != 5 {
		t.Errorf("ret=%v, want=5", ret)
	}

	if ret := wl.UpdateWorkloadByFile(path3); ret != -1 {
		t.Errorf("ret=%v, want=-1", ret)
	}
}