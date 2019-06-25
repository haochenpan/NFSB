### GNF Architecture

### GNF Implementation
#### Interfaces
#### Goroutines
#### Concurrent Logic

### For Developers:
#### Controller Signals and Workloads (what GNF responses to)

In datastruct.go
```go
package main
type UserData struct {
	Action       string   // load, run, stop, or interrupt
	NewWorkLoad  bool     // NewWorkLoad is true if WorkLoadFile is a string of wl parameters and values; 
	WorkLoadFile string   // NewWorkLoad is false if WorkLoadFile is a gnf instance local file path
	GnfIPS       []string
}

```
#### Important Exported (Public) Functions (what GNF sends)

In utilities.go:
```go
package gnf

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

func (bm *BmStats) String() string {
	// performs some string formatting
	// returns a YCSB-like string representation of bm
}

func EncodeBmStat(data *BmStats) []byte {
    // returns an empty byte array upon error
    // returns the encoded byte array if succeed
}

func DecodeBmStat(dataBytes []byte) BmStats {
    // returns an empty BmStats upon error
    // returns the decoded byte array if succeed
}

```

#### Add a Workload Parameter

- choose a proper section (e.g. remoteDB), a proper parameter name and the default value in `workload_template`
    - leave the right hand side of the equal sign empty is the default value is the zero value of that type
- In `workload.go`, add the parameter (need to capitalize the first letter) in the corresponding place
    - method `UpdateWorkloadByLine` automatically check whether int > 0 or float between 0 - 1 (inclusive). 
    - If there are special rules, add a method that starts with `isValid` in `utilities.go` (e.g. `isValidRemoteDB`, `isValidDistribution`, and `isValidKeyRange`). Then insert this method in the right `case` statement in `UpdateWorkloadByLine`
- Use it wherever you have a *Workload
- Add some unit tests if you introduce new methods

#### Add a Operation Generator


#### Support a new DB

### Future Work

Throughput optimization
``