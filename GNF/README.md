## Generic Network Function (GNF) 

### Introduction
There are two types of GNFs: GNF (with controller) and GNF-Cli (without controller). 
While the normal GNFs wait for controller commands to stat/stop benchmarking at almost the same time, 
a GNF-Cli instance can be used for debugging or testing purposes.

### Architecture & Design
Please see our paper for HotNets 2019

### Implementation

#### Interfaces

##### Database Client

<details>

<summary> DBClient in database.go. When adding a new type of storage node (i.e. other than Redis), need to implement this interface with two methods. </summary>

```go
type DBClient interface {
	DBWrite(key, val string) error
	DBRead(key string) (string, error)
}
```

Also, add an entry in function `getRemoteDBClients()` in `database.go`, and an entry in `isValidRemoteDB()` in `utilities.go`
to make a string that represents this type of DB can be picked up from a workload file.

</details>


##### Operation Generator

<details>

<summary> OpGenerator in generator.go. When implementing a new kind of request distribution, need to implement this interface. </summary>

```go
type OpGenerator interface {
	GenThread(allToExe chan<- exeCmd, exeToGen <-chan bool, genToCli chan<- genCmd, wl *Workload, phase exePhase)
}
```

Also, add an entry in function `getOpGenerator()` in `generator.go`, and en entry in `isValidDistribution()` in `utilities.go`

</details>




#### Threads / Goroutines

Besides `GenThread()` above in Operation Generator section, there are a few more goroutines:

<details>

<summary> exeRecvThread() in control.go: receives controller signals (`UserData` struct in outer package) and interpret into executor commands; </summary>

```go
type UserData struct {
	Action       string   // load, run, stop, or interrupt
	NewWorkLoad  bool     // NewWorkLoad is true if WorkLoadFile is a string of wl parameters and values; 
	WorkLoadFile string   // NewWorkLoad is false if WorkLoadFile is a gnf instance local file path
	GnfIPS       []string
}
```

</details>


<details>

<summary> exeSendThread() in control.go: sends benchmark statistics from executor to the controller; </summary>

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

</details>


`exeSignThread()` in `control.go`: receives OS signals and interpret into executor commands;

`cliThread()` in `gnf.go`: performs DB operations according to `GenThread()` commands;

`staThread()` in `gnf.go`: converges DB Client Threads' per-request statistics to one benchmark statistics.

#### Concurrent Logic

`GnfMain()` in `gnf.go`: interpret command line arguments and spawns GNF or GNF-Cli

`mainRoutine()` in `gnf.go`: GNF executor's main routine, incl. responses to executor commands (e.g. start benchmarking)

`benchmarkRoutine()` in `gnf.go`: GNF executor's routine while benchmarking. 
Compared to the main routine, responses to commands and exception handling are different.


### For Developers:

#### Add a Workload Parameter

- choose a proper section (e.g. remoteDB), a proper parameter name and the default value in `workload_template`
    - leave the right hand side of the equal sign empty is the default value is the zero value of that type
- In `workload.go`, add the parameter (need to capitalize the first letter) in the corresponding place
    - method `UpdateWorkloadByLine` automatically check whether int > 0 or float between 0 - 1 (inclusive). 
    - If there are special rules, add a method that starts with `isValid` in `utilities.go` (e.g. `isValidRemoteDB`, `isValidDistribution`, and `isValidKeyRange`). Then insert this method in the right `case` statement in `UpdateWorkloadByLine`
- Use it wherever you have a *Workload
- Add some unit tests if you introduce new methods


### Future Work

Implements more operation generator according to different distributions; 

Add more DB backend support;

Add more unit tests.
