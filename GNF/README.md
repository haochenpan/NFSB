### GNF Architecture

### GNF Implementation
#### Interfaces
#### Goroutines
#### Concurrent Logic

### For Developers & Maintainers:
### Important Exported (Public) Functions

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
### Future Work
