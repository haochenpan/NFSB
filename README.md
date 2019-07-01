## NFSB - Network Function Storage Benchmarking

## Getting Started

#### Dependencies (ZMQ and Redis binding)

```bash
go get github.com/pebbe/zmq4
go get github.com/go-redis/redis
```
#### Start the UserInput
```bash
# start  UserInput, parameters see the output
cd UserInput
go run *.go
```

#### Start the controller
```bash
# start Controller with UserInput, parameters see the output
cd Controller
go run *.go outputFileName.txt

# start Controller with Benchmark mode, parameters see the output
cd Controller
go run *.go benchmark number_of_rounds outputFileName.txt
```


#### Start Generic Network Function (GNF)

```bash
# start GNF with controller, parameters see the output
go run gnfmain.go gnf
# start GNF without controller, parameters see the output
go run gnfmain.go gnf-cli
```

The code of GNF is in folder GNF/, detailed explanation see [the readme page of GNF](GNF/README.md) 



## Any Questions? Encounter a problem?
 
Please use Issues section of this Github repo.
