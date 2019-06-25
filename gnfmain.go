package main

import (
	gnf "NFSB/GNF"
	"fmt"
)

func main() {
	if err := gnf.Main(); err != nil {
		fmt.Println(err)
	}
}
