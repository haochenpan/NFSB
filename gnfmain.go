package main

import (
	gnf "NFSB/GNF"
	"fmt"
)

func main() {
	if err := gnf.GnfMain(); err != nil {
		fmt.Println(err)
	}
}
