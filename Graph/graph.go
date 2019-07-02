package main

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"

	"gonum.org/v1/gonum/graph/simple"
)

var g *simple.WeightedDirectedGraph

type Edge struct {
	srcID, targetID int
}

func createGraph() {
	g = simple.NewWeightedDirectedGraph(0, math.Inf(1))

	file, err := os.Open("graphConfig.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		text := scanner.Text()
		text_trim := strings.Fields(text)
		src_id, err1 := strconv.Atoi(text_trim[0])
		dest_id, err2 := strconv.Atoi(text_trim[1])
		weight, err3 := strconv.ParseFloat(text_trim[2], 64)

		if err1 != nil || err2 != nil || err3 != nil {
			fmt.Println("Invalid format of userInput Edge")
		}

		g.SetWeightedEdge(simple.WeightedEdge{F: simple.Node(src_id), T: simple.Node(dest_id), W: weight})
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func printEdges(g *simple.WeightedDirectedGraph) {
	w, _ := g.Weight(1, 2)
	fmt.Println(w)
	// edges := g.Edges()

	// for edges.Next() != false {
	// 	edge := edges.Edge()
	// 	from := edge.From().ID
	// 	to := edge.To().ID
	// }
}

func main() {
	createGraph()
	printEdges(g)
}
