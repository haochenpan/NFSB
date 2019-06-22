package DataStruct

import (
	"bytes"
	"encoding/gob"
	"log"
)

type UserData struct {
	Action       string
	NewWorkLoad  bool
	WorkLoadFile string
	GnfIPS       []string
}

type Stats struct {
	Latency    string
	Throughput string
}

func Encode(data *UserData) []byte {
	var res bytes.Buffer

	enc := gob.NewEncoder(&res)
	if err := enc.Encode(&data); err != nil {
		log.Fatal(err)
	}
	return res.Bytes()
}

func Decode(dataBytes []byte) UserData {
	var buff bytes.Buffer
	var userData UserData

	buff.Write(dataBytes)
	dec := gob.NewDecoder(&buff)
	if err := dec.Decode(&userData); err != nil {
		log.Fatal(err)
	}
	return userData
}

func EncodeStat(data *Stats) []byte {
	var res bytes.Buffer

	enc := gob.NewEncoder(&res)
	if err := enc.Encode(&data); err != nil {
		log.Fatal(err)
	}
	return res.Bytes()
}

func DecodeStat(dataBytes []byte) Stats {
	var buff bytes.Buffer
	var stats Stats

	buff.Write(dataBytes)
	dec := gob.NewDecoder(&buff)
	if err := dec.Decode(&stats); err != nil {
		log.Fatal(err)
	}
	return stats
}
