package Utility

import (
	"NFSB/DataStruct"
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

var (
	prefix          = "../Config/"
	workload_prefix = "../"
)

func ReadControllerIp() string {
	file, err := os.Open(prefix + "ControllerIP.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		return scanner.Text()
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	return ""
}

func ReadRedisIP() []string {
	var redisIP []string
	file, err := os.Open(prefix + "RedisConfig.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		redisIP = append(redisIP, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	return redisIP
}

func ReadGNFIP() []string {
	var gnfIP []string
	file, err := os.Open(prefix + "GNFIP.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		gnfIP = append(gnfIP, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	return gnfIP
}

func IsFileExist(filename string) bool {
	if _, err := os.Stat(workload_prefix + filename); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

func LoadWorkLoadFile(filename string) string {
	fileBytes, err := ioutil.ReadFile(workload_prefix + filename)
	if err != nil {
		log.Fatal(err)
	}
	return string(fileBytes)
}

func LoadPortConfig(m map[string]string) {
	file, err := os.Open(prefix + "Config.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		text := scanner.Text()
		text_trim := strings.Fields(text)
		key := text_trim[0]
		port := text_trim[2]
		m[key] = port
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func PrintUserData(data DataStruct.UserData) {
	fmt.Printf("%+v\n", data)
}

func PrintStatsData(stats DataStruct.Stats) {
	fmt.Printf("%+v\n", stats)
}

func LoadGnfAddress() []string {
	gnfsIP := ReadGNFIP()
	return gnfsIP
}

func PrintGNFNotKnown() {
	fmt.Println("Does not know the GNF you passed in")
}

func CreateFile(fileName string) {
	path := workload_prefix + fileName
	_, err := os.Stat(path)

	// if file does not exist we create it
	if os.IsNotExist(err) {
		file, err := os.Create(path)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()
	}
}

func AppendStatsToFile(path, stats string) {
	f, err := os.OpenFile(workload_prefix+path, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	defer f.Close()
	if err != nil {
		log.Fatal(err)
	}

	_, err = f.WriteString(stats)
}
