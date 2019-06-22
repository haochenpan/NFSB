package gnf

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
)



type Workload struct {
	RemoteDB         string
	RemoteDBHost     string
	RemoteDBPort     int
	RemoteDBPassword string

	RemoteDBLoadThreadCount int
	RemoteDBRunThreadCount  int

	RemoteDBInsertKeyRange        KeyRanges
	RemoteDBInsertValueSizeInByte int

	RemoteDBOperationCount        int
	RemoteDBOperationRange        KeyRanges
	RemoteDBOperationDistribution string
	RemoteDBReadRatio             float64
	RemoteDBWriteRatio            float64
}

func InitWorkload() *Workload {
	return &Workload{
		RemoteDB:         "redis",
		RemoteDBHost:     "localhost",
		RemoteDBPort:     6379,
		RemoteDBPassword: "",

		RemoteDBLoadThreadCount: 1,
		RemoteDBRunThreadCount:  1,

		RemoteDBInsertKeyRange:        KeyRanges{{0, 1000}},
		RemoteDBInsertValueSizeInByte: 64,

		RemoteDBOperationCount:        1000,
		RemoteDBOperationRange:        KeyRanges{{0, 1000}},
		RemoteDBOperationDistribution: "uniform",
		RemoteDBReadRatio:             0.9,
		RemoteDBWriteRatio:            0.1,
	}
}

/*
	Update a workload parameter by feeding a line that looks like param=value
	if found no update: return 0, nil;
	if found one update: return 1, nil;
	if found an ill update: return -1, some_error_message OR panic.
*/
func (wl *Workload) UpdateWorkloadByLine(line string) (int, error) {
	if idx := strings.Index(line, "#"); idx != -1 {
		line = line[:idx]
	}

	var pair []string
	if pair = strings.Split(line, "="); len(pair) == 1 {
		return 0, nil

	}
	// remoteDB* in wl file -> RemoteDB* in wl struct
	param := strings.Title(strings.TrimSpace(pair[0]))
	value := strings.TrimSpace(pair[1])
	if (len(param) == 0) || (len(value) == 0) {
		return 0, nil
	}

	s := reflect.ValueOf(wl).Elem() // could be optimized
	field := s.FieldByName(param)
	if !field.IsValid() || !field.CanSet() {
		return -1, errors.New(param + " is not a valid workload parameter")
	}

	switch field.Kind() {
	case reflect.Int:
		val, err := strconv.Atoi(value)
		if err != nil {
			return -1, errors.New("In updateing workload parameter " + param + ", " + value + " is not an int")
		} else if val <= 0 {
			return -1, errors.New("In updateing workload parameter " + param + ", " + value + " is <= 0")
		} else if field.Int() != reflect.ValueOf(val).Int() {
			//fmt.Println("setting a new int field")
			field.Set(reflect.ValueOf(val))
			return 1, nil
		}

	case reflect.Float64:
		val, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return -1, errors.New("In updateing workload parameter " + param + ", " + value + " is not a float")
		} else if val < 0 {
			return -1, errors.New("In updateing workload parameter " + param + ", " + value + " is < 0")
		} else if field.Float() != reflect.ValueOf(val).Float() {
			//fmt.Println("setting a new float field")
			field.Set(reflect.ValueOf(val))
			return 1, nil
		}

	case reflect.String:
		if (param == "RemoteDB") && (!isValidRemoteDB(value)) {
			return -1, errors.New("not a valid DB option")
		} else if param == "RemoteDBOperationDistribution" && (!isValidDistribution(value)) {
			return -1, errors.New("not a valid distribution option")
		} else if field.String() != reflect.ValueOf(value).String() {
			//fmt.Println("setting a new string field")
			field.Set(reflect.ValueOf(value))
			return 1, nil
		}

	case reflect.Slice:
		val := strings.Split(value, ",")
		ranges := make(KeyRanges, len(val))
		for i, v := range val {
			onePair := strings.Split(v, "-")
			if len(onePair) < 2 {
				return -1, errors.New("In updateing workload parameter " + param + ", " + value + " is ill formatted")
			}
			oneStart, err := strconv.Atoi(strings.TrimSpace(onePair[0]))
			if err != nil {
				return -1, errors.New("In updateing workload parameter " + param + ", " + onePair[0] + " is not an int")
			}
			oneEnd, err := strconv.Atoi(strings.TrimSpace(onePair[1]))
			if err != nil {
				return -1, errors.New("In updateing workload parameter " + param + ", " + onePair[1] + " is not an int")
			}
			oneRange := KeyRange{oneStart, oneEnd}
			ranges[i] = oneRange
		}

		if !isValidKeyRange(ranges) {
			return -1, errors.New("not a valid keyrange")
		}
		fieldSlice := field.Slice(0, field.Len())
		value := reflect.ValueOf(ranges).Slice(0, len(ranges))

		if !fieldSlice.Interface().(KeyRanges).equal(value.Interface().(KeyRanges)) {
			//fmt.Println("two slices are not equal!")
			field.Set(reflect.ValueOf(ranges))
			return 1, nil
		}
	}

	return 0, nil
}

func (wl *Workload) UpdateWorkloadByFile(path string) {
	//fPath := "workloads/workload_template"

	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	updateCount := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		inc, err := wl.UpdateWorkloadByLine(scanner.Text())
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
		}
		if inc == 1 {
			updateCount += inc
		}
	}
	fmt.Println(strconv.Itoa(updateCount) + " workload parameter(s) updated")
	wl.Inspect()
}
func (wl *Workload) Inspect() {
	s := reflect.ValueOf(wl).Elem()
	typeOfT := s.Type()

	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		fmt.Printf("%2d: %-30s %-20s %v\n", i,
			typeOfT.Field(i).Name, f.Type(), f.Interface())
	}
}
