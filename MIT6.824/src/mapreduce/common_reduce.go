package mapreduce

import (
	"encoding/json"
	"io/ioutil"
	"sort"
	"os"
	"log"
	"fmt"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string,       // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string,       // write the output here
	nMap int,             // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//

	var fileContent []KeyValue

	for index := 0; index < nMap; index++ {
		fileName := reduceName(jobName, index, reduceTaskNumber)

		data, err := ioutil.ReadFile(fileName)
		if err != nil {
			panic(err)
		}

		var tempContent []KeyValue

		json.Unmarshal([]byte(data), &tempContent)
		fileContent = append(fileContent, tempContent...)
	}

	sort.Sort(KeyValueSlice(fileContent))

	currentKey := fileContent[0].Key
	var valueSet []string

	file, err := os.Create(outFile)
	fmt.Println("OutFile: ", outFile)
	if err != nil {
		log.Fatal("OS Create: ", err)
	}
	enc := json.NewEncoder(file)

	for _, kv := range fileContent {
		if kv.Key != currentKey {
			enc.Encode(KeyValue{currentKey, reduceF(currentKey, valueSet)})

			currentKey = kv.Key
			valueSet = []string{kv.Value}
		} else {
			valueSet = append(valueSet, kv.Value)
		}
	}
	enc.Encode(KeyValue{currentKey, reduceF(currentKey, valueSet)})
	file.Close()
}

type KeyValueSlice []KeyValue

func (s KeyValueSlice) Len() int           { return len(s) }
func (s KeyValueSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s KeyValueSlice) Less(i, j int) bool { return s[i].Key < s[j].Key }
