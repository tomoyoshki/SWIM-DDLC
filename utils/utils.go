package utils

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
)

const (
	PUT         = 0
	GET         = 1
	DELETE      = 2
	LS          = 3
	STORE       = 4
	NUM_VERSION = 5
	ALLOCATE    = 6
	TRAIN       = 7
)

func SetupPythonServer() {
	cmd := exec.Command("python3", "python/server.py")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	go cmd.Run()
}
func RemoveFromList(list []string, target string) []string {
	for i, other := range list {
		if other == target {
			return append(list[:i], list[i+1:]...)
		}
	}
	return list
}

var Addresses = []string{
	"fa22-cs425-7501.cs.illinois.edu",
	"fa22-cs425-7502.cs.illinois.edu",
	"fa22-cs425-7503.cs.illinois.edu",
	"fa22-cs425-7504.cs.illinois.edu",
	"fa22-cs425-7505.cs.illinois.edu",
	"fa22-cs425-7506.cs.illinois.edu",
	"fa22-cs425-7507.cs.illinois.edu",
	"fa22-cs425-7508.cs.illinois.edu",
	"fa22-cs425-7509.cs.illinois.edu",
	"fa22-cs425-7510.cs.illinois.edu",
}

type ChannelInMessage struct {
	Action    int
	Localfile string
	Sdfsfile  string
	Version   int
}

type ChannelOutMessage struct {
	Action   int
	Replicas []string
	Version  int
}

/* This represents a metadata for a file; */
type FileMetaData struct {
	Version  int
	Replicas []string
}

type NodeMetaData struct {
	Node string
}

func CreateFileDirectory(filepath string) {
	target_filename_array := strings.Split(filepath, "/")
	target_file_directory := strings.Join(target_filename_array[0:len(target_filename_array)-1], "/")
	if _, err := os.Stat(target_file_directory); os.IsNotExist(err) {
		os.MkdirAll(target_file_directory, 0700) // Create your file
	}
}

func IndexOf(target string, data []string) int {
	for k, v := range data {
		if target == v {
			return k
		}
	}
	return -1 //not found.
}

func RemoveDuplicateValues(data []string) []string {
	keys := make(map[string]bool)
	list := []string{}

	// If the key(values of the slice) is not equal
	// to the already present value in new slice (list)
	// then we append it. else we jump on another element.
	for _, entry := range data {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}

func Mod(a, b int) int {
	return (a%b + b) % b
}

func FormatPrint(print string) {
	fmt.Print("\n" + strings.Repeat("=", 80) + "\n")
	fmt.Println("=\t", print)
	fmt.Println(strings.Repeat("=", 80))
}

func LogError(err error, message string, exit bool) {
	if err != nil {
		log.Println(message, ": ", err)
		if exit {
			os.Exit(1)
		}
	}
}
