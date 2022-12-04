package utils

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
)

// Packet message type
const (
	PING       string = "ping"              // When a Client Ping a Server
	ACK        string = "acknowledgement"   // Response from Server to the Ping of the Client
	BYE        string = "leave"             // When a server leaves the cluster
	INTRODUCE  string = "introduce"         // Ping from new node to the introducer
	JOIN       string = "join"              // Message to notifies others for a new join
	JOINACK    string = "acknowledge join"  // Message from the server back to the new node
	FAILURE    string = "fail"              // Message send from a node that detected a server
	WHOISINTRO string = "who is introducer" // When a process joins, it asks who the introducer is.
	IAM        string = "I am introducer"
	NO         string = "no, I am not the introducer"
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
	INFERENCE   = 8
	REMOVE      = 9
	STATUS      = 10
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

var AllPotentialProcesses = []string{
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

type MLMessage struct {
	Action         int
	JobID          int
	BatchSize      int
	ModelType      string
	ModelName      string
	MembershipList []string
	JobInfo        *JobStatus
}

/* This represents a metadata for a file; */
type FileMetaData struct {
	Version  int
	Replicas []string
}

type NodeMetaData struct {
	Node string
}

// process_allocation     map[string]int      // Maps process to which i-th N/10 (assume num_workers = 10)
type JobStatus struct {
	JobId                int                 // Id of the job
	BatchSize            int                 // Batch size
	NumWorkers           int                 // Number of workers doing this job
	QueryRate            float32             // Query rate
	ModelType            string              // Current job's model type
	ModelName            string              // Current job's model name
	ProcessBatchProgress map[string]int      // Maps process to its current batch number in the job (which batch in each N/10)
	ProcessTestFiles     map[string][]string // Maps process to its assigned test files (of length each_process_total_task)
	TaskQueues           []string
	tasklock             sync.Mutex
}

func (j *JobStatus) AssignWorks(process string) ([]string, int, int) {
	j.tasklock.Lock()
	batch_size := j.BatchSize
	current_batch_files := []string{}

	queue := j.TaskQueues
	log.Printf("On process %v: Job %v, number of job in the queue: %v", process, j.JobId, len(queue))
	if len(queue) == 0 {
		/* No more tasks to do for this job. Done! */
		j.tasklock.Unlock()
		return nil, 0, 0
	} else if len(queue) > batch_size {
		current_batch_files = queue[:batch_size]
		queue = queue[batch_size:]
		j.TaskQueues = queue
	} else {
		current_batch_files = j.TaskQueues[:]
		j.TaskQueues = nil
	}
	// Update the current process' in-progress work.
	j.ProcessTestFiles[process] = current_batch_files
	// Update the global job status.
	j.tasklock.Unlock()

	current_batch := j.ProcessBatchProgress[process]
	return current_batch_files, current_batch, 1
}

// func (j *JobStatus) Lock() {
// j.tasklock.Lock()
// }

// func (j *JobStatus) Unlock() {
// j.tasklock.Unlock()
// }

// func (j *JobStatus) AssignLock(lock *sync.Mutex) {
// j.tasklock = lock
// }

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

func PrintJobInfo(jobs_info map[int]*JobStatus) {
	for _, job := range jobs_info {
		PrintJob(job)
	}
}
func PrintJob(job *JobStatus) {
	fmt.Print("Printing job info\n")
	fmt.Println(strings.Repeat("=", 80))
	fmt.Println("=\tJob Id: ", job.JobId)
	fmt.Println("=\tCurrent Batch size: ", job.BatchSize)
	fmt.Println("=\tQuery rate: ", job.QueryRate)
	fmt.Println("=\tModel type: ", job.ModelType)
	fmt.Println("=\tModel name: ", job.ModelName)
	fmt.Println("=\tRemaining files: ", len(job.TaskQueues))
	fmt.Println("=\tVMs assigned to this job")
	for process := range job.ProcessTestFiles {
		fmt.Printf("=\t\t%v\n", process)
	}
	fmt.Println(strings.Repeat("=", 80))
}
