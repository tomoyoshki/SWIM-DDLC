package utils

import (
	"fmt"
	"log"
	"math"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"
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
	PUT                = 0
	GET                = 1
	DELETE             = 2
	LS                 = 3
	STORE              = 4
	NUM_VERSION        = 5
	ALLOCATE           = 6
	TRAIN              = 7
	INFERENCE          = 8
	REMOVE             = 9
	STATUS             = 10
	FAILED             = 11
	INFERENCE_RESULT_0 = 12
	INFERENCE_RESULT_1 = 13
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
	QueryCount           int                 // Number of tasks done for this job.
	QueryRate            float32             // Query rate
	ModelType            string              // Current job's model type
	ModelName            string              // Current job's model name
	StartTime            time.Time           // Start time of running INFERENCE (NOT INITIALIZATION)
	QueryTime            []float64           // Query time for each batch.
	ProcessBatchProgress map[string]int      // Maps process to its current batch number in the job (which batch in each N/10)
	ProcessTestFiles     map[string][]string // Maps process to its assigned test files (of length each_process_total_task)
	TaskQueues           []string
	Workers              []string // List of existing worker processes
	tasklock             sync.Mutex
	countlock            sync.Mutex
	querytimelock        sync.Mutex
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

func (j *JobStatus) UpdateCount(size int) {
	j.countlock.Lock()
	j.QueryCount += size
	j.countlock.Unlock()
}

func (j *JobStatus) GetQueryStatistics() (float64, float64, float64, float64, float64) {
	j.querytimelock.Lock()
	if len(j.QueryTime) == 0 {
		j.querytimelock.Unlock()
		return 0, 0, 0, 0, 0
	}
	data := make([]float64, len(j.QueryTime))
	copy(data, j.QueryTime)
	j.querytimelock.Unlock()

	total_time := float64(0)
	for _, time := range data {
		total_time += time
	}
	N := len(data)
	mean := total_time / float64(N)
	var sd float64
	for j := 0; j < N; j++ {
		sd += math.Pow(data[j]-mean, 2)
	}
	sd = math.Sqrt(sd / float64(N))

	/* Find percentiles */
	sort.Float64s(data)
	twenty_fifth := data[N/4]
	median := data[N/2]
	seventy_fifth := data[3*N/4]
	return mean, sd, twenty_fifth, median, seventy_fifth
}

func (j *JobStatus) AddQueryTime(time float64) {
	j.querytimelock.Lock()
	j.QueryTime = append(j.QueryTime, time)
	j.querytimelock.Unlock()
}

// Completes the work done.
func (j *JobStatus) RestoreTasks(process string, tasks []string) {
	j.tasklock.Lock()

	queue := j.TaskQueues
	queue = append(tasks, queue...)
	j.TaskQueues = queue
	log.Printf("Restored process %v tasks for job %v, the length of queue is %v", process, j.JobId, len(queue))
	// Set the current process' to empty
	j.ProcessTestFiles[process] = []string{}

	// Update the global job status.
	j.tasklock.Unlock()
}

func (j *JobStatus) CalculateQueryRate() float64 {
	j.countlock.Lock()
	count := float64(j.QueryCount) / time.Now().Sub(j.StartTime).Seconds()
	j.countlock.Unlock()
	return count
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

func PrintJobInfo(jobs_info map[int]*JobStatus) {
	for _, job := range jobs_info {
		PrintJob(job)
	}
}
func PrintJob(job *JobStatus) {
	fmt.Print("Printing job info\n")
	fmt.Println(strings.Repeat("=", 80))
	fmt.Println("=\tJob Id: ", job.JobId)
	fmt.Println("=\tJob Start Time ", job.StartTime.String())
	fmt.Println("=\tCurrent Batch size: ", job.BatchSize)
	query_rate := float64(job.QueryCount) / time.Now().Sub(job.StartTime).Seconds()
	fmt.Printf("=\tQuery rate: %v/s\n", query_rate)
	fmt.Println("=\tQuery count: ", job.QueryCount)
	fmt.Println("=\tModel type: ", job.ModelType)
	avg, sd, first, median, second := job.GetQueryStatistics()
	fmt.Println("=\tAverage Inferencing Time for A Batch: ", avg)
	fmt.Println("=\tStandard Deviation for Inferencing Time ", sd)
	fmt.Printf("=\t\t25-th Percentile:%v \n", first)
	fmt.Printf("=\t\t50-th Percentile:%v \n", median)
	fmt.Printf("=\t\t75-th Percentile:%v \n", second)
	fmt.Println("=\tModel name: ", job.ModelName)
	fmt.Println("=\tRemaining files: ", len(job.TaskQueues))
	fmt.Println("=\tWorkers for this job")
	for i, worker := range job.Workers {
		fmt.Printf("=\t\t%v: %v\n", i, worker)
	}
	// fmt.Println("=\tVMs assigned to this job")
	// for process, file := range job.ProcessTestFiles {
	// 	fmt.Printf("=\t\t%v: %v\n", process, len(file))
	// }
	fmt.Println(strings.Repeat("=", 80))
}
