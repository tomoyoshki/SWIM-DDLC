package main

import (
	"bufio"
	"bytes"
	"cs425mp4/client_modules/client"
	"cs425mp4/client_modules/client_model"
	"cs425mp4/server"
	"cs425mp4/utils"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var PORT_NUMBER = 1111
var INTRODUCER_PORT_NUMBER = 2222
var MASTER_PORT_NUMBER = 3333

var INTRODUCER_IP = ""

var MASTER_ADDRESS = fmt.Sprintf("%s:%d", INTRODUCER_IP, MASTER_PORT_NUMBER)

// @param: timeout
var PING_TIMEOUT = time.Second * 30000
var PING_TIME_INTERVAL = time.Millisecond * 3000

var this_ip = GetIP()

var introduce_server *net.UDPConn
var node_server *net.UDPConn

var this_host, err = os.Hostname()
var this_id = this_host + "_" + strconv.Itoa(int(time.Now().UnixMilli()))
var LOCALNAME string
var ticker *time.Ticker = time.NewTicker(PING_TIME_INTERVAL)

var log_file *os.File

var done = make(chan bool, 1)

var user_leave = true

var membership_list []string    // Membership list
var membership_mutex sync.Mutex // Locks for critical sections
var server_command = make(chan string)

/* Messages for node server's incomming message/request */
var MasterIncommingChannel = make(chan utils.ChannelInMessage)

/* Messages for master's output message */
var MasterOutgoingChannel = make(chan utils.ChannelOutMessage)

// Message for Scheduler and GRPC
var SchedulerMLChannel = make(chan utils.MLMessage)

/* Messages for failed process' information */
var MasterFailChannel = make(chan string)

/* Messages for new process joining */
var MasterNewChannel = make(chan []string)

/* Messages for ending master server */
var filesystem_finish_channel = make(chan bool)

var new_introducer_channel = make(chan string)

/* Maps a file to its replicas */
var file_metadata = make(map[string]utils.FileMetaData)

/* Maps processes name to a map (which maps to file names) */
var node_metadata = make(map[string][]string)

var server_files = []string{}

type Packet struct {
	Packet_Senderid  string   // ID of the sender
	Packet_Type      string   // PING ACK BYE JOIN FAILURE
	Packet_PiggyBack []string // PiggyBack information, if packet type == JOIN, the member id and member to join
}

type Action struct {
	ActionType string // Membership list actions type
	TargetID   string // The target on which the action acts
}

// Membership list actions type
const (
	INSERT  string = "insert"
	DELETE  string = "delete"
	READ_ML string = "read membership list"
)

const (
	FS_PUT    = 0
	FS_GET    = 1
	FS_DELETE = 2
	FS_LS     = 3
	FS_STORE  = 4
	// NUM_VERSION = 3
)

/* ------------------------ Scheduler Data Structures ------------------------  */

/* Channel for receiving new jobs (command as list of strings) */
var JobsQueue = make(chan string)
var batch_size_1 = 32
var batch_size_2 = 32
var ScheduleWaitGroup sync.WaitGroup
var round_robin_running = false
var jobs []int
var current_job = -1 // Should iterate between 0 and 1, indicating current job
var job_status = make(map[int]JobStatus)
var dir_test_files_map = make(map[string][]string) // Maps a directory to its files

// // Maps a process to its corresponding channel (for tracking each progress)
// var ProcessScheduleMap = make(map[string](chan utils.ChannelOutMessage))

// // Maps a process to a channel that will receive done message.
// var ProgressChannel = make(map[string](chan string))

type TrainTask struct {
	model     string
	test_data []string
}

type JobStatus struct {
	job_id                  int                 // Id of the job
	batch_size              int                 // Batch size
	num_workers             int                 // Number of workers doing this job
	each_process_total_task int                 // Total test files in this job / num_workers
	query_rate              float32             // Query rate
	model_type              string              // Current job's model type
	model_name              string              // Current job's model name
	process_allocation      map[string]int      // Maps process to which i-th N/10 (assume num_workers = 10)
	process_batch_progress  map[string]int      // Maps process to its current batch number in the job (which batch in each N/10)
	process_test_files      map[string][]string // Maps process to its assigned test files (of length each_process_total_task)
}

var test_dir = []string{"test_data/images"}

// This function will load all test data to the SDFS at the beginning.
func load_test_set() {
	utils.FormatPrint("Loading test dataset")
	for _, directory := range test_dir {
		all_files := []string{}
		// Loop through each test file under current dir:
		files, _ := ioutil.ReadDir(directory)
		for _, file := range files {
			localfilename := directory + "/" + file.Name()
			all_files = append(all_files, localfilename)
			// log.Print("\n\nClient started requesting put")
			sdfsfilename := localfilename
			addresses, new_sdfsfilename, err := client.ClientRequest(MASTER_ADDRESS, localfilename, sdfsfilename, utils.PUT)
			if err != nil {
				log.Printf("Error Requesting for files: %v", err)
				break
			}
			for _, fileserver_addr := range addresses {
				target_addr_port := fmt.Sprintf("%s:%v", fileserver_addr, MASTER_PORT_NUMBER)
				log.Printf("Uploading file %v to node server %v", new_sdfsfilename, target_addr_port)
				go client.ClientUpload(target_addr_port, localfilename, new_sdfsfilename)
			}
		}
		dir_test_files_map[directory] = all_files
	}
	utils.FormatPrint("Finished loading test dataset")
}

func main() {
	// setup log files
	SetupFiles()
	defer log_file.Close()
	// Constantly reading user inputs for instructions
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Fprintf(os.Stdout, "> ")

	utils.SetupPythonServer()

	for scanner.Scan() {
		input := scanner.Text()
		input_list := strings.Split(input, " ")
		if len(input) == 0 {
			fmt.Fprintf(os.Stdout, "> ")
			continue
		}
		if input == "list_mem" {
			fmt.Println("\n\nCurrent Membership List: ")
			fmt.Println(strings.Repeat("=", 80))
			for _, member := range membership_list {
				member_ip := strings.Split(member, "_")[0]
				if member_ip == INTRODUCER_IP {
					fmt.Println("=\t", member, " ***")
				} else {
					fmt.Println("=\t", member)
				}
			}
			fmt.Println(strings.Repeat("=", 80) + "\n\n")
		} else if input == "list_self" {
			fmt.Println("Current id: ", this_id)
			utils.FormatPrint("current_id: " + this_id)
		} else if input == "join" {
			if user_leave == false {
				continue
			}

			user_leave = false
			ticker.Reset(PING_TIME_INTERVAL)
			INTRODUCER_IP = WhoIsIntroducer()
			MASTER_ADDRESS = fmt.Sprintf("%s:%d", INTRODUCER_IP, MASTER_PORT_NUMBER)
			/* Starting detecting and responding */
			go NodeClient()
			go NodeServer()
			if this_host == INTRODUCER_IP {
				/* Setup the introducer */
				go IntroduceServer()
				go server.Server(MASTER_PORT_NUMBER, MasterIncommingChannel, MasterOutgoingChannel, filesystem_finish_channel, new_introducer_channel, SchedulerMLChannel, &server_files)
				go MasterServer()
			} else {
				/* Setup client and request to the introducer */
				go AskToIntroduce()
				go NewIntroducer()
				go server.Server(MASTER_PORT_NUMBER, MasterIncommingChannel, MasterOutgoingChannel, filesystem_finish_channel, new_introducer_channel, SchedulerMLChannel, &server_files)
			}
		} else if input == "leave" {
			user_leave = true
			if this_host == INTRODUCER_IP {
				introduce_server.Close()
			}
			node_server.Close()
			done <- true
			ticker.Stop()
		} else if input_list[0] == "test" {
			handleTest(input, input_list)
		} else if input_list[0] == "start_job" {

		} else {
			command := strings.ToLower(input_list[0])
			handleSDFSCommand(command, input_list)
		}
		fmt.Fprintf(os.Stdout, "\n> ")
	}
}

func handleTest(input string, input_list []string) {
	if len(input_list) == 1 {
		log.Println("Invalid test command [python, train, inference, start]")
	}
	command := input_list[1]
	switch command {
	case "python":
		utils.SetupPythonServer()
	case "train":
		size := 10
		res, err := client_model.AskToInitializeModel("localhost:9999", 1, size, "image")
		if err != nil {
			log.Print("Test train error: ", err)
			return
		}
		log.Printf("Test Train Response: %v", res)
	case "inference":
		inference_res, err := client_model.AskToInference("localhost:9999", 1, 0, 1, "python/data/")
		if err != nil {
			log.Println("AskToInference fails")
			return
		}
		ires := make(map[string][]string)
		err = json.Unmarshal(inference_res, &ires)
		for k, v := range ires {
			log.Printf("%v: %v", k, v)
		}
	case "start":
		replicas := make(map[string][]string)
		replicas["1-image1.jpg"] = []string{"fa22-cs425-7501.cs.illinois.edu:3333"}
		client_model.SendInferenceInformation("fa22-cs425-7503.cs.illinois.edu:3333", 1, 0, replicas)
	default:
		log.Println("Invalid test command [python, train, inference, start]")
	}
}

func handleSDFSCommand(command string, input_list []string) {
	switch command {
	case "put":
		if len(input_list) != 3 {
			utils.FormatPrint(fmt.Sprintf("Invalid format of PUT: %v", input_list))
			break
		}
		log.Print("\n\nClient started requesting put")
		localfilename := input_list[1]
		sdfsfilename := input_list[2]
		addresses, new_sdfsfilename, err := client.ClientRequest(MASTER_ADDRESS, localfilename, sdfsfilename, utils.PUT)
		if err != nil {
			log.Printf("Error Requesting for files: %v", err)
			break
		}
		for _, fileserver_addr := range addresses {
			target_addr_port := fmt.Sprintf("%s:%v", fileserver_addr, MASTER_PORT_NUMBER)
			log.Printf("Uploading file %v to node server %v", new_sdfsfilename, target_addr_port)
			go client.ClientUpload(target_addr_port, localfilename, new_sdfsfilename)
		}
		log.Print("\n\nMain put ended requesting")
	case "get":
		if len(input_list) != 3 {
			utils.FormatPrint(fmt.Sprintf("Invalid format of GET: %v", input_list))
			break
		}
		log.Print("\n\nClient started requesting get")
		localfilename := input_list[2]
		sdfsfilename := input_list[1]
		addresses, new_sdfsfilename, err := client.ClientRequest(MASTER_ADDRESS, localfilename, sdfsfilename, utils.GET)
		if err != nil {
			log.Printf("Error Requesting for files: %v", err)
			if len(addresses) == 0 {
			}
			break
		}
		for i, fileserver_addr := range addresses {
			target_addr_port := fmt.Sprintf("%s:%v", fileserver_addr, MASTER_PORT_NUMBER)
			log.Printf("Retrieving file %v from  node server %v", new_sdfsfilename, target_addr_port)
			err := client.ClientDownload(target_addr_port, localfilename, new_sdfsfilename)
			if err == nil {
				break
			}
			if i == len(addresses)-1 {
				utils.FormatPrint("Received no files")
			}
		}
		log.Print("\n\n Main ended requesting get")
	case "delete":
		if len(input_list) != 2 {
			utils.FormatPrint(fmt.Sprintf("Invalid format of DELETE: %v", input_list))
			break
		}
		log.Print("\n\n Client started requesting delete")
		sdfsfilename := input_list[1]
		// Replica addresses containing the sdfsfilename
		addresses, _, err := client.ClientRequest(MASTER_ADDRESS, "", sdfsfilename, utils.DELETE)
		if err != nil {
			log.Printf("Error Requesting for files: %v", err)
			break
		}
		// Request delete at each replica address
		for _, fileserver_addr := range addresses {
			target_addr_port := fmt.Sprintf("%s:%v", fileserver_addr, MASTER_PORT_NUMBER)
			log.Printf("Deleting file to node server %v", target_addr_port)
			client.ClientDelete(target_addr_port, sdfsfilename)
		}
		log.Print("\n\n Main ended requesting delete")

	case "ls":
		if len(input_list) != 2 {
			utils.FormatPrint(fmt.Sprintf("Invalid format of ls: %v", input_list))
			break
		}
		sdfsfilename := input_list[1]
		log.Printf("ls %v on Simple Distributed File System", sdfsfilename)
		addresses, _, _ := client.ClientRequest(MASTER_ADDRESS, "", sdfsfilename, utils.LS)

		fmt.Println(strings.Repeat("=", 80))
		for _, address := range addresses {
			fmt.Println("=\t", address)
		}
		fmt.Println(strings.Repeat("=", 80) + "\n\n")
	case "store":
		utils.FormatPrint(fmt.Sprintf("The current processes store the following SDFS files: %v", server_files))
	case "metadata":
		fmt.Println("\n", strings.Repeat("=", 80))
		fmt.Println("=\tThe current processes store the following node metadata files: ")
		for process, files := range node_metadata {
			fmt.Printf("=\tProcesses %v, has following files: %v\n", process, files)
		}
		fmt.Println("=\tThe current processes store the file metadata files: ")
		for file, meta := range file_metadata {
			fmt.Printf("=\tFilename: %v, highest version: %v, on replicas: %v,\n", file, meta.Version, meta.Replicas)
		}
		fmt.Println("\n", strings.Repeat("=", 80))
	case "introducer":
		utils.FormatPrint(fmt.Sprintf("The current introducer is: %v \n", INTRODUCER_IP))
	default:
		// get-version sdfsfilename num-version localfilename
		if command == "get-versions" {
			handleGetVersions(input_list)
		} else {
			handleMLCommand(command, input_list)
			utils.FormatPrint(fmt.Sprintf("Invalid command %v\n", input_list))
		}
	}
}

func handleMLCommand(input string, input_list []string) {
	switch input {
	case "load_test_dataset":
		// Put local dataset tup
		go load_test_set()
		break
	case "start_job":
		// start_job job_id batch_size model_type
		if len(input_list) != 4 {
			log.Println("Invalid start_job command [start_job model_type job_id (0 or 1) batch_size]")
			return
		}
		model_type := input_list[1]

		if model_type != "image" && model_type != "speech" {
			log.Println("Invalid model_type (image or speech)")
			return
		}

		job_id := input_list[2]
		job_id_int, err := strconv.Atoi(job_id)
		if err != nil || (job_id != "0" && job_id != "1") {
			log.Println("Invalid job_id (0, 1)")
			return
		}

		batch_size := input_list[3]
		batch_size_int, err := strconv.Atoi(batch_size)
		if err != nil {
			log.Println("Invalid batch_size")
			return
		}

		_, err = client_model.ClientStartJob(MASTER_ADDRESS, job_id_int, batch_size_int, model_type)
		if err != nil {
			log.Println("Received error starting job from server")
		}
	case "inference":
		// inference job_id
	}

}

func handleGetVersions(input_list []string) {
	if len(input_list) != 4 {
		utils.FormatPrint(fmt.Sprintf("Invalid format of get-versions: %v", input_list))
		return
	}
	sdfsfilename := input_list[1]
	num_version, err := strconv.Atoi(input_list[2])
	if err != nil {
		utils.FormatPrint(fmt.Sprintf("Invalid format of get-versions: %v", input_list))
		return
	}
	localfilename := input_list[3]
	// Get the replicas that have address
	replica_addresses, highest_version_str, err := client.ClientRequest(MASTER_ADDRESS, localfilename, sdfsfilename, utils.NUM_VERSION)
	if err != nil {
		log.Printf("Could not get-version from Master")
		return
	}
	// Get the highest version of the file
	highest_version, err := strconv.Atoi(highest_version_str)
	for i := 0; i < num_version; i += 1 {
		last_version := highest_version - i
		// If there are less than the number of versions requested
		if last_version < 1 {
			break
		}
		// Get the last version file on the server
		versioned_sdfsfilename := strconv.Itoa(last_version) + "-" + sdfsfilename
		versioned_localfilename := strconv.Itoa(last_version) + "-" + localfilename
		for i, fileserver_addr := range replica_addresses {
			target_addr_port := fmt.Sprintf("%s:%v", fileserver_addr, MASTER_PORT_NUMBER)
			log.Printf("Trying to download %v as %v from %v", versioned_sdfsfilename, versioned_localfilename, target_addr_port)
			log.Printf("Retrieving file %v with version %v from node server %v", versioned_sdfsfilename, last_version, target_addr_port)
			err := client.ClientDownload(target_addr_port, versioned_localfilename, versioned_sdfsfilename)
			if err == nil {
				break
			}
			if i == len(replica_addresses)-1 {
				log.Printf("Received no files")
			}
		}
	}
}

// Establish a introducer or reestablish one
func IntroduceServer() {
	udp_addr, err := net.ResolveUDPAddr("udp", INTRODUCER_IP+":"+strconv.Itoa(INTRODUCER_PORT_NUMBER))
	utils.LogError(err, "Unable to resolve Introducer UDP Addr: ", true)
	introduce_server, err = net.ListenUDP("udp", udp_addr)
	utils.LogError(err, "Unable to Listen as a UDP server IntroduceServer(): ", true)

	MembershipListManager(Action{INSERT, this_id})

	defer introduce_server.Close()
	// Handling Initial Joins
	IntroduceIntroducer()

	for !user_leave {
		// listen for new incoming Join
		buffer := make([]byte, 1024)
		_, node_addr, err := introduce_server.ReadFromUDP(buffer)
		if err != nil {
			log.Println("Unable to read from UDP: ", err)
			continue
		}
		buffer = bytes.Trim(buffer, "\x00")

		// Parse the response and add to the membership list
		response, err := ConvertFromResponse(buffer)
		if err != nil {
			log.Println("Unable to convert response to struct Packet")
			continue
		}

		if response.Packet_Type != utils.INTRODUCE {
			continue
		}

		// inserting into the membership list and give the new node
		membership_mutex.Lock()

		// Send the JOIN to the neighbors in old membershiplist
		packet_to_neighbor := Packet{this_id, utils.JOIN, []string{response.Packet_Senderid}}
		packet_to_neighbor_s, _ := ConvertToSend(packet_to_neighbor)
		DisseminatePacket(packet_to_neighbor_s)

		// Update the membershiplist and send to the node
		MembershipListManager(Action{INSERT, response.Packet_Senderid})
		packet_to_node := Packet{this_id, utils.JOINACK, membership_list}
		packet_to_node_s, _ := ConvertToSend(packet_to_node)
		_, err = introduce_server.WriteToUDP(packet_to_node_s, node_addr)
		if err != nil {
			log.Println("Unable to send JOINACK and membership list to the new node")
			continue
		}
		membership_mutex.Unlock()
		utils.FormatPrint(response.Packet_Senderid + " joined")
	}
}

func IntroduceIntroducer() {
	for _, addr := range utils.AllPotentialProcesses {
		// Get UDP address of the processes
		potential_process_udp_addr, err := net.ResolveUDPAddr("udp", addr+":"+strconv.Itoa(PORT_NUMBER))
		utils.LogError(err, "Unable to resolve UDP Address: ", false)
		// Dial each process
		introduce_request, err := net.DialUDP("udp", nil, potential_process_udp_addr)
		if err != nil {
			log.Println("Unable to dial UDP: ", err)
			continue
		}
		// Create packet to send INTRODUCE request
		packet_to_send := Packet{this_id, utils.INTRODUCE, membership_list}
		// Convert to UDP sendable format
		packet, err := ConvertToSend(packet_to_send)
		utils.LogError(err, "Unable to convert the struct to udp message: ", false)
		// Ask each end point to send membership list back
		_, err = introduce_request.Write(packet)
		utils.LogError(err, "Unable to send the packetfalse", false)
		// Set time out
		introduce_request.SetDeadline(time.Now().Add(time.Millisecond))
		response_buffer := make([]byte, 1024)
		_, _, err = introduce_request.ReadFromUDP(response_buffer)
		if err != nil {
			log.Println("Unable to receive response from the server: ", err)
			// timeout, go to next server
			continue
		}
		response_buffer = bytes.Trim(response_buffer, "\x00")
		utils.LogError(err, "Unable to read from UDP on rejoin: ", false)
		response_packet, err := ConvertFromResponse(response_buffer)
		utils.LogError(err, "Unable to covert from udp response to struct Packet: ", false)

		// Copy the membership list to my ow
		membership_mutex.Lock()
		if len(response_packet.Packet_PiggyBack) > len(membership_list) {
			membership_list = response_packet.Packet_PiggyBack
		}
		membership_mutex.Unlock()
	}
}

func AskToIntroduce() {
	introducer_address, err := net.ResolveUDPAddr("udp", INTRODUCER_IP+":"+strconv.Itoa(INTRODUCER_PORT_NUMBER))
	utils.LogError(err, "Unable to resolve introducer address in AskToIntroduce().", true)

	connection, err := net.DialUDP("udp", nil, introducer_address)
	utils.LogError(err, "Unable to dial introducer address in AskToIntroduce()", true)
	defer connection.Close()

	/* Create JOIN request packat */
	request_packet := Packet{this_id, utils.INTRODUCE, []string{}}
	request, err := ConvertToSend(request_packet)
	utils.LogError(err, "Unable to convert join packet to bytes in AskToIntroduce()", true)

	_, err = connection.Write(request)
	utils.LogError(err, "Unable to Write to UDP connection to introducer in AskToIntroduce()", true)

	response_buffer := make([]byte, 1024)
	_, err = connection.Read(response_buffer)
	utils.LogError(err, "Unable to Read to UDP response from introducer in AskToIntroduce()", true)
	response_buffer = bytes.Trim(response_buffer, "\x00")

	response, err := ConvertFromResponse(response_buffer)
	utils.LogError(err, "Unable to Convert UDP Byte response to packet in AskToIntroduce()", true)

	/* If introducer acknowledges our join request */
	if response.Packet_Type == utils.JOINACK {
		membership_list = response.Packet_PiggyBack
	} else {
		utils.LogError(err, "Unable to get JOINACK from introducer in AskToIntroduce()", true)
	}
}

func WhoIsIntroducer() string {
	for _, addr := range utils.AllPotentialProcesses {
		if addr == this_host {
			continue
		}
		// Get UDP address of the processes node server.
		potential_process_udp_addr, err := net.ResolveUDPAddr("udp", addr+":"+strconv.Itoa(PORT_NUMBER))
		utils.LogError(err, "Unable to resolve UDP Address in WhoIsIntroducer(): ", false)
		// Dial each process
		introduce_request, err := net.DialUDP("udp", nil, potential_process_udp_addr)
		if err != nil {
			log.Println("Unable to dial UDP in WhoIsIntroducer(): ", err)
			continue
		}
		// Create packet to send INTRODUCE request
		packet_to_send := Packet{this_id, utils.WHOISINTRO, []string{""}}
		// Convert to UDP sendable format
		packet, err := ConvertToSend(packet_to_send)
		utils.LogError(err, "Unable to convert the struct to udp message: ", false)
		// Ask each end point to send membership list back
		_, err = introduce_request.Write(packet)
		utils.LogError(err, "Unable to send the packetfalse", false)
		// Set time out
		introduce_request.SetDeadline(time.Now().Add(time.Second))
		response_buffer := make([]byte, 1024)
		_, _, err = introduce_request.ReadFromUDP(response_buffer)
		if err != nil {
			log.Println("Unable to receive response from the server: ", err)
			// timeout, go to next server
			continue
		}
		response_buffer = bytes.Trim(response_buffer, "\x00")
		utils.LogError(err, "Unable to read from UDP on rejoin: ", false)
		response_packet, err := ConvertFromResponse(response_buffer)
		utils.LogError(err, "Unable to covert from udp response to struct Packet: ", false)
		if response_packet.Packet_Type == utils.IAM {
			return strings.Split(response_packet.Packet_Senderid, "_")[0]
		}
	}
	return this_host
}

// The input is the metadata collected from all the non-faulty processe
// (excluding the previous Master, as it already failed)
func InitializeMetadata(ProcessFiles map[string][]string) {
	file_highest_version := make(map[string]int)
	file_replicas := make(map[string][]string)
	for process, files := range ProcessFiles {
		process = strings.Split(process, ":")[0]
		truncated_files := []string{}
		for _, file := range files {
			/* Parse the filename into version and actual file name*/
			version, _ := strconv.Atoi(strings.Split(file, "-")[0])
			filename := strings.Split(file, "-")[1]
			/* Collect all plain file names for this processes */
			truncated_files = append(truncated_files, filename)
			/* Add this process to the file's replica */
			file_replicas[filename] = append(file_replicas[filename], process)
			/* Collect the higest version number of this file */
			if value, ok := file_highest_version[filename]; ok {
				/* If exists and current version is higher*/
				if version > value {
					file_highest_version[filename] = version
				}
			} else {
				file_highest_version[filename] = version
			}
		}
		/* For each process, initialize the node metadata. */
		node_metadata[process] = utils.RemoveDuplicateValues(truncated_files)
	}
	/* Loop through each file, initialize the file metadata structure. */
	for filename, replicas := range file_replicas {
		highest_version := file_highest_version[filename]
		file_metadata[filename] = utils.FileMetaData{Version: highest_version, Replicas: utils.RemoveDuplicateValues(replicas)}
	}

	for filename, file_meta := range file_metadata {
		if len(file_meta.Replicas) < 4 {
			/* This file has one missing replica (from the old Master) */
			log.Printf("Reassigning the old master's file %v", filename)
			replicas := file_meta.Replicas
			memlist_copy, _ := GetMembershipList()
			memlist_copy = GetHostsFromID(memlist_copy)
			if len(memlist_copy) < 4 {
				// If total node less than 4, no more new replica.
				break
			}
			/* Find all non-replcia of this file's processes */
			for _, replica := range replicas {
				memlist_copy = RemoveFromList(memlist_copy, replica)
			}
			log.Print("Memlist before hashing: ", memlist_copy)
			candidate := HashedReplicas(memlist_copy)[0]
			// Update filedata's replicas:
			new_file_metadata := file_meta
			new_file_metadata.Replicas = append(replicas, candidate)
			highest_vers := new_file_metadata.Version
			file_metadata[filename] = new_file_metadata

			node_metadata[candidate] = append(node_metadata[candidate], filename)

			// Send this to filesystem manager
			log.Printf("Now requesting replica on %v to send file %v to candidate %v", replicas, filename, candidate)
			go client.ClientRequestReplicas(filename, candidate, replicas, highest_vers)

		}
	}

}

// Establish typical server handling PING and ACK
func NodeServer() {
	udp_addr, err := net.ResolveUDPAddr("udp", this_ip+":"+strconv.Itoa(PORT_NUMBER))
	utils.LogError(err, "Unable to resolve Introducer UDP Addr: ", true)
	node_server, err = net.ListenUDP("udp", udp_addr)
	utils.LogError(err, "Unable to Listen as a UDP server in NodeServer()", true)
	defer node_server.Close()
	for !user_leave {
		buffer := make([]byte, 1024)
		_, node_addr, err := node_server.ReadFromUDP(buffer)
		if err != nil {
			log.Println("Unable to read from UDP NodeServer(): ", err)
			continue
		}
		buffer = bytes.Trim(buffer, "\x00")
		// Parse the response and add to the membership list
		response, err := ConvertFromResponse(buffer)
		if err != nil {
			log.Println("Unable to convert response to struct Packet")
			continue
		}

		if response.Packet_Type == utils.WHOISINTRO {
			var ack_packet Packet
			if INTRODUCER_IP == this_host {
				ack_packet = Packet{this_id, utils.IAM, []string{}}
			} else {
				ack_packet = Packet{this_id, utils.NO, []string{}}
			}
			ack_message, err := ConvertToSend(ack_packet)
			utils.LogError(err, "Unable to convert ack message to send NodeServer()", false)
			_, _ = node_server.WriteToUDP(ack_message, node_addr) // send Yes back
			continue
		}

		// If receives a PING message
		if response.Packet_Type == utils.PING {
			ack_packet := Packet{this_id, utils.ACK, []string{}}
			ack_message, err := ConvertToSend(ack_packet)
			utils.LogError(err, "Unable to convert ack message to send NodeServer()", false)
			_, _ = node_server.WriteToUDP(ack_message, node_addr) // send ack back to the pinger
		} else {
			// Receives FAILURE, BYE, JOIN, INTRODUCE
			membership_mutex.Lock()
			if response.Packet_Type == utils.FAILURE || response.Packet_Type == utils.BYE {
				// FAILURE message, deleting node from membership
				if response.Packet_PiggyBack[0] == this_id {
					membership_list = []string{}
					membership_mutex.Unlock()
					continue
				}
				_, deleted := MembershipListManager(Action{DELETE, response.Packet_PiggyBack[0]})
				/* Inform the master (if it is) that a failed process found. */
				if deleted {
					failed_ip := strings.Split(response.Packet_PiggyBack[0], "_")[0]
					log.Println("Failed ip is ", failed_ip)
					if failed_ip == INTRODUCER_IP {
						/* Detects the introducer/master failed. */
						this_process_index := utils.IndexOf(this_id, membership_list)
						// Get the master index
						master_index := utils.IndexOf(failed_ip, membership_list)
						greatest_index := len(membership_list) - 1
						if master_index != greatest_index && this_process_index == greatest_index {
							INTRODUCER_IP = this_host
							MASTER_ADDRESS = fmt.Sprintf("%s:%d", INTRODUCER_IP, MASTER_PORT_NUMBER)
							memlist_copy, _ := GetMembershipList()
							memlist_copy = GetHostsFromID(memlist_copy)
							ProcessFiles := client.ClientMasterElect(RemoveFromList(memlist_copy, failed_ip))
							InitializeMetadata(ProcessFiles)
							go MasterServer()
							go IntroduceIntroducer()

						} else if master_index == greatest_index && this_process_index == greatest_index-1 {
							// If I have the greatest index,
							INTRODUCER_IP = this_host
							MASTER_ADDRESS = fmt.Sprintf("%s:%d", INTRODUCER_IP, MASTER_PORT_NUMBER)
							memlist_copy, _ := GetMembershipList()
							memlist_copy = GetHostsFromID(memlist_copy)
							ProcessFiles := client.ClientMasterElect(RemoveFromList(memlist_copy, failed_ip))
							InitializeMetadata(ProcessFiles)
							go MasterServer()
							go IntroduceIntroducer()
						}
					}
					log.Printf("Successfully deleted %v", failed_ip)
					if this_host == INTRODUCER_IP {
						MasterFailChannel <- response.Packet_PiggyBack[0]
					}
					/* If deleted successfully, then notify others. */
					disseminate_packet := Packet{this_id, response.Packet_Type, []string{response.Packet_PiggyBack[0]}}
					disseminate_message, err := ConvertToSend(disseminate_packet)
					if err != nil {
						log.Println("Unable to send disseminate message: ", err)
					} else {
						DisseminatePacket(disseminate_message)
					}
					fmt.Println("\n", strings.Repeat("=", 80))
					fmt.Println("=\t ", strings.Split(response.Packet_Senderid, "_")[0], " requested to delete")
					fmt.Println("=\t\t", strings.Split(response.Packet_PiggyBack[0], "_")[0])
					fmt.Println(strings.Repeat("=", 80))
				}
			} else if response.Packet_Type == utils.JOIN {
				// Received a JOIN message, inserting an ID to the array
				_, inserted := MembershipListManager(Action{INSERT, response.Packet_PiggyBack[0]})
				// If it is a new info, disseminate
				if inserted {
					disseminate_packet := Packet{this_id, response.Packet_Type, []string{response.Packet_PiggyBack[0]}}
					disseminate_message, err := ConvertToSend(disseminate_packet)
					if err != nil {
						log.Println("Unable to send disseminate message: ", err)
					} else {
						DisseminatePacket(disseminate_message)
					}
					log.Println(response.Packet_Senderid, " requested on inserting ", response.Packet_PiggyBack[0])
				}
			} else if this_ip != INTRODUCER_IP && response.Packet_Type == utils.INTRODUCE {
				// If a Non introducer receives an INTRODUCE message
				MembershipListManager(Action{INSERT, response.Packet_Senderid})
				ml, _ := MembershipListManager(Action{READ_ML, ""})
				reply_packet := Packet{this_id, "INTRODUCE_ACK", ml}
				reply_message, err := ConvertToSend(reply_packet)
				if err != nil {
					log.Println("Unable to ConvertToSend message in NodeServer()", err)
				} else {
					// Reply the Introducer the full list
					_, _ = node_server.WriteToUDP(reply_message, node_addr)
				}
			}
			membership_mutex.Unlock()
		}
	}
}

// This thread constantly PING neighbors
func NodeClient() {
	// Every time interval
	for {
		select {
		case <-ticker.C:
			membership_mutex.Lock()
			ml, _ := MembershipListManager(Action{READ_ML, ""})
			membership_mutex.Unlock()
			n := len(ml)
			if n <= 4 {
				// Ping all other node
				for _, member_id := range ml {
					if this_id != member_id {
						go Ping(member_id)
					}
				}
			} else {
				for i, member := range ml {
					// PING two successors and one predecessor
					if member == this_id {
						pred_id_1 := ml[utils.Mod(i-1, n)]
						suc_id_1 := ml[utils.Mod(i+1, n)]
						suc_id_2 := ml[utils.Mod(i+2, n)]
						for _, neighbor := range []string{pred_id_1, suc_id_1, suc_id_2} {
							go Ping(neighbor)
						}
						break
					}
				}
			}
		case <-done:
			return
		}
	}

}

func NewIntroducer() {
	for {
		select {
		case new_introducer := <-new_introducer_channel:
			INTRODUCER_IP = new_introducer
			MASTER_ADDRESS = fmt.Sprintf("%s:%d", INTRODUCER_IP, MASTER_PORT_NUMBER)
		}
	}
}

// TODO: Before allocation happens, must initialize the job status map!
func InitializeJobStatus(job_id int, model_type string) {
	// Initializes the num_workers, batch_size, etc
	// Calculates the total task for each process,
	// Assigns appropriate test_files

	// If the job exists, delete it and re-initialize it
	if _, ok := job_status[job_id]; ok {
		delete(job_status, job_id)
	}
	var dir string
	if model_type == "speech" {
		dir = test_dir[0]
	} else if model_type == "image" {
		dir = test_dir[1]
	} else {
		panic("underfined model type")
	}
	all_files := dir_test_files_map[dir] // Gets all the test files under dir.

	//! Fix the hardcoded batch_size_1 (if needed)
	new_status := JobStatus{job_id: job_id, batch_size: batch_size_1, model_type: model_type}
	mem_list, _ := GetMembershipList() //TODO: ADD LOCKING
	N := len(mem_list)
	new_status.num_workers = N
	new_status.each_process_total_task = len(all_files) / N
	start, end := 0, 0
	for i, process := range mem_list {
		start = i * new_status.each_process_total_task
		end = start + new_status.each_process_total_task
		new_status.process_test_files[process] = all_files[start:end]
		new_status.process_allocation[process] = i     // Assign batch number.
		new_status.process_batch_progress[process] = 0 // progress set to 0.
	}
	// Handle leftovers (total task per process alaways round down)
	if end < len(all_files) {
		last_process := mem_list[N-1]
		for _, left_over := range all_files[end:] {
			new_status.process_test_files[last_process] = append(new_status.process_test_files[last_process], left_over)
		}
	}
	job_status[job_id] = new_status
	log.Printf("Job id %v (model type: %v) initialized!", job_id, model_type)
}

// Keeps on sending test files for each process by batch.
// TODO: TO BE DELETED ONCE RoundRobin IS DONE.
func Allocate(process string, total_task int, batch int, testfiles []string) {
	defer ScheduleWaitGroup.Done()
	// Fetch a batch at a time
	for i := 0; i < total_task; i += batch {
		if len(jobs) == 2 {
			break
		}
		end := i + batch
		if end >= total_task {
			end = total_task - 1
		}
		current_batch := testfiles[i:end]
		fmt.Printf("Current batch files: %v", current_batch)
		// Array of current batch file's metadata
		// files_replicas := make([]utils.FileMetaData, len(current_batch))
		files_replicas := make(map[string]utils.FileMetaData)
		// For each file in the batch, send it through channel.
		for _, filename := range current_batch {
			file_meta := file_metadata[filename]
			files_replicas[filename] = file_meta
		}
		// TODO: Call askToReplicate and pass in files_replicas
		// client.FetchBatchData(process, files_replicas)

		log.Printf("Process %v's batch number %v is done! Moving on to the next batch.", process, i)
	}

	// Handle 2 jobs currently
}

// called on the per-process basis: round-robins style allocation
// TODO: Change parameters. Inside len(jobs) == 1, should just use job_status datastructure to determine progress.
func RoundRobin(process string, total_task int, batch int, testfiles []string) {
	defer ScheduleWaitGroup.Done()
	for {
		// job:= -1
		if len(jobs) == 0 {
			current_job = -1
			jobs = []int{} // Clear the jobs
			break
		} else if len(jobs) == 1 {
			// Just one job.
			current_job = 0
			// job := jobs[current_job]
			process_total_task := job_status[current_job].each_process_total_task
			test_files := job_status[current_job].process_test_files[process]
			current_batch := job_status[current_job].process_batch_progress[process]
			batch_size := job_status[current_job].batch_size
			for start_index := current_batch * batch_size; start_index < process_total_task; start_index += batch_size {
				if len(jobs) == 2 {
					break
				}
				end := start_index + batch_size
				if end >= process_total_task {
					end = process_total_task - 1
				}
				current_batch := test_files[start_index:end]
				fmt.Printf("Current batch files: %v", current_batch)
				// Array of current batch file's metadata
				// files_replicas := make([]utils.FileMetaData, len(current_batch))
				files_replicas := make(map[string]utils.FileMetaData)
				// For each file in the batch, send it through channel.
				for _, filename := range current_batch {
					file_meta := file_metadata[filename]
					files_replicas[filename] = file_meta
				}
				// TODO: Call askToReplicate and pass in files_replicas
				// client.FetchBatchData(process, files_replicas) // Wait until this finishes

				log.Printf("Process %v's batch number %v is done! Moving on to the next batch.", process, start_index)
			}

		} else if len(jobs) == 2 {
			// job := jobs[current_job]

			for {
				// Round robin between two jobs
				process_total_task := job_status[current_job].each_process_total_task
				test_files := job_status[current_job].process_test_files[process]
				current_batch := job_status[current_job].process_batch_progress[process]
				batch_size := job_status[current_job].batch_size
				start_index := current_batch * batch_size
				end_index := start_index + batch_size
				if end_index >= process_total_task {
					end_index = process_total_task - 1
				}
				// TODO: Fix testfiles; should put it into testfile
				batch_files := test_files[start_index:end_index]
				fmt.Printf("Current batch files: %v", current_batch)
				// Array of current batch file's metadata
				// files_replicas := make([]utils.FileMetaData, len(current_batch))
				files_replicas := make(map[string]utils.FileMetaData)
				// For each file in the batch, send it through channel.
				for _, filename := range batch_files {
					file_meta := file_metadata[filename]
					files_replicas[filename] = file_meta
				}

				// Update Status
				new_batch := current_batch + 1
				if new_batch*batch_size > process_total_task {
					// TODO: Then this job is finished.
					jobs = RemoveFromIntList(jobs, current_job)
					current_job = 0
					break // Now there is only one job left, continue
				} else {
					job_status[current_job].process_batch_progress[process] = new_batch
				}
				current_job = (current_job + 1) % 2 // 0 ->1 or 1 -> 0
			}
		}
		if current_job != -1 {

		}
	}

}

// This thread acts as the scheduler that allocates resources
func SchedulerServer() {
	for {
		select {
		case new_job := <-JobsQueue:
			log.Printf("New job received: %v", new_job)
			// inference job_id
			if round_robin_running {
				// TODO FIX new job's name
				jobs = append(jobs, 1) // 2nd job
			} else {
				jobs = append(jobs, 0) // 1st job
				round_robin_running = true
				// go RoundRobin()
			}

			// command format: run model_name test_set_path
			// testset_directory := new_job[2]
			// test_files := []string{}
			// for file, _ := range file_metadata {
			// 	if strings.HasPrefix(file, "/") {
			// 		test_files = append(test_files, file)
			// 	}
			// }
			// // log.Printf("Test files: %v", test_files)
			// number_files := len(test_files)
			// each_vm_tasks := number_files / len(membership_list)
			// members_host := GetHostsFromID(membership_list) // Get rid of timestamp
			// for i, process := range members_host {
			// 	ScheduleWaitGroup.Add(1)
			// 	// Allocate the test files for each process concurrently.
			// 	start := i * each_vm_tasks
			// 	end := i*each_vm_tasks + each_vm_tasks
			// 	go Allocate(process, each_vm_tasks, batch_size_1, test_files[start:end])
			// }
			// ScheduleWaitGroup.Wait()

			// fmt.Printf("Job for %v\n is DONE!", new_job[1])
		}
	}
}

// This handles the master server logic, it checks the incomming messages from
// other nodes and processes them accordingly.
func MasterServer() {
	for {
		select {
		// Receive from Filesystem Server
		case client_order := <-MasterIncommingChannel:
			log.Print("Something: ", client_order)
			if client_order.Action == utils.PUT {
				filename := client_order.Sdfsfile
				if file, ok := file_metadata[filename]; ok {
					// If seens this file, increase version and reply
					file.Version += 1
					file_metadata[filename] = file
					MasterOutgoingChannel <- utils.ChannelOutMessage{
						Action:   FS_PUT,
						Replicas: file.Replicas,
						Version:  file.Version}
				} else {
					// Create new file record
					membership_mutex.Lock()
					mem_list, _ := GetMembershipList()
					membership_mutex.Unlock()
					replicas := HashedReplicas(mem_list)
					file_metadata[filename] = utils.FileMetaData{Version: 1, Replicas: replicas}
					// Add this record to the replica process' metadadta:
					for _, v := range replicas {
						node_metadata[v] = append(node_metadata[v], filename)
					}
					log.Printf("From Master Node: The file %v is sent to : %v", filename, replicas)
					// Reply
					MasterOutgoingChannel <- utils.ChannelOutMessage{
						Action:   FS_PUT,
						Replicas: replicas,
						Version:  1}
				}
			} else if client_order.Action == utils.GET || client_order.Action == utils.NUM_VERSION {
				filename := client_order.Sdfsfile
				if file, ok := file_metadata[filename]; ok {
					// If the target file for GET exists, return list of IPs.
					MasterOutgoingChannel <- utils.ChannelOutMessage{
						Action:   FS_GET,
						Replicas: file.Replicas,
						Version:  file.Version}
				} else {
					// The file does not exists:
					MasterOutgoingChannel <- utils.ChannelOutMessage{
						Action:   FS_GET,
						Replicas: []string{""},
						Version:  -1}
				}
			} else if client_order.Action == utils.DELETE {
				filename := client_order.Sdfsfile
				if file, ok := file_metadata[filename]; ok {
					// If the target file for GET exists, return list of IPs.
					replicas := file.Replicas
					delete(file_metadata, filename)
					// For each node, delete the file from its file list.
					for _, v := range replicas {
						updated_file_list := RemoveFromList(node_metadata[v], filename)
						node_metadata[v] = updated_file_list
						// TODO: If len of node_metadata[v] is 0, we can remove the process.
					}
					MasterOutgoingChannel <- utils.ChannelOutMessage{
						Action:   FS_DELETE,
						Replicas: replicas,
						Version:  file.Version}
				} else {
					// The file does not exists:
					MasterOutgoingChannel <- utils.ChannelOutMessage{
						Action:   FS_DELETE,
						Replicas: []string{""},
						Version:  -1}
				}

			} else if client_order.Action == utils.LS {
				filename := client_order.Sdfsfile
				if file, ok := file_metadata[filename]; ok {
					replicas := file.Replicas
					MasterOutgoingChannel <- utils.ChannelOutMessage{
						Action:   FS_LS,
						Replicas: replicas,
						Version:  -1}
				} else {
					MasterOutgoingChannel <- utils.ChannelOutMessage{
						Action:   FS_LS,
						Replicas: []string{""},
						Version:  -1}
				}
			} else if client_order.Action == utils.TRAIN {
				// Requested by Client to initialize train
				membership_mutex.Lock()
				mem_list, _ := GetMembershipList()
				membership_mutex.Unlock()
				for i := range mem_list {
					mem_list[i] = ExtractIPFromID(mem_list[i])
				}
				MasterOutgoingChannel <- utils.ChannelOutMessage{
					Action:   utils.TRAIN,
					Replicas: mem_list,
					Version:  -1}
			}
			// MasterFailChannel is filled after the failed_process is deleted
		case failed_process := <-MasterFailChannel:
			/*
				Two cases to receive other nodes' failure messages:
				1. From server (received others' failure message).
				2. fro client's ping (itself detected other's failure.
			*/
			log.Print("\n\n" + strings.Repeat("=", 80) + "Received failed process" + "\n" + strings.Repeat("=", 80))
			failed_process = strings.Split(failed_process, "_")[0]
			log.Printf("Process %v failed", failed_process)
			if files, ok := node_metadata[failed_process]; ok {
				// Delete the process first
				log.Printf("Deleting process %v from the metadata", failed_process)
				delete(node_metadata, failed_process)
				/* Loop through each file from the failed process and re-assign. */
				for _, file := range files {
					log.Printf("Reassigning file %v", file)
					replicas := file_metadata[file].Replicas
					membership_mutex.Lock()
					memlist_copy, _ := GetMembershipList()
					membership_mutex.Unlock()
					memlist_copy = GetHostsFromID(memlist_copy)
					if len(memlist_copy) < 4 {
						// If total node less than 4, no more new replica.
						new_replicas := file_metadata[file]
						/* Remove the failed process from file's replica.*/
						new_replicas.Replicas = RemoveFromList(replicas, failed_process)
						file_metadata[file] = new_replicas
						break
					}
					/* Find all non-replcia of this file's processes */
					for _, replica := range replicas {
						memlist_copy = RemoveFromList(memlist_copy, replica)
					}
					log.Print("Memlist before hashing: ", memlist_copy)
					candidate := HashedReplicas(memlist_copy)[0]
					// Find available process that holds the file
					avaliables := RemoveFromList(replicas, failed_process)
					// Update filedata's replicas:
					new_file_metadata := file_metadata[file]
					new_file_metadata.Replicas = append(avaliables, candidate)
					highest_vers := new_file_metadata.Version
					file_metadata[file] = new_file_metadata

					node_metadata[candidate] = append(node_metadata[candidate], file)

					// Send this to filesystem manager
					log.Printf("Now requesting replica on %v to send file %v to candidate %v", avaliables, file, candidate)
					go client.ClientRequestReplicas(file, candidate, avaliables, highest_vers)
				}
			}
			// Receive from finish channel
			// case <-filesystem_finish_channel:
			// 	return
		}
	}
}

func GetHostsFromID(mem_list []string) []string {
	result := []string{}
	for _, v := range mem_list {
		result = append(result, strings.Split(v, "_")[0])
	}
	return result
}

func RemoveFromList(list []string, target string) []string {
	for i, other := range list {
		if other == target {
			return append(list[:i], list[i+1:]...)
		}
	}
	return list
}

func RemoveFromIntList(list []int, target int) []int {
	for i, other := range list {
		if other == target {
			return append(list[:i], list[i+1:]...)
		}
	}
	return list
}

func HashedReplicas(memlist []string) []string {
	if len(memlist) < 4 {
		return GetHostsFromID(memlist)
	}
	rand.Seed(time.Now().UTC().UnixNano())
	hashed_list := make([]string, len(memlist))
	permutation := rand.Perm(len(memlist))

	for i, v := range permutation {
		hashed_list[v] = strings.Split(memlist[i], "_")[0]
	}
	// Return the first four processes of the shuffled list.
	return hashed_list[0:4]
}

// Ping the target id and check if it fails
func Ping(target_id string) {
	// generate target_ip
	target_ip := ExtractIPFromID(target_id)
	target_address, err := net.ResolveUDPAddr("udp", target_ip+":"+strconv.Itoa(PORT_NUMBER))
	utils.LogError(err, "Unable to resolve target address in Ping()", false)

	// create Ping Message
	ping_request_packet := Packet{this_id, utils.PING, []string{}}
	ping_request, err := ConvertToSend(ping_request_packet)
	utils.LogError(err, "Unable to convert ping request packet to bytes in NodeClient()", false)

	// Connection fails
	connection, err := net.DialUDP("udp", nil, target_address)
	utils.LogError(err, "Unable to establish connection with target in Ping()", false)

	// Send Ping to the target_id
	_, err = connection.Write(ping_request)
	utils.LogError(err, "Unable to write to neighbor with target in Ping()", false)

	// Set time out
	connection.SetDeadline(time.Now().Add(PING_TIMEOUT))

	// Generate response from the PING
	response_buffer := make([]byte, 1024)
	_, err = connection.Read(response_buffer)
	if err != nil {
		log.Println("Failure Detection "+target_id+" failed!: ", err)
		fmt.Println(strings.Repeat("=", 80))
		fmt.Println("=\tFailure Detection "+target_id+" failed!: ", err)
		fmt.Println(strings.Repeat("=", 80))

		// Detects a failure
		// First give the failed process a notice of failure

		failure_notice_packet := Packet{this_id, utils.FAILURE, []string{target_id}}
		failure_notice_request, err := ConvertToSend(failure_notice_packet)
		utils.LogError(err, "Unable to write to neighbor with target in Ping()", false)
		_, _ = connection.Write(failure_notice_request)
		// Then update membership list and notify others
		membership_mutex.Lock()
		MembershipListManager(Action{DELETE, target_id})
		failed_ip := strings.Split(target_id, "_")[0]
		if failed_ip == INTRODUCER_IP {
			/* Detects the introducer/master failed. */
			this_process_index := utils.IndexOf(this_id, membership_list)
			master_index := utils.IndexOf(target_id, membership_list)
			greatest_index := len(membership_list) - 1
			if master_index != greatest_index && this_process_index == greatest_index {
				INTRODUCER_IP = this_host
				MASTER_ADDRESS = fmt.Sprintf("%s:%d", INTRODUCER_IP, MASTER_PORT_NUMBER)
				memlist_copy, _ := GetMembershipList()
				memlist_copy = GetHostsFromID(memlist_copy)
				ProcessFiles := client.ClientMasterElect(RemoveFromList(memlist_copy, target_id))
				InitializeMetadata(ProcessFiles)
				go MasterServer()
				go IntroduceServer()

			} else if master_index == greatest_index && this_process_index == greatest_index-1 {
				INTRODUCER_IP = this_host
				MASTER_ADDRESS = fmt.Sprintf("%s:%d", INTRODUCER_IP, MASTER_PORT_NUMBER)
				memlist_copy, _ := GetMembershipList()
				memlist_copy = GetHostsFromID(memlist_copy)
				ProcessFiles := client.ClientMasterElect(RemoveFromList(memlist_copy, target_id))
				InitializeMetadata(ProcessFiles)
				go MasterServer()
				go IntroduceServer()
			}
		}
		NotifyFailure(target_id)
		membership_mutex.Unlock()
		if this_host == INTRODUCER_IP {
			MasterFailChannel <- target_id
		}
	} else {
		// do nothing
	}

}

// Create a FAILURE package and Dissmeniate the packet to neighbor
func NotifyFailure(failed_id string) {
	notify_request_packet := Packet{this_id, utils.FAILURE, []string{failed_id}}
	notify_request, err := ConvertToSend(notify_request_packet)
	utils.LogError(err, "Unable to convert notify request packet to bytes in NotifyFailure()", false)
	DisseminatePacket(notify_request)
}

// Send the packet to the neighbor
// Must be locked prior and unlocked after
// Used to send one time message that does not require ACK back
func DisseminatePacket(udp_message []byte) {
	// If each node has less than 3 neighbors
	if len(membership_list) > 4 {
		for i, member := range membership_list {
			// send the packet to neighbors
			if member == this_id {
				pred_ip_addr_1 := ExtractIPFromID(membership_list[utils.Mod(i-1, len(membership_list))])
				suc_ip_addr_1 := ExtractIPFromID(membership_list[utils.Mod(i+1, len(membership_list))])
				suc_ip_addr_2 := ExtractIPFromID(membership_list[utils.Mod(i+2, len(membership_list))])
				for _, neighbor := range []string{pred_ip_addr_1, suc_ip_addr_1, suc_ip_addr_2} {
					neighbor_udp_addr, err := net.ResolveUDPAddr("udp", neighbor+":"+strconv.Itoa(PORT_NUMBER))
					utils.LogError(err, "Unable to resolve Introducer's Neighbor UDP Addr: ", false)
					udp_connection, err := net.DialUDP("udp", nil, neighbor_udp_addr)
					utils.LogError(err, "Unable to establish connection with target in DisseminatePacket()", false)
					_, _ = udp_connection.Write(udp_message)
					// Close the connection since we do not care the result
					udp_connection.Close()
				}
				break
			}
		}
	} else {
		for _, member_id := range membership_list {
			if member_id == this_id {
				continue
			}
			neighbor_ip_addr := ExtractIPFromID(member_id)
			neighbor_udp_addr, err := net.ResolveUDPAddr("udp", neighbor_ip_addr+":"+strconv.Itoa(PORT_NUMBER))
			utils.LogError(err, "Unable to resolve Introducer's Neighbor UDP Addr: ", false)
			udp_connection, err := net.DialUDP("udp", nil, neighbor_udp_addr)
			utils.LogError(err, "Unable to establish connection with target in DisseminatePacket()", false)
			_, _ = udp_connection.Write(udp_message)
			udp_connection.Close()
		}
	}
}

func BinarySearch(target_id string) (int, int) {
	left := 0
	right := len(membership_list) - 1
	for left <= right {
		mid := (left + right) * 1.0 / 2
		if membership_list[mid] == target_id {
			return mid, mid
		} else if membership_list[mid] < target_id {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return -1, left
}

func ExtractIPFromID(target_id string) string {
	return strings.Split(target_id, "_")[0]
}

/* Returns the non-loopback local IP of the host */
/* https://stackoverflow.com/questions/23558425/how-do-i-get-the-local-ip-address-in-go */
func GetIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback then display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}

func SetupFiles() {
	os.RemoveAll("./log")
	os.RemoveAll("./downloaded")
	os.RemoveAll("./targets")
	utils.CreateFileDirectory("./log/log.log")
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	var err error
	log_dir := "./log/log.log"
	utils.CreateFileDirectory(log_dir)
	log_file, err = os.OpenFile(log_dir, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0766)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	log.SetOutput(log_file)
}

/* Convert packet to Bytes for UDP */
func ConvertToSend(p Packet) ([]byte, error) {
	marshal_packet, err := json.Marshal(p)
	if err != nil {
		return []byte{}, err
	}
	return marshal_packet, nil

}

/* Convert UDP response to packet struct */
func ConvertFromResponse(response []byte) (Packet, error) {
	var p Packet
	err := json.Unmarshal(response, &p)
	if err != nil {
		return p, err
	}
	return p, nil
}

func MembershipListManager(action Action) ([]string, bool) {
	switch action.ActionType {
	case INSERT:
		return InsertToMembershipList(action.TargetID)
	case DELETE:
		return DeleteFromMembershipList(action.TargetID)
	case READ_ML:
		return GetMembershipList()
	default:
		log.Println("Unable to recognize action type for update membership list")
	}
	return []string{}, false
}

/* Inserts target to membership list. Returns updated membership list. */
func InsertToMembershipList(target string) ([]string, bool) {
	found, _ := BinarySearch(target)
	if found == -1 {
		membership_list = append(membership_list, target)
		sort.Strings(membership_list)
		return membership_list, true
	}
	return membership_list, false
}

/* Removes target from membership list. Returns updated membership list. */
func DeleteFromMembershipList(target string) ([]string, bool) {
	found, index := BinarySearch(target)
	if found == -1 {
		return membership_list, false
	}
	membership_list = append(membership_list[:index], membership_list[index+1:]...)
	return membership_list, true
}

/* Returns the curernt membership list this process has */
func GetMembershipList() ([]string, bool) {
	membership_list_copy := make([]string, len(membership_list))
	copy(membership_list_copy, membership_list)
	return membership_list_copy, false
}
