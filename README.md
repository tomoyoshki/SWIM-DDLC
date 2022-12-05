# CS 425 MP 4 Distributed ML Inferencing

- Ruipeng Han (ruipeng2@illinois.edu)
- Tomoyoshi Kimura (tkimura4@illinois.edu)

For this project, we implemented a variant of roubin-robin schduling algorithm to enable fair allocation of tasks for inferencing multiple machine larning models across distirbuted nodes.


## Project structure

```
.
├── README.md
├── client
│   └── client.go
├── data
│   ├── ...
├── fileclient
│   └── fileclient.go
├── fileserver
│   └── fileserver.go
├── go.mod
├── go.sum
├── grep
│   ├── logger
│   └── querier
├── log
├── machine_i.log
├── node.go
├── proto
│   ├── filetransfer
│   │   ├── filetransfer.pb.go
│   │   ├── filetransfer.proto
│   │   └── filetransfer_grpc.pb.go
│   └── packet
│       ├── packet.pb.go
│       ├── packet.proto
│       └── packet_grpc.pb.go
├── report
│   └── CS 425 MP 3 Report.pdf
├── server
│   └── server.go
├── storage
│   ├── file.go
│   └── storage.go
├── targets
│   └── hrpmyson.dat
├── test
└── utils
    └── utils.go
```

## Usage

### Run go program directly

```bash
go run node.go
```

### Build and run 

```bash
go build node.go
./node
```

### Commands
Once build and run, you are entered a shell. Here are list of four commands you can do:

**Membership List Commands**

- ```join```: This will makes the current process joins the group. If this is an introducer process, it will joins as an introducer role; otherwise it will ask the existing introducer to join the group.

- ```list_mem```: This will output a list of current members on the process's membership list.

- ```leave```: This command will make the process leave voluntarily. Running this command will notify its neighbors that it will leave, who will update their membership lists accordingly and propagate the leave message to their neighbors.

- ```list_self```: This command will list the current process's id.

**Filesystem Commands**

- ```put localfilename sdfsfilename```: Put the localfile `localfilename` as `sdfsfilename` on the filesystem
- ```get sdfsfilename localfilename```: Gets the file `sdfsfilename` from the filesystem as `localfilename`
- ```delete sdfsfilename```: Deletes the `sdfsfilename` on the server
- ```ls sdfsfilename```: Lists machines that have `sdfsfilename`
- ```store```: Lists files on the current node as a part of the filesystem
- ```get-versions sdfsfilename num-versions localfilename```: Gets the **last** `num-versions` files from the filesystem


**Distributed ML Commands**

- ```load_test_dataset```: Loads the test dataset that will be used for all ML models for inferencing.
- ```start_job job_id batch_size model_type model_name```: Initializes and creates a job status for `model_name` with `job_id` and `batch_size`.
- ```inference job_id```: Starts inferencing for the model with job id `job_id `
- ```job_status job_id```: Lists the current job status for the job with id `job_id `

