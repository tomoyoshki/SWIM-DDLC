package fileserver

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"

	"cs425mp4/client_modules/client"
	"cs425mp4/client_modules/client_model"
	fileproto "cs425mp4/proto/filetransfer"
	"cs425mp4/storage"
	"cs425mp4/utils"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	sdfsfile_directory string = "targets/"
)

type Server struct {
	input_channel          chan utils.ChannelInMessage
	output_channel         chan utils.ChannelOutMessage
	new_introducer_channel chan string
	serverfileinfo         *[]string
	storage                storage.Manager
	fileproto.UnimplementedFileServiceServer
}

func NewServer(storage storage.Manager, input_channel chan utils.ChannelInMessage, output_channel chan utils.ChannelOutMessage, new_introducer_channel chan string, serverfileinfo *[]string) Server {
	log.Print("Creating new Server storage")
	return Server{
		input_channel:          input_channel,
		output_channel:         output_channel,
		new_introducer_channel: new_introducer_channel,
		serverfileinfo:         serverfileinfo,
		storage:                storage,
	}
}

func (s Server) Upload(stream fileproto.FileService_UploadServer) error {
	var name string
	var file *storage.File
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			if err := s.storage.Store(file); err != nil {
				return status.Error(codes.Internal, err.Error())
			}
			tempinfo := append(*s.serverfileinfo, name)
			*s.serverfileinfo = utils.RemoveDuplicateValues(tempinfo)
			return stream.SendAndClose(&fileproto.UploadResponse{Status: name})
		}
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}

		if name == "" {
			name = req.GetFilename()
			file = storage.NewFile(name)
			log.Printf("fileserver.go:Upload():Received file %v", name)
		}
		if err := file.Write(req.GetChunk()); err != nil {
			return status.Error(codes.Internal, err.Error())
		}
	}
}

// Server to Client
func (s Server) Download(in *fileproto.DownloadRequest, srv fileproto.FileService_DownloadServer) error {
	// Open the file
	sdfsfile := sdfsfile_directory + in.Filename
	fil, err := os.Open(sdfsfile)
	if err != nil {
		return err
	}
	// Maximum 1KB per message
	buf := make([]byte, 1024)
	log.Printf("Sending file %v to the client", sdfsfile)
	for {
		num, err := fil.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if err := srv.Send(
			&fileproto.DownloadResponse{
				Chunk: buf[:num],
			},
		); err != nil {
			return err
		}
	}
	return nil
}

func (s Server) Delete(ctx context.Context, req *fileproto.DeleteRequest) (*fileproto.DeleteResponse, error) {
	res := fileproto.DeleteResponse{
		Filename: req.Filename,
	}
	newfileinfo := []string{}
	// For each file we have under targets/
	for _, file := range *s.serverfileinfo {
		file_regex := "[0-9]+-" + req.Filename
		log.Printf("Checking if %v matches %v", file_regex, file)
		match, _ := regexp.MatchString(file_regex, file)
		// If the file is the target file
		if match {
			log.Printf("They match")
			log.Printf("Deleting the file %v", file)
			err := os.Remove(sdfsfile_directory + file)
			if err != nil {
				log.Printf("fileserver.go:Delete():Failed to remove file %v", sdfsfile_directory+file)
			}
		} else {
			newfileinfo = append(newfileinfo, file)
		}
	}

	*s.serverfileinfo = newfileinfo
	log.Printf("After delete: %v", newfileinfo)
	log.Printf("new server file info: %v", newfileinfo)
	return &res, nil
}

func (s Server) MasterRequest(ctx context.Context, req *fileproto.MasterNodeRequest) (*fileproto.MasterNodeResponse, error) {
	action := req.Action
	log.Print("Received request")

	s.input_channel <- utils.ChannelInMessage{
		Action:    int(action),
		Localfile: req.Localfilename,
		Sdfsfile:  req.Sdfsfilename,
	}
	log.Printf("Node.go successfully processed the request")
	out, _ := <-s.output_channel
	buf := &bytes.Buffer{}
	gob.NewEncoder(buf).Encode(out.Replicas)
	bs := buf.Bytes()
	res := fileproto.MasterNodeResponse{
		NumVersion: strconv.Itoa(out.Version),
		Replicas:   bs,
	}
	log.Print("Responding")
	return &res, nil
}

// Each process sends response including all te files they have back to the master
func (s Server) MasterElectBroadcast(ctx context.Context, req *fileproto.MasterElectRequest) (*fileproto.MasterElectResponse, error) {
	log.Print("Received request with master id: ", req.MasterId)
	s.new_introducer_channel <- req.MasterId
	buf := &bytes.Buffer{}
	gob.NewEncoder(buf).Encode(*s.serverfileinfo)
	bs := buf.Bytes()
	res := fileproto.MasterElectResponse{
		Message:   "Ok",
		ProcessId: "good",
		Details:   bs,
	}
	return &res, nil
}

// The node containing the file [req.Sdfsfilename] is sending it to some replica process [req.ReplicaAddr]
func (s Server) MasterAskToReplicate(ctx context.Context, req *fileproto.MasterReplicateRequest) (*fileproto.MasterReplicateResponse, error) {
	response := fileproto.MasterReplicateResponse{
		Response: "ok",
	}
	log.Printf("fileserver.go:MasterAskToReplicate(): Asked to send %v to addr %v", req.Sdfsfilename, req.ReplicaAddr)
	file_version, err := strconv.Atoi(req.Version)
	if err != nil {
		return &response, err
	}
	for i := 1; i <= file_version; i += 1 {
		temp_version := strconv.Itoa(i)
		err := client.ClientUpload(req.ReplicaAddr, "targets/"+temp_version+"-"+req.Sdfsfilename, temp_version+"-"+req.Sdfsfilename)
		if err != nil {
			response.Response = "Error uploading files"
			return &response, err
		}
	}
	return &response, err
}

// The node containing the file [req.Sdfsfilename] is sending it to some replica process [req.ReplicaAddr]
func (s Server) StartJob(ctx context.Context, req *fileproto.JobRequest) (*fileproto.JobResponse, error) {
	response := fileproto.JobResponse{
		Status: "OK",
	}
	s.input_channel <- utils.ChannelInMessage{
		Action:  int(utils.TRAIN),
		Version: int(req.JobId),
	}

	out, _ := <-s.output_channel

	var err error
	for _, member := range out.Replicas {
		_, err = client_model.AskMemberToInitializeModels(member+":3333", int(req.JobId), int(req.BatchSize), req.ModelType)
		if err != nil {
			log.Printf("Startjob failed to ask member to initialzie models for member: %v", member)
			break
		}
	}
	return &response, err
}

// First download files into a folder, and then starts inferencing
func (s Server) SendJobInformation(ctx context.Context, req *fileproto.JobInformationRequest) (*fileproto.JobInformationResponse, error) {
	response := fileproto.JobInformationResponse{
		Status: "OK",
	}

	log.Println("Received SendJobInformation()")
	file_prefix := fmt.Sprintf("python/data/%d/%d/", req.JobId, req.BatchId)
	var file_replicas map[string][]string
	gob.NewDecoder(bytes.NewReader(req.Replicas)).Decode(&file_replicas)

	// for each file in the batch, download it to python/data/job_id/batch_id/sdfsfilename
	for file_name, replicas := range file_replicas {
		for _, replica := range replicas {
			local_file_name := file_prefix + "test/" + file_name
			log.Printf("Replica: %v\n, filename: %v", replica, file_name)
			err := client.ClientDownload(replica, "../"+local_file_name, file_name)
			if err == nil {
				break
			}
		}
	}

	// Tell python to inference and get the result
	res, err := client_model.AskToInference("localhost:9999", int(req.JobId), int(req.BatchId), len(file_replicas), file_prefix)
	log.Print("Done inferencing")
	if err != nil {
		response.Status = "Failed to inference"
		return &response, err
	}
	log.Print("Have a result to send back")
	response.InferenceResult = res
	return &response, nil
}

func (s Server) AskMemberToInitializeModels(ctx context.Context, req *fileproto.ModelTrainRequest) (*fileproto.ModelTrainResponse, error) {
	response := fileproto.ModelTrainResponse{
		Status: "OK",
	}
	_, err := client_model.AskToInitializeModel("localhost:9999", int(req.JobId), int(req.BatchSize), req.ModelType)
	if err != nil {
		log.Println("AskMemberToInitializeModels failed to call AskToInitializeModel()")
		response.Status = "Error"
	}
	return &response, err
}
