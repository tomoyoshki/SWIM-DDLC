package fileserver

import (
	"bytes"
	"context"
	"encoding/gob"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"

	"cs425mp3/client"
	fileproto "cs425mp3/proto/filetransfer"
	"cs425mp3/storage"
	"cs425mp3/utils"

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
			return &response, err
		}
	}
	return &response, err
}
