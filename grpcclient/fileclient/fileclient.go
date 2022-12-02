package fileclient

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	fileproto "cs425mp4/proto/filetransfer"
	"cs425mp4/storage"
	"cs425mp4/utils"
)

type Client struct {
	storage storage.Manager
	client  fileproto.FileServiceClient
}

func NewClient(conn grpc.ClientConnInterface, storage storage.Manager) Client {
	return Client{
		client:  fileproto.NewFileServiceClient(conn),
		storage: storage,
	}
}

func (c Client) Upload(ctx context.Context, localfilename string, sdfsfilename string) (string, error) {
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(1000*time.Second))
	defer cancel()

	stream, err := c.client.Upload(ctx)
	if err != nil {
		return "", err
	}

	fil, err := os.Open(localfilename)
	if err != nil {
		return "", err
	}

	// Maximum 128MB size per stream.
	buf := make([]byte, 11*1024*1024)

	log.Printf("fileclient.go:Upload():Sending file %v to server as %v", localfilename, sdfsfilename)
	total := 0
	for {
		num, err := fil.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}

		if err := stream.Send(
			&fileproto.UploadRequest{
				Chunk:    buf[:num],
				Filename: sdfsfilename,
			},
		); err != nil {
			return "", err
		}
		total += num
		log.Printf("Send %v bytes", total)
	}

	res, err := stream.CloseAndRecv()
	if err != nil {
		return "", err
	}
	log.Printf("Upload status: %v", res.GetStatus())
	return res.GetStatus(), nil
}

func (c Client) Download(localfilename string, sdfsfilename string) error {
	stream, err := c.client.Download(
		context.Background(),
		&fileproto.DownloadRequest{
			Filename: sdfsfilename,
		},
	)
	if err != nil {
		log.Fatal(err)
	}
	file := storage.NewFile(localfilename)
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			if err := c.storage.Store(file); err != nil {
				return err
			}

			return nil
		}
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		if err := file.Write(req.GetChunk()); err != nil {
			return status.Error(codes.Internal, err.Error())
		}
	}
}

func (c Client) Delete(ctx context.Context, sdfsfilename string) error {
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(10*time.Second))
	defer cancel()
	res, err := c.client.Delete(ctx, &fileproto.DeleteRequest{
		Filename: sdfsfilename,
	})
	if err != nil {
		log.Printf("fileclient.go:Delete():could not delete: %v", err)
		return err
	}
	log.Printf("Successfully deleted %v", res.Filename)
	return nil
}

// Return list of replica addresses, numversion_sdfsfilename, and error
func (c Client) MasterRequest(ctx context.Context, localfilename string, sdfsfilename string, action int) ([]string, string, error) {
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(10*time.Second))
	defer cancel()
	res, err := c.client.MasterRequest(ctx, &fileproto.MasterNodeRequest{
		Action:        int32(action),
		Localfilename: localfilename,
		Sdfsfilename:  sdfsfilename,
		NodeId:        "id",
	})
	if err != nil {
		return []string{}, "", err
	}

	var results []string
	gob.NewDecoder(bytes.NewReader(res.Replicas)).Decode(&results)

	if action == utils.NUM_VERSION {
		return results, res.NumVersion, nil
	}
	new_sdfsfilename := res.NumVersion + "-" + sdfsfilename
	return results, new_sdfsfilename, nil
}

func (c Client) MasterElectBroadcast(ctx context.Context) ([]string, error) {
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(10*time.Second))
	defer cancel()
	hostname, _ := os.Hostname()
	res, err := c.client.MasterElectBroadcast(ctx, &fileproto.MasterElectRequest{
		MasterId: hostname,
	})
	if err != nil {
		log.Printf("fileclient.go:MasterElectBroadcast():Could not master elect broadcst: %v", err)
		return []string{}, err
	}
	var results []string
	gob.NewDecoder(bytes.NewReader(res.Details)).Decode(&results)
	log.Printf("Received file information %v", results)
	return results, nil
}

// Master asks others to replicate information

func (c Client) MasterAskToReplicate(ctx context.Context, replica_addr string, sdfsfilename string, numversion int) error {
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(10*time.Second))
	defer cancel()
	_, err := c.client.MasterAskToReplicate(ctx, &fileproto.MasterReplicateRequest{
		ReplicaAddr:  replica_addr,
		Sdfsfilename: sdfsfilename,
		Version:      strconv.Itoa(numversion),
	})
	return err
}

// Ask the machine to train their models
func (c Client) StartJob(ctx context.Context, job_id int, batch_size int, model_type string) (string, error) {
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(10*time.Second))
	defer cancel()
	res, err := c.client.StartJob(ctx, &fileproto.JobRequest{
		JobId:     int32(job_id),
		BatchSize: int32(batch_size),
		ModelType: model_type,
	})
	if err != nil {
		log.Printf("Startjob() error: %v", err)
		return "", err
	}
	// log.Printf("Startjob result: %v", res.Status)
	return res.Status, err
}

// Coordinator tell some machine which replicas have files, return the result that machine inferenced on
func (c Client) SendJobInformation(ctx context.Context, batch_id int, job_id int, replicas map[string][]string) (string, error) {
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(100*time.Second))
	defer cancel()
	buf := &bytes.Buffer{}
	gob.NewEncoder(buf).Encode(replicas)
	replicas_bytes := buf.Bytes()
	res, err := c.client.SendJobInformation(ctx, &fileproto.JobInformationRequest{
		BatchId:  int32(batch_id),
		JobId:    int32(job_id),
		Replicas: replicas_bytes,
	})

	if err != nil {
		log.Printf("SendJobInformation(): %v", err)
		return "", err
	}
	ires := make(map[string][]string)
	err = json.Unmarshal(res.InferenceResult, &ires)
	for k, v := range ires {
		log.Printf("%v: %v", k, v)
	}
	return res.Status, nil
}

func (c Client) AskMemberToInitializeModels(ctx context.Context, job_id int, batch_size int, model_type string) (string, error) {
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(10*time.Second))
	defer cancel()
	res, err := c.client.AskMemberToInitializeModels(ctx, &fileproto.ModelTrainRequest{
		JobId:     int32(job_id),
		BatchSize: int32(batch_size),
		ModelType: model_type,
	})

	if err != nil {
		log.Printf("AskMemberToInitializeModels() fails")
		return "", err
	}

	return res.Status, nil
}
