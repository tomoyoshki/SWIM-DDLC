package fileclient

import (
	"bytes"
	"context"
	"encoding/gob"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	fileproto "cs425mp3/proto/filetransfer"
	"cs425mp3/storage"
	"cs425mp3/utils"
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
