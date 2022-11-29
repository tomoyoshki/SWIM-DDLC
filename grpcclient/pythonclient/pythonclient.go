package pythonclient

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"

	pythonproto "cs425mp4/proto/gopython"
)

type PythonClient struct {
	client pythonproto.GoPythonClient
}

func NewPythonClient(conn grpc.ClientConnInterface) PythonClient {
	return PythonClient{
		client: pythonproto.NewGoPythonClient(conn),
	}
}

func (c PythonClient) SetBatchSize(ctx context.Context, batch_size int) error {
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(10*time.Second))
	defer cancel()
	res, err := c.client.SetBatchSize(ctx, &pythonproto.BatchRequest{
		BatchSize: int32(batch_size),
	})
	if err != nil {
		log.Printf("%v", err)
	}
	log.Printf("Response: %v\n", res.Status)
	return err
}
