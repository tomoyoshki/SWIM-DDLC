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

func (c PythonClient) InitializeModel(ctx context.Context, job_id int, batch_size int, model_type string) (string, error) {
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(10*time.Second))
	defer cancel()
	res, err := c.client.InitializeModel(ctx, &pythonproto.InitializeRequest{
		JobId:     int32(job_id),
		BatchSize: int32(batch_size),
		ModelType: model_type,
	})
	if err != nil {
		log.Printf("%v", err)
		return "", err
	}
	log.Printf("Response: %v\n", res.Status)
	return res.Status, err
}

func (c PythonClient) ModelInference(ctx context.Context, job_id int, batch_id int, inference_size int, data_folder string) ([]byte, error) {
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(100*time.Second))
	defer cancel()
	res, err := c.client.ModelInference(ctx, &pythonproto.InferenceRequest{
		JobId:               int32(job_id),
		InferenceSize:       int32(inference_size),
		InferenceDataFolder: data_folder,
	})

	if err != nil {
		log.Printf("ModelInference: %v", err)
		return []byte{}, err
	}
	return res.InferenceResult, nil
}
