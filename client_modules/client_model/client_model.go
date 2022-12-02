package client_model

import (
	"context"
	"log"

	"cs425mp4/grpcclient/fileclient"
	"cs425mp4/grpcclient/pythonclient"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// // Client asks Server to setup the model in all virtual machines
func ClientStartJob(addr string, job_id int, batch_size int, model_type string) (string, error) {
	log.Printf("Client requesting to start job")
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("ClientStartJob() did not connect: %v", err)
		return "", err
	}
	defer conn.Close()

	client := fileclient.NewClient(conn, nil)
	res_status, err := client.StartJob(context.Background(), job_id, batch_size, model_type)
	if res_status != "OK" {
		log.Print("Failed to initialize model")
	}
	return res_status, err
}

// Server send job information for member to request files to download
func SendInferenceInformation(addr string, job_id int, batch_id int, file_replicas map[string][]string) string {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("SendInferenceInformation() did not connect: %v", err)
		return "Connection Failed"
	}
	defer conn.Close()
	client := fileclient.NewClient(conn, nil)
	res_status, err := client.SendJobInformation(context.Background(), batch_id, job_id, file_replicas)
	if res_status != "OK" {
		log.Printf("SendInferenceInformation() failed")
		return "Send Information Failed Failed"
	}
	log.Print("Good status")
	return "OK"
}

// Master asks Members in the Membership list to initialize their models
func AskMemberToInitializeModels(addr string, job_id int, batch_size int, model_type string) (string, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("SendInferenceInformation() did not connect: %v", err)
		return "Connection Failed", err
	}
	defer conn.Close()
	client := fileclient.NewClient(conn, nil)
	res, err := client.AskMemberToInitializeModels(context.Background(), job_id, batch_size, model_type)
	if res != "OK" {
		log.Printf("AskMemberToInitializeModels() failed")
		return "Send Information Failed Failed", err
	}
	return "OK", nil
}

// Go Member ask Python Server to initialize model
func AskToInitializeModel(addr string, job_id int, batch_size int, model_type string) (string, error) {
	log.Println("Requested to initialize model job")
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("ClientAskToReplicate() did not connect: %v", err)
		return "", err
	}
	defer conn.Close()
	client := pythonclient.NewPythonClient(conn)
	res_status, err := client.InitializeModel(context.Background(), job_id, batch_size, model_type)
	if res_status != "OK" {
		log.Print("Failed to initialize model")
	}
	return res_status, err
}

// Go Member Ask Python Server to inference on takss
func AskToInference(addr string, job_id int, batch_id int, inference_size int, data_folder string) ([]byte, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("AskToInference() did not connect: %v", err)
		return nil, err
	}
	defer conn.Close()
	client := pythonclient.NewPythonClient(conn)
	res_status, err := client.ModelInference(context.Background(), job_id, batch_id, inference_size, data_folder)
	return res_status, nil
}
