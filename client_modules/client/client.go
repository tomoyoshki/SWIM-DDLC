package client

import (
	"context"
	"fmt"
	"log"
	"strings"

	"cs425mp4/grpcclient/fileclient"
	"cs425mp4/storage"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	python_addr = "localhost:9999"
)

// Called by a client trying to PUT a file [localfilename] as [sdfsfilename] to any process [addr]
func ClientUpload(addr string, localfilename string, sdfsfilename string) error {
	log.Println("client.go:ClientUpload(): uploading to addr: ", addr)
	// conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	maxMsgSize := 12 * 1024 * 1024
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize), grpc.MaxCallSendMsgSize(maxMsgSize)))
	if err != nil {
		log.Print("ClientUpload(): could not connect", err)
		return err
	}
	defer conn.Close()

	client := fileclient.NewClient(conn, nil)
	_, err = client.Upload(context.Background(), localfilename, sdfsfilename)
	if err != nil {
		log.Printf("ClientUpload(): Could not upload: %v", err)
		return err
	}
	log.Print("\nClientUpload() ended.\n")

	return nil
}

// Called by a client trying to GET a file [sdfsfilename] from any process [addr] as [localfilename]
func ClientDownload(addr string, localfilename string, sdfsfilename string) error {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Print("ClientDownload(): could not connect", err)
		return err
	}
	defer conn.Close()

	client := fileclient.NewClient(conn, storage.New("./downloaded/"))
	err = client.Download(localfilename, sdfsfilename)
	if err != nil {
		log.Print("ClientDownload(): could not download", err)
		return err
	}
	log.Printf("\n\nScuccessfully downloaded file %v from the server as %v\n\n", sdfsfilename, localfilename)
	return nil
}

// Called by a client trying to DELETE a file [sdfsfilename] from the master process [addr]
func ClientDelete(addr string, sdfsfilename string) error {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Print("ClientDelete(): could not connect", err)
		return err
	}
	defer conn.Close()

	client := fileclient.NewClient(conn, storage.New("./downloaded/"))
	err = client.Delete(context.Background(), sdfsfilename)

	if err != nil {
		log.Print("ClientDelete(): could not delete", err)
		return err
	}
	log.Printf("\n\nScuccessfully deleted file %v from the server\n\n", sdfsfilename)
	return nil
}

// Called by a client to request an [action] for [localfilename] as [sdfsfilename] to the master [addr]
func ClientRequest(addr string, localfilename string, sdfsfilename string, action int) ([]string, string, error) {
	log.Printf("Establishes connection to Client Request to %s", addr)
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("did ClientRequest() could not connect: %v", err)
		return []string{}, "", err
	}
	defer conn.Close()

	client := fileclient.NewClient(conn, storage.New("./downloaded/"))
	response, new_sdfsfilename, err := client.MasterRequest(context.Background(), localfilename, sdfsfilename, action)
	if err != nil {
		return []string{}, "", nil
	}
	log.Print("Response from request: ", response)
	return response, new_sdfsfilename, nil
}

// Called by client or the new Master Node to get all filenames on every server
func ClientMasterElect(LiveProcesses []string) map[string][]string {
	log.Print("Master asking for files from all nodes")
	/* Maps a file to its replicas */
	var ProcessFiles = make(map[string][]string)

	/* Loop through each process and ask their files */
	for _, address := range LiveProcesses {
		log.Printf("Sending broadcast elect message to %v", address)
		address = fmt.Sprintf("%v:3333", address)
		conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("ClientMasterElect():did not connect: %v", err)
		}
		defer conn.Close()

		client := fileclient.NewClient(conn, storage.New("./downloaded/"))
		// Returns list of files ([]string) that the process [address] has
		response, err := client.MasterElectBroadcast(context.Background())
		if err != nil {
			log.Print("Error receiving election response from ", address)
		}
		ProcessFiles[address] = response
	}
	log.Printf("Received from other processes: %v", ProcessFiles)
	return ProcessFiles
}

// Master ask a list of replicas to send the replicated files
func ClientRequestReplicas(sdfsfilename string, non_replica_addr string, replica_addresses []string, num_version int) {
	non_replica_addr_port := fmt.Sprintf("%s:%v", non_replica_addr, 3333)
	for _, replica_addr := range replica_addresses {
		addr := fmt.Sprintf("%s:%v", replica_addr, 3333)
		err := ClientAskToReplicate(addr, non_replica_addr_port, sdfsfilename, num_version)
		if err == nil {
			log.Printf("Successfully requested %v to send address", addr)
			break
		}
	}
	log.Print("\n" + strings.Repeat("=", 80) + "Finished requesting for replicas" + "\n" + strings.Repeat("=", 80))
}

// Called by a Master to ask some Process [addr] to send the file [sdfsfilename] to [non_replica_addr]
func ClientAskToReplicate(addr string, non_replica_addr string, sdfsfilename string, num_version int) error {
	log.Printf("Master Asking process [%v] to send file [%v] to process [%v]", addr, sdfsfilename, non_replica_addr)
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("ClientAskToReplicate() did not connect: %v", err)
		return err
	}
	defer conn.Close()

	client := fileclient.NewClient(conn, nil)
	err = client.MasterAskToReplicate(context.Background(), non_replica_addr, sdfsfilename, num_version)
	return err
}
