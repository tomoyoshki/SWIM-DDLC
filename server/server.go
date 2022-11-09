package server

import (
	"cs425mp3/fileserver"
	uploadpb "cs425mp3/proto/filetransfer"
	"cs425mp3/storage"
	"cs425mp3/utils"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"
)

func Server(port int, input_channel chan utils.ChannelInMessage, output_channel chan utils.ChannelOutMessage, done chan bool, new_introducer_channel chan string, fileinfo *[]string) {
	log.Printf("Started listening at port: %v", port)
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Print("Server error: ", err)
		return
	}
	defer lis.Close()

	// Bootstrap upload server.
	uplSrv := fileserver.NewServer(storage.New("./targets/"), input_channel, output_channel, new_introducer_channel, fileinfo)

	maxMsgSize := 12 * 1024 * 1024
	// Bootstrap gRPC server.
	rpcSrv := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMsgSize),
		grpc.MaxSendMsgSize(maxMsgSize),
	)

	// Register and start gRPC server.
	log.Print("Registrating Server Service")
	uploadpb.RegisterFileServiceServer(rpcSrv, uplSrv)
	log.Fatal(rpcSrv.Serve(lis))
	log.Print("Deregistrating servers")
	done <- true
}
