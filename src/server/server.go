package main

import (
	"../chord"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"log"
	"net"
	"os"
	"strconv"
)

//Server variables
var hostIP net.IP
var listeningPort string
var hostAddr string
var currentNode chord.NodeID
//var forPredNode chord.NodeID

//Finger Table for the Node
var fingerTable []*chord.NodeID

//-----------------------------------------------------------------//

//Structure to Invoke Function Receivers
type processInpReq struct {
}

//To Store File Metadata, Content
var fileDB = make(map[string]string)
var fileVersionList = make(map[string]int32)

//-----------------------------------------------------------------//

//Write File
func (p processInpReq) WriteFile(ctx context.Context, rFile *chord.RFile) (err error) {

	//Check Write File Content
	if rFile == nil {
		//fmt.Println("WriteFile Error: Content Empty")

		//Exception
		sysException := new(chord.SystemException)
		sysException.Message = new(string)
		*sysException.Message = "WriteFile Error: Content Empty"
		err = sysException
		return

	}

	//Get the Write File Structure ready
	writeFileContent := new(chord.RFile)
	writeFileContent.Meta = new(chord.RFileMetadata)
	writeFileContent.Content = new(string)
	writeFileContent.Meta.Filename = new(string)
	writeFileContent.Meta.Version = new(int32)
	writeFileContent.Meta.ContentHash = new(string)

	//Receive File Name and Content
	writeFileContent.Meta.Filename = rFile.Meta.Filename
	writeFileContent.Content = rFile.Content

	//Generate sha256 ID
	key256 := sha256.New()
	key256.Write([]byte(*writeFileContent.Meta.Filename))
	fileKey256 := hex.EncodeToString(key256.Sum(nil))

	//Get the Node Successor
	nodeSucc, tErr := p.FindSucc(ctx, fileKey256)

	if tErr != nil {
		//fmt.Println("WriteFile Error: Program Exied as FindPred returned FALSE")

		//Exception
		sysException := new(chord.SystemException)
		sysException.Message = new(string)
		*sysException.Message = "WriteFile Error: Program Exied as FindPred returned FALSE"
		err = sysException
		return
	}

	//Check the File Owner
	if nodeSucc.ID != currentNode.ID {
		//fmt.Println("WriteFile Error: Server does not own the file")

		//Exception
		sysException := new(chord.SystemException)
		sysException.Message = new(string)
		*sysException.Message = "WriteFile Error: Server does not own the file"
		err = sysException
		return
	}

	//Check and Add File Version
	fileVersion, isFileExists := fileVersionList[*writeFileContent.Meta.Filename]

	if isFileExists {
		fileVersion++
		*writeFileContent.Meta.Version = fileVersion
		fileVersionList[*writeFileContent.Meta.Filename] = fileVersion
	} else {
		*writeFileContent.Meta.Version = 0
		fileVersionList[*writeFileContent.Meta.Filename] = 0
	}

	//File Hash#
	*writeFileContent.Meta.ContentHash = fileKey256

	//Push the file Content
	fileContent := *rFile.Content
	fileDB[*writeFileContent.Meta.ContentHash] = fileContent

	//fmt.Println("-----------------------------------------------")
	fmt.Println("WriteFile: Success - ", *writeFileContent.Meta.Filename)
	//
	////For verification
	//fmt.Println(fileVersionList)
	//fmt.Println(*writeFileContent.Meta.Filename, *writeFileContent.Meta.Version, *writeFileContent.Meta.ContentHash)
	//fmt.Println(fileDB[*writeFileContent.Meta.ContentHash])
	//fmt.Println("-----------------------------------------------")

	return nil

}

//-----------------------------------------------------------------//

//Read File
func (p processInpReq) ReadFile(ctx context.Context, filename string) (r *chord.RFile, err error) {

	//Check if File name is not Blank
	if filename == " " {
		//fmt.Println("ReadFile Error: File Name Empty")

		//Exception
		sysException := new(chord.SystemException)
		sysException.Message = new(string)
		*sysException.Message = "ReadFile Error: File Name Empty"
		err = sysException
		return
	}

	//Generate sha256 ID
	key256 := sha256.New()
	key256.Write([]byte(filename))
	fileKey256 := hex.EncodeToString(key256.Sum(nil))

	//Get the Node Successor
	nodeSucc, tErr := p.FindSucc(ctx, fileKey256)

	if tErr != nil {
		return
	}

	//Check the Node Successor
	if nodeSucc.ID != currentNode.ID {
		//fmt.Println("ReadFile Error: Server does not own the file")

		//Exception
		sysException := new(chord.SystemException)
		sysException.Message = new(string)
		*sysException.Message = "ReadFile Error: Server does not own the file"
		err = sysException
		return
	}

	//Check file exist in tha node
	fileVersion, isFileExists := fileVersionList[filename]

	if !isFileExists {
		//fmt.Println("ReadFile Error: File not exist")

		//Exception
		sysException := new(chord.SystemException)
		sysException.Message = new(string)
		*sysException.Message = "ReadFile Error: File not exist"
		err = sysException
		return
	}

	//Send Output to Read File
	r = new(chord.RFile)
	r.Meta = new(chord.RFileMetadata)
	r.Content = new(string)
	r.Meta.Filename = new(string)
	r.Meta.ContentHash = new(string)
	r.Meta.Version = new(int32)

	//Push the value to the response
	*r.Meta.Filename = filename
	*r.Meta.Version = fileVersion
	*r.Meta.ContentHash = fileKey256
	*r.Content = fileDB[*r.Meta.ContentHash]

	//For verification
	//fmt.Println("-----------------------------------------------")
	//fmt.Println(*r.Meta.Filename)
	//fmt.Println(*r.Meta.Version)
	//fmt.Println(*r.Content)
	fmt.Println("ReadFile: Success - ", *r.Meta.Filename)
	//fmt.Println("-----------------------------------------------")

	return

}

//-----------------------------------------------------------------//

//Set Finger Table
func (p processInpReq) SetFingertable(ctx context.Context, nodelist []*chord.NodeID) (err error) {

	//Receive Finger Table
	fingerTable = nodelist

	//fmt.Println("SetFingerTable: Success")
	//fmt.Println(fingerTable[0])
	return nil

}

//-----------------------------------------------------------------//

//Find Successor
func (p processInpReq) FindSucc(ctx context.Context, key string) (r *chord.NodeID, err error) {

	//Get the Node Predecessor
	predNode, tErr := p.FindPred(ctx, key)

	if tErr != nil {
		//fmt.Println("FindSucc Error: Exited as FindPred returned FALSE")

		//Exception
		sysException := new(chord.SystemException)
		sysException.Message = new(string)
		*sysException.Message = "FindSucc Error: Exited as FindPred returned FALSE"
		err = sysException

		return

	}

	//Concatenate Path
	predNodePath := predNode.IP + ":" + strconv.Itoa(int(predNode.Port))
	transport, tErr := thrift.NewTSocket(predNodePath)

	if tErr != nil {
		//fmt.Println("FindSucc Error: Unable to connect to", predNodePath)

		//Exception
		sysException := new(chord.SystemException)
		sysException.Message = new(string)
		*sysException.Message = "FindSucc Error: Unable to connect to" + predNodePath
		err = sysException
		return

	}

	defer transport.Close()

	transport.Open()

	//Define Transport and protocol Factory
	transportFactory := thrift.NewTBufferedTransportFactory(1400)
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	//Wrap Transport Factory
	useTransport, err := transportFactory.GetTransport(transport)

	//Set the Client to Invoke the RPC
	client := chord.NewFileStoreClientFactory(useTransport, protocolFactory)

	//fmt.Println("GetSucc: Success")

	//Get Node Successor
	return client.GetNodeSucc(ctx)

}

//-----------------------------------------------------------------//

//Find Predecessor
func (p processInpReq) FindPred(ctx context.Context, key string) (r *chord.NodeID, err error) {

	//If no Finger Table Exist for the current Node
	if fingerTable == nil {
		//fmt.Println("FindPred Error: Finger Table not set for this node")

		//Exception
		sysException := new(chord.SystemException)
		sysException.Message = new(string)
		*sysException.Message = "FindPred Error: Finger Table not set for this node"
		err = sysException

		return nil, err
	}

	//Check if the Key received present in between Current Node & First Entry of Finger Table
	//thisNode := forPredNode
	thisNode := currentNode
	isBetween := IsInBetween(key, thisNode.ID, fingerTable[0].ID)

	for !isBetween {

		for i := len(fingerTable)-1; i > 0; i-- {

			nxtSucc := *fingerTable[i]
			//if IsInBetween(key, thisNode.ID, nxtSucc.ID) {
			if IsInBetween(nxtSucc.ID, thisNode.ID, key) {

				//To Redirect the connection to the Predecessor Node
				//forPredNode = nxtSucc

				//Route the Connection fo the New Node
				return RouteConnection(nxtSucc, key)
			}

		}

		break
	}

	//fmt.Println("FindPred: Success")

	return &thisNode, nil

}

//-----------------------------------------------------------------//

//Get Node Successor
func (p processInpReq) GetNodeSucc(ctx context.Context) (r *chord.NodeID, err error) {

	//If no Finger Table Exist for the current Node
	if fingerTable == nil {
		//fmt.Println("GetNodeSucc Error: Finger Table not set for this node")

		//Exception
		sysException := new(chord.SystemException)
		sysException.Message = new(string)
		*sysException.Message = "GetNodeSucc Error: Finger Table not set for this node"
		err = sysException

		return
	}

	//Return the First Finger Table Entry from the current node
	return fingerTable[0], nil

}

//-----------------------------------------------------------------//

//Function Main
func main() {

	//Get Public IP of host
	hostIP = GetPublicIP()

	//listeningPort := "9090"
	listeningPort = os.Args[1]

	//Key
	var nodeKey string
	nodeKey = hostIP.String() + ":" + listeningPort

	//Current Node ID
	key256 := sha256.New()
	key256.Write([]byte(nodeKey))
	currentNode.ID = hex.EncodeToString(key256.Sum(nil))
	currentNode.IP = hostIP.String()
	portInt, _ := strconv.Atoi(listeningPort)
	currentNode.Port = int32(portInt)

	//forPredNode = currentNode	//To use with FindPred

	//Temporary print
	//fmt.Println("Node Key: ", nodeKey)
	//fmt.Println("Current Node: ", currentNode)

	//Listen to the localhost:port
	//hostAddr := string(hostIP) + ":" + listeningPort
	//hostAddr = "localhost:" + listeningPort
	//serverTransport, err := thrift.NewTServerSocket(hostAddr)
	serverTransport, err := thrift.NewTServerSocket(nodeKey)

	//If error in listening to port, exit
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
	defer serverTransport.Close()

	serverTransport.Open()

	//Setup file store handler
	var handler processInpReq
	processor := chord.NewFileStoreProcessor(handler)
	server := thrift.NewTSimpleServer2(processor, serverTransport)

	if err = server.Listen(); err != nil {
		fmt.Println("Server unable to listen to requests")
		log.Fatal(err)
		return
	}

	fmt.Println("Server listening to the request @ ", hostIP, ":", listeningPort)
	//fmt.Println("Node#: ", currentNode.ID)

	server.AcceptLoop()

	server.Serve()

}

//-----------------------------------------------------------------//

//Retrieve Public IP
func GetPublicIP() net.IP {

	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}

	defer conn.Close()

	publicIP := conn.LocalAddr().(*net.UDPAddr)

	return publicIP.IP
}

//-----------------------------------------------------------------//

//Check if the Key value is Between #Node & Finger Table Entry
func IsInBetween(key string, thisNode string, fTableEntry string) bool {

	//if (thisNode > key) && (fTableEntry > key) {
	//	return true
	//}
	//
	//if (fTableEntry > key) && (thisNode > key) {
	//	return true
	//}
	//
	//return false

	if thisNode < fTableEntry {
		return (thisNode <= key) && (key < fTableEntry)
	}

	return (thisNode <=key) || (key < fTableEntry)

	
	//
	//if (stringcs.Compare(thisNode, key) > 0) && (strings.Compare(fTableEntry, key) > 0){
	//	return true
	//} else if (strings.Compare(fTableEntry, thisNode) > 0){
	//	if (strings.Compare(fTableEntry, key) > 0) && (strings.Compare(thisNode, key) > 0){
	//			return true
	//	}
	//}
	//
	//return false

}

//-----------------------------------------------------------------//

//Route Connection to the Adjacent Node to Search the #Key
func RouteConnection(nxtNode chord.NodeID, key string) (r *chord.NodeID, err error) {

	//Create Context for FindPred
	var nodeCtx context.Context

	//Concatenate Path
	adjNodePath := nxtNode.IP + ":" + strconv.Itoa(int(nxtNode.Port))
	transport, err := thrift.NewTSocket(adjNodePath)

	if err != nil {
		fmt.Println("Unable to connect to ", nxtNode)

	}

	defer transport.Close()

	transport.Open()

	//Define Transport and protocol Factory
	transportFactory := thrift.NewTBufferedTransportFactory(1400)
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	//Wrap Transport Factory
	useTransport, err := transportFactory.GetTransport(transport)

	//Set the Client to Invoke the RPC
	client := chord.NewFileStoreClientFactory(useTransport, protocolFactory)

	//fmt.Println("Route Connection Executed.")

	//Call FindPredecessor with the Key Value
	return client.FindPred(nodeCtx, key)

}

//-----------------------------------------------------------------//
