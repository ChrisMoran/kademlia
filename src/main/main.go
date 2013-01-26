package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"time"
	"strings"
	"bytes"
	"os"
	"bufio"
)

import (
    "kademlia"
)


func main() {
	// By default, Go seeds its RNG with 1. This would cause every program to
	// generate the same sequence of IDs.
	rand.Seed(time.Now().UnixNano())
	
	// Get the bind and connect connection strings from command-line arguments.
	flag.Parse()
	args := flag.Args()
	if len(args) != 2 {
		log.Fatal("Must be invoked with exactly two arguments!\n")
	}
	listenStr := args[0]
	firstPeerStr := args[1]

	fmt.Printf("kademlia starting up!\n")
	kadem := kademlia.NewKademlia()

	rpc.Register(kadem)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", listenStr)
	if err != nil {
		log.Fatal("Listen: ", err)
	}

	// Serve forever.
	go http.Serve(l, nil)

    // Confirm our server is up with a PING request and then exit.
    // Your code should loop forever, reading instructions from stdin and
    // printing their results to stdout. See README.txt for more details.
	input := bufio.NewReader(os.Stdin)
	for {
		commandStr, err := input.ReadString('\n')
		commandStr = strings.TrimRight(strings.TrimRight(commandStr, string('\n')), string(' '))
		if err != nil {
			log.Fatal("Reading line:", err) 
		}
		command_parts := strings.Split(commandStr, string(' '))
		if len(commandStr) == 0 || len(command_parts) == 0 {
			continue
		}
		command := []byte(strings.ToLower(command_parts[0]))
		switch {
		case bytes.Equal(command, []byte("ping")):
			if len(command_parts) != 2 {
				fmt.Println("Invalid format ping\n\tping nodeID\n\tping host:port")
				continue
			}
			client, err := rpc.DialHTTP("tcp", firstPeerStr)
			if err != nil {
				log.Fatal("DialHTTP: ", err)
			}
			ping := new(kademlia.Ping)
			ping.MsgID = kademlia.NewRandomID()
			var pong kademlia.Pong
			err = client.Call("Kademlia.Ping", ping, &pong)
			if err != nil {
				log.Fatal("Call: ", err)
			}
			
			log.Printf("ping msgID: %s\n", ping.MsgID.AsString())
			log.Printf("pong msgID: %s\n", pong.MsgID.AsString())

		case bytes.Equal(command, []byte("store")):
			if len(command_parts) != 3 {
				fmt.Println("Invalid format store\n\tstore key data")
				continue
			}
			client, err := rpc.DialHTTP("tcp", firstPeerStr)
			if err != nil {
				log.Fatal("DialHTTP: ", err)
			}
			req := new(kademlia.StoreRequest)
			req.MsgID = kademlia.NewRandomID()
			req.Key, err = kademlia.FromString(command_parts[1])
			if err != nil {
				fmt.Printf("ERR: %v\n", err)
				continue
			}

			req.Value = []byte(command_parts[2])
			var res kademlia.StoreResult
			err = client.Call("Kademlia.Store", req, &res)
			if err != nil {
				fmt.Printf("ERR: %v\n", err)
				continue
			}
			
			fmt.Println("OK")
		case bytes.Equal(command, []byte("find_node")):
			if len(command_parts) != 2 {
				fmt.Println("Invalid format find_node\n\tfind_node key")
				continue
			}
			client, err := rpc.DialHTTP("tcp", firstPeerStr)
			if err != nil {
				log.Fatal("DialHTTP: ", err)
			}
			req := new(kademlia.FindNodeRequest)
			req.MsgID = kademlia.NewRandomID()
			req.NodeID, err = kademlia.FromString(command_parts[1])
			if err != nil {
				fmt.Printf("ERR: %v\n", err)
				continue
			}
			var res kademlia.FindNodeResult
			err = client.Call("Kademlia.FindNode", req, &res)
			if err != nil {
				fmt.Printf("ERR: %v\n", err)
				continue
			}
			
			fmt.Printf("OK\n")
		case bytes.Equal(command, []byte("find_value")):
			if len(command_parts) != 2 {
				fmt.Println("Invalid format find_value\n\tfind_value key")
				continue
			}
			
			client, err := rpc.DialHTTP("tcp", firstPeerStr)
			if err != nil {
				log.Fatal("DialHTTP: ", err)
			}
			req := new(kademlia.FindValueRequest)
			req.MsgID = kademlia.NewRandomID()
			req.Key, err = kademlia.FromString(command_parts[1])
			if err != nil {
				fmt.Printf("ERR: %v\n", err)
				continue
			}
			var res kademlia.FindValueResult
			err = client.Call("Kademlia.FindValue", req, &res)
			if err != nil {
				fmt.Printf("ERR: %v\n", err)
				continue
			}
			
			fmt.Printf("IMPLEMENT ME!\n")
		case bytes.Equal(command, []byte("get_node_id")):
			if len(command_parts) != 1 {
				fmt.Println("Invalid format get_node_id\n\tget_node_id")
				continue
			}
			fmt.Println("Getting local node id")
		case bytes.Equal(command, []byte("get_local_value")):
			if len(command_parts) != 2 {
				fmt.Println("Invalid format get_local_value\n\tget_local_value key")
				continue
			}
			fmt.Printf("Getting local node value %s\n", command_parts[1])
		default:
			fmt.Printf("Unknown command: %s\n", command_parts[0])
		}
	}
}

