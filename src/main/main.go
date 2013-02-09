package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"kademlia"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

func doPing(addressOrId string) {
	var pingAddress string
	if len(strings.Split(addressOrId, string(":"))) == 2 {
		pingAddress = addressOrId
	} else {
		log.Fatal("Implement ping for node id")
	}
	client, err := rpc.DialHTTP("tcp", pingAddress)
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

	if ping.MsgID.Equals(pong.MsgID) {
		fmt.Print("OK\n")
	} else {
		fmt.Print("ERR\n")
	}
}

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
	myIpPort := strings.Split(listenStr, ":")
	if len(myIpPort) != 2 {
		log.Fatal("Invalid format of arg one, expected IP:PORT\n")
	}
	if strings.Contains(myIpPort[0], "localhost") {
		myIpPort[0] = "127.0.0.1"
	}

	ipAndPort := strings.Split(firstPeerStr, ":")
	if len(ipAndPort) != 2 {
		log.Fatal("Invalid format of arg two, expected IP:PORT\n")
	}

	if false == strings.Contains(listenStr, firstPeerStr) {
		port, err := strconv.Atoi(myIpPort[1])
		if err != nil {
			log.Fatal("Could not parse local port\n")
		}
		me := kademlia.Contact{NodeID: kademlia.CopyID(kadem.NodeID), Host: net.ParseIP(myIpPort[0]), Port: uint16(port)}
		err = kadem.Join(me, ipAndPort[0], ipAndPort[1])
		if err != nil {
			log.Fatal("Error joinging network", err)
		}
	}

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
			doPing(command_parts[1])
		case bytes.Equal(command, []byte("store")):
			if len(command_parts) != 3 {
				fmt.Println("Invalid format store\n\tstore key data")
				continue
			}
			client, err := rpc.DialHTTP("tcp", firstPeerStr)
			if err != nil {
				log.Fatal("DialHTTP: ", err)
			}
			req := kademlia.StoreRequest{MsgID: kademlia.NewRandomID()}
			req.Key, err = kademlia.FromString(command_parts[1])
			if err != nil {
				fmt.Printf("ERR: %v\n", err)
				continue
			}

			req.Value = []byte(command_parts[2])
			res := new(kademlia.StoreResult)
			err = client.Call("Kademlia.Store", req, res)
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
			req := kademlia.FindNodeRequest{MsgID: kademlia.NewRandomID()}
			req.NodeID, err = kademlia.FromString(command_parts[1])
			if err != nil {
				fmt.Printf("ERR: %v\n", err)
				continue
			}
			res := new(kademlia.FindNodeResult)
			err = client.Call("Kademlia.FindNode", req, res)
			if err != nil {
				fmt.Printf("ERR: %v\n", err)
				continue
			}

			fmt.Printf("OK\n")
			fmt.Print("Nodes:")
			for _, node := range res.Nodes {
				fmt.Printf("%s %v %v\n", node.NodeID.AsString(), node.IPAddr, node.Port)
			}

		case bytes.Equal(command, []byte("find_value")):
			if len(command_parts) != 2 {
				fmt.Println("Invalid format find_value\n\tfind_value key")
				continue
			}

			client, err := rpc.DialHTTP("tcp", firstPeerStr)
			if err != nil {
				log.Fatal("DialHTTP: ", err)
			}
			req := kademlia.FindValueRequest{MsgID: kademlia.NewRandomID()}
			req.Key, err = kademlia.FromString(command_parts[1])
			if err != nil {
				fmt.Printf("ERR: %v\n", err)
				continue
			}
			res := new(kademlia.FindValueResult)
			err = client.Call("Kademlia.FindValue", req, res)
			if err != nil {
				fmt.Printf("ERR: %v\n", err)
				continue
			}

			if res.Value != nil {
				fmt.Printf("Found Value\n%s\n", string(res.Value))
			} else {
				fmt.Print("Found nodes")
				for _, node := range res.Nodes {
					fmt.Printf("%s %v %v\n", node.NodeID.AsString(), node.IPAddr, node.Port)
				}
			}

		case bytes.Equal(command, []byte("get_node_id")):
			if len(command_parts) != 1 {
				fmt.Println("Invalid format get_node_id\n\tget_node_id")
				continue
			}
			fmt.Printf("OK: %s\n", kadem.NodeID.AsString())
		case bytes.Equal(command, []byte("get_local_value")):
			if len(command_parts) != 2 {
				fmt.Println("Invalid format get_local_value\n\tget_local_value key")
				continue
			}
			key, err := kademlia.FromString(command_parts[1])
			if err != nil {
				fmt.Printf("ERR: %v\n", err)
				continue
			}
			var val []byte
			val = kadem.StoredData[key]
			if val != nil {
				fmt.Printf("OK: %s\n", string(val))
			} else {
				fmt.Printf("ERR no data for key\n")
			}
		default:
			fmt.Printf("Unknown command: %s\n", command_parts[0])
		}
	}
}
