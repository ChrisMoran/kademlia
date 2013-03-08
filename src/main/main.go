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

func contactToAddrString(con kademlia.Contact) string {
	return fmt.Sprintf("%s:%d", con.Host.String(), con.Port)
}

func doPing(kadem *kademlia.Kademlia, con kademlia.Contact, addressOrId string) {
	var pingAddress string
	if len(strings.Split(addressOrId, string(":"))) == 2 {
		pingAddress = addressOrId
	} else {
		id, err := kademlia.FromString(addressOrId)
		if err != nil {
			fmt.Printf("ERR could not interpret nodeid as ID")
			return
		}
		con, err := kadem.ContactFromID(id)
		if err != nil {
			fmt.Println("ERR : unknown node")
			return
		}
		pingAddress = contactToAddrString(con)
	}
	client, err := rpc.DialHTTP("tcp", pingAddress)
	if err != nil {
		log.Fatal("DialHTTP: ", err)
	}
	ping := kademlia.Ping{Sender: con}
	ping.MsgID = kademlia.NewRandomID()
	var pong kademlia.Pong
	err = client.Call("Kademlia.Ping", ping, &pong)
	if err != nil {
		log.Fatal("Call: ", err)
	}

	if ping.MsgID.Equals(pong.MsgID) {
		fmt.Print("OK\n")
	} else {
		fmt.Print("ERR : response message id does not match\n")
	}
	_ = client.Close()
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
	port, err := strconv.Atoi(myIpPort[1])
	if err != nil {
		log.Fatal("Could not parse local port\n")
	}

	rpc.Register(kadem)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", listenStr)
	if err != nil {
		log.Fatal("Listen: ", err)
	}

	// Serve forever.
	go http.Serve(l, nil)

	me := kademlia.Contact{NodeID: kademlia.CopyID(kadem.NodeID), Host: net.ParseIP(myIpPort[0]), Port: uint16(port)}
	if false == strings.Contains(listenStr, firstPeerStr) {
		err = kadem.Join(me, ipAndPort[0], ipAndPort[1])
		if err != nil {
			log.Fatal("Error joinging network", err)
		}
	}

	fmt.Println("Finished starting up")

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
			doPing(kadem, me, command_parts[1])
		case bytes.Equal(command, []byte("store")):
			if len(command_parts) != 4 {
				fmt.Println("Invalid format store\n\tstore nodeid key data")
				continue
			}

			id, err := kademlia.FromString(command_parts[1])
			if err != nil {
				fmt.Printf("ERR could not interpret nodeid as ID")
				return
			}
			con, err := kadem.ContactFromID(id)
			if err != nil {
				fmt.Println("ERR : unknown node")
				continue
			}
			remoteAddr := contactToAddrString(con)
			client, err := rpc.DialHTTP("tcp", remoteAddr)
			if err != nil {
				log.Fatal("DialHTTP: ", err)
			}
			req := kademlia.StoreRequest{MsgID: kademlia.NewRandomID(), Sender: me}
			req.Key, err = kademlia.FromString(command_parts[2])
			if err != nil {
				fmt.Printf("ERR: %v\n", err)
				continue
			}

			req.Value = []byte(command_parts[3])
			res := new(kademlia.StoreResult)
			err = client.Call("Kademlia.Store", req, res)
			if err != nil {
				fmt.Printf("ERR: %v\n", err)
				continue
			}
			_ = client.Close()

			fmt.Println("OK")
		case bytes.Equal(command, []byte("find_node")):
			if len(command_parts) != 3 {
				fmt.Println("Invalid format find_node\n\tfind_node nodeid key")
				continue
			}
			id, err := kademlia.FromString(command_parts[1])
			if err != nil {
				fmt.Printf("ERR could not interpret nodeid as ID")
				continue
			}
			con, err := kadem.ContactFromID(id)
			if err != nil {
				fmt.Println("ERR : unknown node")
				continue
			}
			remoteAddr := contactToAddrString(con)

			client, err := rpc.DialHTTP("tcp", remoteAddr)
			if err != nil {
				log.Fatal("DialHTTP: ", err)
			}
			req := kademlia.FindNodeRequest{MsgID: kademlia.NewRandomID()}
			req.NodeID, err = kademlia.FromString(command_parts[2])
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

			_ = client.Close()

			fmt.Printf("OK\n")
			for _, node := range res.Nodes {
				fmt.Printf("%s\n", node.NodeID.AsString())
			}

		case bytes.Equal(command, []byte("find_value")):
			if len(command_parts) != 3 {
				fmt.Println("Invalid format find_value\n\tfind_value key")
				continue
			}

			id, err := kademlia.FromString(command_parts[1])
			if err != nil {
				fmt.Printf("ERR could not interpret nodeid as ID")
				return
			}

			con, err := kadem.ContactFromID(id)
			if err != nil {
				fmt.Println("ERR : unknown node")
				continue
			}
			remoteAddr := contactToAddrString(con)

			client, err := rpc.DialHTTP("tcp", remoteAddr)
			if err != nil {
				log.Fatal("ERR: DialHTTP -> ", err)
			}
			req := kademlia.FindValueRequest{MsgID: kademlia.NewRandomID()}
			req.Key, err = kademlia.FromString(command_parts[2])
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

			_ = client.Close()

			if res.Value != nil {
				fmt.Printf("%s\n", string(res.Value))
			} else {
				for _, node := range res.Nodes {
					fmt.Printf("%s\n", node.NodeID.AsString())
				}
			}

		case bytes.Equal(command, []byte("whoami")):
			if len(command_parts) != 1 {
				fmt.Println("Invalid format get_node_id\n\twhoami")
				continue
			}
			fmt.Printf("%s\n", kadem.NodeID.AsString())
		case bytes.Equal(command, []byte("local_find_value")):
			if len(command_parts) != 2 {
				fmt.Println("Invalid format get_local_value\n\tlocal_find_value key")
				continue
			}
			key, err := kademlia.FromString(command_parts[1])
			if err != nil {
				fmt.Printf("ERR: %v\n", err)
				continue
			}
			var val kademlia.TimeValue
			val, ok := kadem.StoredData[key]
			if ok {
				fmt.Printf("OK: %s\n", string(val.Data))
			} else {
				fmt.Printf("ERR no data for key\n")
			}
		case bytes.Equal(command, []byte("get_contact")):
			if len(command_parts) != 2 {
				fmt.Println("Invalid format get_contact\n\tget_contact nodeid")
				continue
			}
			key, err := kademlia.FromString(command_parts[1])

			con, err := kadem.ContactFromID(key)
			if err != nil {
				fmt.Println("ERR")
			} else {
				fmt.Printf("%s %d\n", con.Host.String(), con.Port)
			}
		case bytes.Equal(command, []byte("iterativeStore")):
			if len(command_parts) != 3 {
				fmt.Println("Invalid format iterativeStore")
				continue
			}
			req := kademlia.StoreRequest{MsgID: kademlia.NewRandomID(), Sender: me}
			req.Key, err = kademlia.FromString(command_parts[1])
			if err != nil {
				fmt.Printf("ERR: %v\n", err)
				continue
			}

			req.Value = []byte(command_parts[2])
			res := new(kademlia.StoreResult)
			lastNode := kadem.IterStore(req, res)
			if len(lastNode.NodeID) != 0 {
				fmt.Printf("%s", lastNode.NodeID.AsString())
			} else {
				fmt.Println("Could not find a neighbor")
			}
		case bytes.Equal(command, []byte("iterativefindnode")):
			if len(command_parts) != 2 {
				fmt.Println("Invalid format iterativeFindNode")
				continue
			}
			req := kademlia.FindNodeRequest{MsgID: kademlia.NewRandomID(), Sender: me}
			req.NodeID, err = kademlia.FromString(command_parts[1])
			if err != nil {
				fmt.Printf("ERR: %v\n", err)
				continue
			}
			res := new(kademlia.FindNodeResult)
			err := kadem.IterFindNode(req, res)
			if err != nil {
				fmt.Printf("ERR: %v", err)
			} else {
				for _, node := range res.Nodes {
					fmt.Printf("%s", node.NodeID.AsString())
				}
			}
		case bytes.Equal(command, []byte("iterativefindvalue")):
			if len(command_parts) != 2 {
				fmt.Println("Invalid format iterativeFindValue")
				continue
			}
			req := kademlia.FindValueRequest{MsgID: kademlia.NewRandomID(), Sender: me}
			req.Key, err = kademlia.FromString(command_parts[1])
			if err != nil {
				fmt.Printf("ERR: %v\n", err)
				continue
			}
			res := new(kademlia.FindValueResult)
			err := kadem.IterFindValue(req, res)
			if err != nil {
				fmt.Printf("ERR: %v", err)
			} else {
				if res.Value != nil {
					fmt.Printf("%s %s\n", res.Nodes[0].NodeID.AsString(), string(res.Value))
				} else {
					fmt.Println("ERR")
				}
			}
		default:
			fmt.Printf("Unknown command: %s\n", command_parts[0])
		}
	}
}
