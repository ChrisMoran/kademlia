package main

import (
	"flag"
	"fmt"
	"kademlia"
	"log"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"time"
)

// run node as daemon, doesn't read from command line
func main() {

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
	me := kademlia.Contact{NodeID: kademlia.CopyID(kadem.NodeID), Host: net.ParseIP(myIpPort[0]), Port: uint16(port)}

	kadem.Start(me, ipAndPort[0], ipAndPort[1])

	d, err := time.ParseDuration("20s")
	if err != nil {
		log.Fatal("Could not parse duration")
	}
	for {
		time.Sleep(d)
	}
}
