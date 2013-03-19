package main

import (
	"bufio"
	"dfs"
	"flag"
	"fmt"
	"io/ioutil"
	"kademlia"
	"log"
	"math/rand"
	"net"
	"os"
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
	dfs := dfs.NewDFS()
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
	me := kademlia.Contact{NodeID: kademlia.CopyID(dfs.Kadem.NodeID), Host: net.ParseIP(myIpPort[0]), Port: uint16(port)}

	dfs.Start(me, ipAndPort[0], ipAndPort[1])

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

		switch {
		case strings.Contains(command_parts[0], "mkdir"):
			if len(command_parts) != 3 {
				log.Println("must supply parent path and new directory name")
				continue
			}
			err := dfs.MakeDirectory(command_parts[1], command_parts[2])
			if err != nil {
				log.Println("error creating directory", err)
			} else {
				log.Println("Created directory", command_parts[2])
			}
		case strings.Contains(command_parts[0], "ls"):
			if len(command_parts) != 2 {
				log.Println("must supply directory path")
				continue
			}
			contents, err := dfs.ListDirectory(command_parts[1])
			if err != nil {
				log.Println("error listing contents of directory", command_parts[1])
				continue
			}
			for _, c := range contents {
				log.Println(c)
			}
		case strings.Contains(command_parts[0], "put"):
			if len(command_parts) != 4 {
				log.Println("must supply parent path, file name, and local file to pull contents from")
				continue
			}
			bytes, err := ioutil.ReadFile(command_parts[3])
			if err != nil {
				log.Println("could not read file contents for", command_parts[3], err)
				continue
			}

			err = dfs.MakeFile(command_parts[1], command_parts[2], bytes)
			if err != nil {
				log.Println("error creating file", command_parts[2], err)
			} else {
				log.Println("created file", command_parts[2])
			}
		case strings.Contains(command_parts[0], "get"):
			if false == (len(command_parts) == 3 || len(command_parts) == 4) {
				log.Print("must supply parent path, file name, optional file to store contents\n")
				continue
			}
			bytes, err := dfs.GetFile(command_parts[1], command_parts[2])

			if err != nil {
				log.Print("error reading file", command_parts[2], err)
				continue
			}
			if len(command_parts) == 3 {
				for _, b := range bytes {
					log.Print(b)
				}
			} else {
				err = ioutil.WriteFile(command_parts[3], bytes, 0644)
				if err != nil {
					log.Println("error writing to file", command_parts[3], err)
				}
			}
		default:
			log.Println("invalid command:", command_parts[0])
			log.Println("known commands are: mkdir ls put get")
		}
	}
}
