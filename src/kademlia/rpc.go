package kademlia

// Contains definitions mirroring the Kademlia spec. You will need to stick
// strictly to these to be compatible with the reference implementation and
// other groups' code.

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"sort"
	"time"
)

// Host identification.
type Contact struct {
	NodeID ID
	Host   net.IP
	Port   uint16
}

// PING
type Ping struct {
	Sender Contact
	MsgID  ID
}

type Pong struct {
	MsgID ID
}

func (k *Kademlia) Ping(ping Ping, pong *Pong) error {
	go k.UpdateContacts(ping.Sender)
	pong.MsgID = CopyID(ping.MsgID)
	return nil
}

// STORE
type StoreRequest struct {
	Sender Contact
	MsgID  ID
	Key    ID
	Value  []byte
}

type StoreResult struct {
	MsgID ID
	Err   error
}

func (k *Kademlia) Store(req StoreRequest, res *StoreResult) error {
	go k.UpdateContacts(req.Sender)
	var sliceCopy []byte = make([]byte, len(req.Value))
	copy(sliceCopy, req.Value)
	k.storedDataMutex.Lock()
	k.StoredData[CopyID(req.Key)] = sliceCopy
	k.storedDataMutex.Unlock()
	res.MsgID = CopyID(req.MsgID)
	return nil
}

func contactToAddressString(con Contact) string {
	return fmt.Sprintf("%s:%d", con.Host.String(), con.Port)
}

func foundNodeToAddrStr(node FoundNode) string {
	return fmt.Sprintf("%s:%d", node.IPAddr, node.Port)
}

func makeStoreRequest(node FoundNode, req StoreRequest, res *StoreResult) {
	client, err := rpc.DialHTTP("tcp", foundNodeToAddrStr(node))
	if err != nil {
		res.Err = err
		return
	}

	err = client.Call("Kademlia.Store", req, res)
	if err != nil && res.Err == nil {
		res.Err = err
	}
}

func (k *Kademlia) IterStore(req StoreRequest, res *StoreResult) FoundNode {
	//nodes := k.FindCloseContacts(req.Key, k.NodeID, K)
	res.MsgID = CopyID(req.MsgID)
	fnReq := FindNodeRequest{Sender: req.Sender, MsgID: NewRandomID(), NodeID: CopyID(req.Key)}
	fnRes := new(FindNodeResult)
	k.IterFindNode(fnReq, fnRes)
	var lastNode FoundNode = FoundNode{}
	if len(fnRes.Nodes) > 0 {

		lastNode = fnRes.Nodes[len(fnRes.Nodes)-1]

		if fnRes.Err != nil {
			res.Err = fnRes.Err
		}

		localRes := new(StoreResult)
		for _, node := range fnRes.Nodes {
			makeStoreRequest(node, req, localRes)
			if localRes.Err != nil {
				res.Err = localRes.Err
			}
		}
	}
	return lastNode
}

// FIND_NODE
type FindNodeRequest struct {
	Sender Contact
	MsgID  ID
	NodeID ID
}

type FoundNode struct {
	IPAddr string
	Port   uint16
	NodeID ID
}

type FindNodeResult struct {
	MsgID ID
	Nodes []FoundNode
	Err   error
}

type FindNodeResultWithID struct {
	Res      FindNodeResult
	SourceID ID
}

type foundNodeDistance struct {
	Node      FoundNode
	PrefixLen int
	Queried   bool
}

type nodeDistanceVector struct {
	Closest int
	Nodes   []foundNodeDistance
}

func (ndv nodeDistanceVector) Len() int {
	return len(ndv.Nodes)
}

func (ndv nodeDistanceVector) Less(i, j int) bool {
	return ndv.Nodes[i].PrefixLen > ndv.Nodes[j].PrefixLen
}

func (ndv nodeDistanceVector) Swap(i, j int) {
	temp := ndv.Nodes[i]
	ndv.Nodes[i] = ndv.Nodes[j]
	ndv.Nodes[j] = temp
}

func makeTimeout(b chan bool, seconds int) {
	dur, err := time.ParseDuration(fmt.Sprintf("%d s", seconds))
	if err != nil {
		dur = 1 * time.Second
	}
	time.Sleep(dur)
	b <- true
}

//SPEC: returns up to k triples for the contacts that it knows to be closest to the key
//      should never return a triple with node id of requestor, or its own id
//      primitive operation, not an iterative one
func (k *Kademlia) FindNode(req FindNodeRequest, res *FindNodeResult) error {
	go k.UpdateContacts(req.Sender)
	res.MsgID = CopyID(req.MsgID)
	res.Nodes = k.FindCloseNodes(req.NodeID, req.Sender.NodeID, MaxBucketSize)
	return nil
}

func remoteFindNode(node FoundNode, req FindNodeRequest, res chan FindNodeResultWithID) {
	retRes := new(FindNodeResult)
	defer (func() { res <- FindNodeResultWithID{Res: *retRes, SourceID: CopyID(node.NodeID)} })()
	client, err := rpc.DialHTTP("tcp", foundNodeToAddrStr(node))
	if err != nil {
		retRes.Err = err
		return
	}
	req.MsgID = NewRandomID()
	err = client.Call("Kademlia.FindNode", req, retRes)
	if err != nil && retRes.Err == nil {
		retRes.Err = err
	}
	if false == req.MsgID.Equals(retRes.MsgID) {
		retRes.Err = errors.New("Invalid message id returned")
	}
}

func (k *Kademlia) IterFindNode(req FindNodeRequest, res *FindNodeResult) error {
	// do iterative find node
	nodes := k.FindCloseNodes(req.NodeID, k.NodeID, K)
	res.MsgID = CopyID(req.MsgID)
	nodes = nodes[0:ALPHA]
	ndv := nodeDistanceVector{Nodes: make([]foundNodeDistance, 0, K)}
	for _, node := range nodes {
		ndv.Nodes = append(ndv.Nodes, foundNodeDistance{Node: node,
			PrefixLen: req.MsgID.Xor(node.NodeID).PrefixLen(),
			Queried:   false})
	}
	sort.Sort(ndv) // being lazy
	ndv.Closest = ndv.Nodes[0].PrefixLen
	resChan := make(chan FindNodeResultWithID, ALPHA)
	doneYet := false

	//timeout after 8 seconds
	timeoutChan := make(chan bool, 1)
	go makeTimeout(timeoutChan, 8)

	for doneYet == false {
		queryCount := 0
		for _, node := range ndv.Nodes {
			if node.Queried == false {
				node.Queried, queryCount = true, queryCount+1
				go remoteFindNode(node.Node, req, resChan)
				if queryCount == ALPHA {
					break
				}
			}
		}
		if queryCount == 0 {
			doneYet = true
		}
		exit := false
		for i := 0; i < queryCount; i++ {
			var nodeRes FindNodeResultWithID
			select {
			case <-timeoutChan:
				exit = true
			case nodeRes = <-resChan:
			}
			if exit {
				doneYet = true
				break
			}
			if nodeRes.Res.Err != nil {
				// TODO : remove node from list
				for index, node := range ndv.Nodes {
					if node.Node.NodeID.Equals(nodeRes.SourceID) {
						ndv.Nodes = append(ndv.Nodes[:index], ndv.Nodes[i+1:]...)
						break
					}
				}
				continue
			}

			// incorporate results into list
			// make a temp container to sort all known nodes so we can grab the closest
			tempNdv := new(nodeDistanceVector)
			tempNdv.Nodes = make([]foundNodeDistance, len(ndv.Nodes), 2*K)
			copy(tempNdv.Nodes, ndv.Nodes)

			for _, node := range nodeRes.Res.Nodes {
				go k.UpdateContacts(FoundNodeToContact(node))

				// check if we already know about this node
				known := false
				for _, knownNode := range ndv.Nodes {
					if knownNode.Node.NodeID.Equals(node.NodeID) {
						known = true
					}
				}
				if known == false {
					newNode := foundNodeDistance{Node: node,
						PrefixLen: req.MsgID.Xor(node.NodeID).PrefixLen(),
						Queried:   false}
					tempNdv.Nodes = append(tempNdv.Nodes, newNode)
				}
			}
			sort.Sort(tempNdv)

			endIndex := K
			if len(tempNdv.Nodes) < K {
				endIndex = len(tempNdv.Nodes)
			}
			for index, node := range tempNdv.Nodes[0:endIndex] {
				if index < len(ndv.Nodes) {
					ndv.Nodes[index] = node
				} else {
					ndv.Nodes = append(ndv.Nodes, node)
				}
			}
		}
	}

	res.Nodes = make([]FoundNode, len(ndv.Nodes))
	for _, node := range ndv.Nodes {
		res.Nodes = append(res.Nodes, node.Node)
	}

	return nil
}

// FIND_VALUE
type FindValueRequest struct {
	Sender Contact
	MsgID  ID
	Key    ID
}

// If Value is nil, it should be ignored, and Nodes means the same as in a
// FindNodeResult.
type FindValueResult struct {
	MsgID ID
	Value []byte
	Nodes []FoundNode
	Err   error
}

type FindValueResultWithID struct {
	Res      FindValueResult
	SourceID ID
}

func (f *FindValueResult) SetErr(err error) { f.Err = err }

// SPEC: if corresponding value is present, assocaited data is returned, other acts like FindNode
func (k *Kademlia) FindValue(req FindValueRequest, res *FindValueResult) error {
	go k.UpdateContacts(req.Sender)
	res.MsgID = CopyID(req.MsgID)
	k.storedDataMutex.Lock()
	val, hasKey := k.StoredData[req.Key]
	k.storedDataMutex.Unlock()
	if hasKey {
		res.Value = make([]byte, len(val))
		copy(res.Value, val)
	} else {
		res.Nodes = k.FindCloseNodes(req.Key, req.Sender.NodeID, MaxBucketSize)
	}

	return nil
}

func remoteFindValue(node FoundNode, req FindValueRequest, res chan FindValueResultWithID) {
	retRes := new(FindValueResult)
	defer (func() { res <- FindValueResultWithID{Res: *retRes, SourceID: CopyID(node.NodeID)} })()
	client, err := rpc.DialHTTP("tcp", foundNodeToAddrStr(node))
	if err != nil {
		retRes.Err = err
		return
	}
	req.MsgID = NewRandomID()
	err = client.Call("Kademlia.FindValue", req, retRes)
	if err != nil && retRes.Err == nil {
		retRes.Err = err
	}
	if false == req.MsgID.Equals(retRes.MsgID) {
		retRes.Err = errors.New("Invalid message id returned")
	}
}

// if we find the value, the first foundnode in the result slice is the one that returned it
// spec doesn't say to check if the value is locally available, so we don't
func (k *Kademlia) IterFindValue(req FindValueRequest, res *FindValueResult) error {
	// do iterative find value
	nodes := k.FindCloseNodes(req.Key, k.NodeID, K)
	res.MsgID = CopyID(req.MsgID)
	nodes = nodes[0:ALPHA]
	ndv := nodeDistanceVector{Nodes: make([]foundNodeDistance, 0, K)}
	for _, node := range nodes {
		ndv.Nodes = append(ndv.Nodes, foundNodeDistance{Node: node,
			PrefixLen: req.MsgID.Xor(node.NodeID).PrefixLen(),
			Queried:   false})
	}
	sort.Sort(ndv) // being lazy
	ndv.Closest = ndv.Nodes[0].PrefixLen
	resChan := make(chan FindValueResultWithID, ALPHA)
	doneYet := false

	timeoutChan := make(chan bool, 1)
	go makeTimeout(timeoutChan, 8)

	for doneYet == false {
		queryCount := 0
		for _, node := range ndv.Nodes {
			if node.Queried == false {
				node.Queried, queryCount = true, queryCount+1
				go remoteFindValue(node.Node, req, resChan)
				if queryCount == ALPHA {
					break
				}
			}
		}
		if queryCount == 0 {
			doneYet = true
		}

		exit := false

		for i := 0; i < queryCount; i++ {
			var nodeRes FindValueResultWithID
			select {
			case <-timeoutChan:
				exit = true
			case nodeRes = <-resChan:
			}
			if exit {
				doneYet = true
				break
			}
			if nodeRes.Res.Err != nil {
				// TODO : remove node from list
				for index, node := range ndv.Nodes {
					if node.Node.NodeID.Equals(nodeRes.SourceID) {
						ndv.Nodes = append(ndv.Nodes[:index], ndv.Nodes[i+1:]...)
						break
					}
				}
				continue
			}

			if nodeRes.Res.Value != nil {
				res.Value = make([]byte, len(nodeRes.Res.Value))
				copy(res.Value, nodeRes.Res.Value)
				// these are here just for the command line to return the finder's ID
				res.Nodes = make([]FoundNode, 0, 1)
				res.Nodes = append(res.Nodes, FoundNode{NodeID: CopyID(nodeRes.SourceID)})
				break
			}

			// incorporate results into list
			// make a temp container to sort all known nodes so we can grab the closest
			tempNdv := new(nodeDistanceVector)
			tempNdv.Nodes = make([]foundNodeDistance, len(ndv.Nodes), 2*K)
			copy(tempNdv.Nodes, ndv.Nodes)

			for _, node := range nodeRes.Res.Nodes {
				// check if we already know about this node
				go k.UpdateContacts(FoundNodeToContact(node))

				known := false
				for _, knownNode := range ndv.Nodes {
					if knownNode.Node.NodeID.Equals(node.NodeID) {
						known = true
					}
				}
				if known == false {
					newNode := foundNodeDistance{Node: node,
						PrefixLen: req.MsgID.Xor(node.NodeID).PrefixLen(),
						Queried:   false}
					tempNdv.Nodes = append(tempNdv.Nodes, newNode)
				}
			}
			sort.Sort(tempNdv)

			endIndex := K
			if len(tempNdv.Nodes) < K {
				endIndex = len(tempNdv.Nodes)
			}
			for index, node := range tempNdv.Nodes[0:endIndex] {
				if index < len(ndv.Nodes) {
					ndv.Nodes[index] = node
				} else {
					ndv.Nodes = append(ndv.Nodes, node)
				}
			}
		}
	}

	if res.Value == nil {
		res.Nodes = make([]FoundNode, len(ndv.Nodes))
		for _, node := range ndv.Nodes {
			res.Nodes = append(res.Nodes, node.Node)
		}
	}
	return nil
}
