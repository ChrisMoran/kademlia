package kademlia

// Contains definitions mirroring the Kademlia spec. You will need to stick
// strictly to these to be compatible with the reference implementation and
// other groups' code.

import "net"

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

func (k *Kademlia) IterStore(req StoreRequest, res *StoreResult) error {
	// do iterative store
	return nil
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

//SPEC: returns up to k triples for the contacts that it knows to be closest to the key
//      should never return a triple with node id of requestor, or its own id
//      primitive operation, not an iterative one
func (k *Kademlia) FindNode(req FindNodeRequest, res *FindNodeResult) error {
	go k.UpdateContacts(req.Sender)
	res.MsgID = CopyID(req.MsgID)
	res.Nodes = k.FindCloseContacts(req.NodeID, req.Sender.NodeID, MaxBucketSize)
	return nil
}

func (k *Kademlia) IterFindNode(req FindNodeRequest, res *FindNodeResult) error {
	// do iterative find node
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
		res.Nodes = k.FindCloseContacts(req.Key, req.Sender.NodeID, MaxBucketSize)
	}

	return nil
}

func (k *Kademlia) IterFindValue(req FindValueRequest, res *FindValueResult) error {
	// do iterative find value
	return nil
}
