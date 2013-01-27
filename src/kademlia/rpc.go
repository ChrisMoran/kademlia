package kademlia
// Contains definitions mirroring the Kademlia spec. You will need to stick
// strictly to these to be compatible with the reference implementation and
// other groups' code.


// PING
type Ping struct {
    MsgID ID
}

type Pong struct {
    MsgID ID
}

func (k *Kademlia) Ping(ping Ping, pong *Pong) error {
    // This one's a freebie.
    pong.MsgID = CopyID(ping.MsgID)
    return nil
}


// STORE
type StoreRequest struct {
    MsgID ID
    Key ID
    Value []byte
}

type StoreResult struct {
    MsgID ID
    Err error
}



func (k *Kademlia) Store(req StoreRequest, res *StoreResult) error {
	var sliceCopy []byte = make([]byte, len(req.Value))
	copy(sliceCopy, req.Value)
	k.storedDataMutex.Lock()
	k.StoredData[CopyID(req.Key)] = sliceCopy
	k.storedDataMutex.Unlock()
	res.MsgID = CopyID(req.MsgID)
	return nil
}


// FIND_NODE
type FindNodeRequest struct {
    MsgID ID
    NodeID ID
}

type FoundNode struct {
    IPAddr string
    Port uint16
    NodeID ID
}

type FindNodeResult struct {
    MsgID ID
    Nodes []FoundNode
    Err error
}

func (k *Kademlia) FindNode(req FindNodeRequest, res *FindNodeResult) error {
    // TODO: Implement.
    return nil
}


// FIND_VALUE
type FindValueRequest struct {
    MsgID ID
    Key ID
}

// If Value is nil, it should be ignored, and Nodes means the same as in a
// FindNodeResult.
type FindValueResult struct {
    MsgID ID
    Value []byte
    Nodes []FoundNode
    Err error
}

func (k *Kademlia) FindValue(req FindValueRequest, res *FindValueResult) error {
    // TODO: Implement.
    return nil
}

