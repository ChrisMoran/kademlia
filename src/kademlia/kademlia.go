package kademlia

// Contains the core kademlia type. In addition to core state, this type serves
// as a receiver for the RPC methods, which is required by that package.

// Core Kademlia type. You can put whatever state you want in this.

import (
	"container/list"
	"fmt"
	"net"
	"net/rpc"
	"sync"
)

const BucketCount = IDBytes * 8

type BucketList [BucketCount]*list.List

const MaxBucketSize = 10 // aka K
const K = MaxBucketSize
const ALPHA = 3

type Kademlia struct {
	NodeID          ID
	storedDataMutex sync.Mutex
	StoredData      map[ID][]byte
	Contacts        BucketList
	contactsMutex   [BucketCount]sync.Mutex
}

func CreateBucketList() (blist BucketList) {
	for i := 0; i < IDBytes*8; i++ {
		blist[i] = list.New()
	}
	return
}

func (k *Kademlia) removeOldContacts(bucketNum int) (removed int) {
	removed = 0
	curBucket := k.Contacts[bucketNum]
	var addr string
	for el := curBucket.Front(); el != nil; {
		addr = fmt.Sprintf("%s:%d", el.Value.(Contact).Host.String(), el.Value.(Contact).Port)
		client, err := rpc.DialHTTP("tcp", addr)
		if err != nil {
			nextEl := el.Next()
			_ = curBucket.Remove(el)
			el, removed = nextEl, removed+1
			continue
		}
		ping := new(Ping)
		ping.MsgID = NewRandomID()
		var pong Pong
		err = client.Call("Kademlia.Ping", ping, &pong)
		if err != nil {
			nextEl := el.Next()
			_ = curBucket.Remove(el)
			el, removed = nextEl, removed+1
			continue
		}
		el = el.Next()
	}
	return
}

func (k *Kademlia) UpdateContacts(con Contact) {
	pre := k.NodeID.Xor(con.NodeID).PrefixLen()
	k.contactsMutex[pre].Lock()
	defer k.contactsMutex[pre].Unlock()
	curBucket := k.Contacts[pre]
	var oldCon *list.Element = nil
	for el := curBucket.Front(); el != nil; el = el.Next() {
		if el.Value.(Contact).NodeID.Equals(con.NodeID) {
			oldCon = el
			break
		}
	}

	if oldCon != nil {
		curBucket.MoveToFront(oldCon)
	} else {
		if curBucket.Len() <= MaxBucketSize {
			curBucket.PushFront(con)
		} else {
			rem := k.removeOldContacts(pre)
			if rem > 0 {
				curBucket.PushFront(con)
			}
		}
	}
}

func ContactToFoundNode(con Contact) FoundNode {
	return FoundNode{IPAddr: con.Host.String(), Port: con.Port, NodeID: CopyID(con.NodeID)}
}

func FoundNodeToContact(node FoundNode) Contact {
	return Contact{NodeID: CopyID(node.NodeID), Port: node.Port, Host: net.ParseIP(node.IPAddr)}
}

// assumes bucket is already locked, slice has proper capacity
func AddBucketContentsToSlice(bucket *list.List, requester ID, s *[]Contact) {
	var maxToAdd, count int = cap(*s) - len(*s), 0
	for con := bucket.Front(); con != nil && count < maxToAdd; con = con.Next() {
		if false == con.Value.(Contact).NodeID.Equals(requester) {
			*s = append(*s, con.Value.(Contact))
			count += 1
		}
	}
}

func AddBucketToSlice(k *Kademlia, requester ID, bucketNum int, s *[]Contact) {
	k.contactsMutex[bucketNum].Lock()
	AddBucketContentsToSlice(k.Contacts[bucketNum], requester, s)
	k.contactsMutex[bucketNum].Unlock()
}

func (k *Kademlia) FindCloseNodes(key ID, requester ID, totalNum int) []FoundNode {
	contacts := k.FindCloseContacts(key, requester, totalNum)
	nodes := make([]FoundNode, len(contacts), cap(contacts))

	for index, con := range contacts {
		nodes[index] = ContactToFoundNode(con)
	}
	return nodes
}

func (k *Kademlia) FindCloseContacts(key ID, requester ID, totalNum int) []Contact {
	pre := k.NodeID.Xor(key).PrefixLen()
	nodes := make([]Contact, 0, totalNum)

	AddBucketToSlice(k, requester, pre, &nodes)

	for i := 1; (i <= pre || i+pre < BucketCount) && len(nodes) < totalNum; i++ {
		if i <= pre {
			AddBucketToSlice(k, requester, pre-i, &nodes)
		}

		if i+pre < BucketCount {
			AddBucketToSlice(k, requester, i+pre, &nodes)
		}
	}

	// this shouldn't happen
	if len(nodes) > totalNum {
		nodes = nodes[0:totalNum]
	}
	return nodes
}

func (k *Kademlia) Join(ip string, port string) {
	// we don't know the node id of the first contact...
}

func NewKademlia() *Kademlia {
	// TODO: Assign yourself a random ID and prepare other state here.	
	var inst *Kademlia = new(Kademlia)
	inst.NodeID = NewRandomID()
	inst.StoredData = make(map[ID][]byte)
	inst.Contacts = CreateBucketList()
	return inst
}
