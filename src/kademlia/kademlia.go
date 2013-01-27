package kademlia
// Contains the core kademlia type. In addition to core state, this type serves
// as a receiver for the RPC methods, which is required by that package.

// Core Kademlia type. You can put whatever state you want in this.

import (
	"sync"
	"container/list"
	"net/rpc"
	"fmt"
)

type BucketList [IDBytes*8]*list.List
const MaxBucketSize = 20

type NodeContact struct {
	NodeID ID
	Address string
	Port uint16
}

type Kademlia struct {
	NodeID ID
	storedDataMutex sync.Mutex
	StoredData map[ID][]byte
	Contacts BucketList
}

func CreateBucketList() (blist BucketList) {
	for i := 0; i < IDBytes * 8; i++ {
		blist[i] = list.New()
	}
	return
}

func (k Kademlia) removeOldContacts(bucketNum int) (removed int) {
	removed = 0
	curBucket := k.Contacts[bucketNum]
	var addr string
	for el := curBucket.Front(); el != nil; {
		addr = fmt.Sprintf("%s:%d", el.Value.(NodeContact).Address, el.Value.(NodeContact).Port)
		client, err := rpc.DialHTTP("tcp", addr)
		if err != nil {
			nextEl := el.Next()
			_ = curBucket.Remove(el)
			el, removed = nextEl, removed + 1
			continue
		}
		ping := new(Ping)
		ping.MsgID = NewRandomID()
		var pong Pong
		err = client.Call("Kademlia.Ping", ping, &pong)
		if err != nil {
			nextEl := el.Next()
			_ = curBucket.Remove(el)
			el, removed = nextEl, removed + 1
			continue
		}
		el = el.Next()
	}
	return
}

func (k Kademlia) UpdateContacts(con NodeContact) {
	pre := k.NodeID.Xor(con.NodeID).PrefixLen()
	curBucket := k.Contacts[pre]
	var oldCon *list.Element = nil
	for el := curBucket.Front(); el != nil; el = el.Next() {
		if el.Value.(NodeContact).NodeID.Equals(con.NodeID) {
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

func NewKademlia() *Kademlia {
    // TODO: Assign yourself a random ID and prepare other state here.	
	var inst *Kademlia = new(Kademlia)
	inst.NodeID = NewRandomID()
	inst.StoredData = make(map[ID][]byte)
	inst.Contacts = CreateBucketList()
	return inst
}


