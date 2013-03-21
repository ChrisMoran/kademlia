package kademlia

// Contains the core kademlia type. In addition to core state, this type serves
// as a receiver for the RPC methods, which is required by that package.

// Core Kademlia type. You can put whatever state you want in this.

import (
	"container/list"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"strings"
	"sync"
	"time"
)

const BucketCount = IDBytes * 8

type BucketList [BucketCount]*list.List

const MaxBucketSize = 10 // aka K
const K = MaxBucketSize
const ALPHA = 3

// how long to wait between checks for stale data, in seconds
const CLEANUP_SECONDS = 10

// how old data must be to be cleaned up, in minutes
const DATA_STALENESS_MIN = 5

type TimeValue struct {
	time time.Time
	Data []byte
}

type Kademlia struct {
	NodeID          ID
	storedDataMutex sync.Mutex
	StoredData      map[ID]TimeValue
	Contacts        BucketList
	contactsMutex   [BucketCount]sync.Mutex
}

func CreateBucketList() (blist BucketList) {
	for i := 0; i < IDBytes*8; i++ {
		blist[i] = list.New()
	}
	return
}

func (k *Kademlia) Start(self Contact, firstContactIp string, firstContactPort string) (error, int) {
	rpc.Register(k)
	rpc.HandleHTTP()

	listenStr := fmt.Sprintf("%s:%d", self.Host.String(), self.Port)

	l, err := net.Listen("tcp", listenStr)
	if err != nil {
		return err, 0
	}

	// Serve forever.
	go http.Serve(l, nil)

	count := 0
	if false == strings.Contains(listenStr, fmt.Sprintf("%s:%s", firstContactIp, firstContactPort)) {

		err, count = k.Join(self, firstContactIp, firstContactPort)
		if err != nil {
			return err, count
		}
	}
	return nil, count
}

func (k *Kademlia) ContactFromID(id ID) (c Contact, e error) {
	prefix := k.NodeID.Xor(id).PrefixLen()
	k.contactsMutex[prefix].Lock()
	defer k.contactsMutex[prefix].Unlock()
	bucket := k.Contacts[prefix]
	for con := bucket.Front(); con != nil; con = con.Next() {
		if con.Value.(Contact).NodeID.Equals(id) {
			return con.Value.(Contact), nil
		}
	}
	e = errors.New("ID is not known")
	return Contact{}, e
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
			client.Close()
			continue
		}
		client.Close()
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

	if pre < BucketCount {
		AddBucketToSlice(k, requester, pre, &nodes)
	}

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

func (k *Kademlia) cleanup() {
	dur := time.Duration(CLEANUP_SECONDS) * time.Second
	time_diff := time.Duration(DATA_STALENESS_MIN) * time.Minute
	var now time.Time
	for {
		time.Sleep(dur)
		k.storedDataMutex.Lock()
		now = time.Now()

		for key, v := range k.StoredData {
			if now.After(v.time.Add(time_diff)) {
				delete(k.StoredData, key)
			}
		}
		k.storedDataMutex.Unlock()
	}
}

func (k *Kademlia) Join(me Contact, ip string, port string) (error, int) {
	// do an rpc call of findnode
	req := FindNodeRequest{Sender: me, MsgID: NewRandomID(), NodeID: k.NodeID}
	res := new(FindNodeResult)

	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%s", ip, port))

	if err != nil {
		return err, 0
	}
	defer client.Close()
	err = client.Call("Kademlia.FindNode", req, res)
	if err != nil {
		return err, 0
	}

	for _, node := range res.Nodes {
		go k.UpdateContacts(FoundNodeToContact(node))
	}
	return nil, len(res.Nodes)
}

func NewKademlia() *Kademlia {
	// TODO: Assign yourself a random ID and prepare other state here.	
	var inst *Kademlia = new(Kademlia)
	inst.NodeID = NewRandomID()
	inst.StoredData = make(map[ID]TimeValue)
	inst.Contacts = CreateBucketList()
	go inst.cleanup()
	return inst
}
