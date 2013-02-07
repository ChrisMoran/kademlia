package kademlia

import (
	"bytes"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"testing"
)

func checkMessageId(t *testing.T, expected ID, actual ID) {
	if false == expected.Equals(actual) {
		t.Error("Message IDs to not match!")
		t.Fail()
	}
}

// try to determine host, or just return 127.0.0.1
func getHostIp() net.IP {
	name, err := os.Hostname()
	if err != nil {
		return net.IPv4(byte(127), 0, 0, 1)
	}
	addrs, err := net.LookupAddr(name)
	if err != nil || len(addrs) == 0 {
		return net.IPv4(byte(127), 0, 0, 1)
	}

	return net.ParseIP(addrs[0]) // return the first one
}

var host net.IP = getHostIp()

// make a contact
func makeRandomContact() Contact {
	port := uint16(rand.Int31n(55000) + 10000) // get a port from 10 000 - 65 000
	return Contact{NodeID: NewRandomID(), Host: host, Port: port}
}

func createContacts(size int) []Contact {
	contacts := make([]Contact, size)
	for i := 0; i < size; i++ {
		contacts[i] = makeRandomContact()
	}
	return contacts
}

type contactDistance struct {
	Con  Contact
	Dist int
}

type distanceSlice []contactDistance

func (ds distanceSlice) Len() int {
	return len(ds)
}

func (ds distanceSlice) Less(i, j int) bool {
	return ds[i].Dist < ds[j].Dist
}

func (ds distanceSlice) Swap(i, j int) {
	temp := ds[i]
	ds[i] = ds[j]
	ds[j] = temp
}

func startRpcClosure() func(Contact) error {
	hasRpcStarted := false
	return func(con Contact) error {
		if hasRpcStarted == false {
			kadem := NewKademlia()
			kadem.NodeID = CopyID(con.NodeID)
			rpc.Register(kadem)
			rpc.HandleHTTP()
			hasRpcStarted = true
		}
		addrStr := fmt.Sprintf("%s:%d", con.Host.String(), con.Port)

		l, err := net.Listen("tcp", addrStr)
		if err != nil {
			return err
		}

		// Serve forever.
		go http.Serve(l, nil)
		return nil
	}
}

var startRpcServer = startRpcClosure()

func TestPingWorks(t *testing.T) {
	k := NewKademlia()
	sender, msgId := makeRandomContact(), NewRandomID()
	req := Ping{Sender: sender, MsgID: msgId}
	res := new(Pong)
	k.Ping(req, res)
	checkMessageId(t, msgId, res.MsgID)
}

func TestStoreKey(t *testing.T) {
	k := NewKademlia()
	senderId, messageId := NewRandomID(), NewRandomID()
	key, err := FromString("1234567890123456789012345678901234567890")
	if err != nil {
		t.Error("Could not encode key")
	}
	value := []byte("thisismydata")
	if err != nil {
		t.Error("Cound not decode value string")
	}
	con := Contact{NodeID: senderId, Host: net.IPv4(0x01, 0x02, 0x03, 0x04), Port: 1234}
	req := StoreRequest{Sender: con, MsgID: messageId, Key: key, Value: value}
	res := new(StoreResult)
	err = k.Store(req, res)
	if err != nil {
		t.Error("Failed to store key-value pair")
	}
	checkMessageId(t, messageId, res.MsgID)
	if false == bytes.Equal(k.StoredData[key], value) {
		t.Error("Value stored is incorrect")
	}
}

func TestStoreKeyWithFindValue(t *testing.T) {
	k := NewKademlia()
	senderId, messageId := NewRandomID(), NewRandomID()
	key, err := FromString("1234567890123456789012345678901234567890")
	if err != nil {
		t.Error("Could not encode key")
		t.Fail()
	}
	value := []byte("thisismydata")
	if err != nil {
		t.Error("Cound not decode value string")
		t.Fail()
	}
	con := Contact{NodeID: senderId, Host: net.IPv4(0x01, 0x02, 0x03, 0x04), Port: 1234}
	req := StoreRequest{Sender: con, MsgID: messageId, Key: key, Value: value}
	res := new(StoreResult)
	err = k.Store(req, res)
	if err != nil {
		t.Error("Failed to store key-value pair")
		t.Fail()
	}
	checkMessageId(t, messageId, res.MsgID)

	messageId = NewRandomID()
	findReq := FindValueRequest{Sender: con, MsgID: messageId, Key: key}
	findRes := new(FindValueResult)
	err = k.FindValue(findReq, findRes)
	if err != nil {
		t.Error("Failed to execute find value")
		t.Fail()
	}
	if false == bytes.Equal(findRes.Value, value) {
		t.Error("Retrieved value incorrect")
		t.Fail()
	}
	checkMessageId(t, messageId, findRes.MsgID)
	if len(findRes.Nodes) != 0 {
		t.Error("Returned neighbor nodes without any neighbors! Impossible!")
		t.Fail()
	}
}

func TestFindNodesWithOnlyAFew(t *testing.T) {
	k := NewKademlia()
	contacts := createContacts(MaxBucketSize - 5) // 10 

	for _, con := range contacts {
		k.UpdateContacts(con)
	}

	me, msgId := makeRandomContact(), NewRandomID()
	k.UpdateContacts(me)

	req := FindNodeRequest{Sender: me, MsgID: msgId, NodeID: me.NodeID}
	res := new(FindNodeResult)
	err := k.FindNode(req, res)
	if err != nil {
		t.Error("Error in finding node")
		t.Fail()
	}
	checkMessageId(t, msgId, res.MsgID)

	if len(res.Nodes) != (MaxBucketSize - 5) {
		t.Errorf("Returned %d nodes instead of %d\n", len(res.Nodes), MaxBucketSize)
		t.Fail()
	}

	for _, con := range contacts {
		found := false
		for _, node := range res.Nodes {
			if con.NodeID.Equals(node.NodeID) {
				found = true
				break
			}
		}
		if false == found {
			t.Errorf("Did not find contact %v in result list", con)
			t.Fail()
		}
	}
}

func TestFindNodesWithExactlyLimit(t *testing.T) {
	k := NewKademlia()
	contacts := createContacts(MaxBucketSize) // 10 

	for _, con := range contacts {
		k.UpdateContacts(con)
	}

	me, msgId := makeRandomContact(), NewRandomID()
	k.UpdateContacts(me)

	req := FindNodeRequest{Sender: me, MsgID: msgId, NodeID: me.NodeID}
	res := new(FindNodeResult)
	err := k.FindNode(req, res)
	if err != nil {
		t.Error("Error in finding node")
		t.Fail()
	}
	checkMessageId(t, msgId, res.MsgID)

	if len(res.Nodes) != MaxBucketSize {
		t.Errorf("Returned %d nodes instead of %d\n", len(res.Nodes), MaxBucketSize)
		t.Fail()
	}

	for _, con := range contacts {
		found := false
		for _, node := range res.Nodes {
			if con.NodeID.Equals(node.NodeID) {
				found = true
				break
			}
		}
		if false == found {
			t.Errorf("Did not find contact %v in result list", con)
			t.Fail()
		}
	}
}

func abs(i int) int {
	if i < 0 {
		return -i
	}
	return i
}

func TestFindNodesWithMoreContacts(t *testing.T) {
	k := NewKademlia()
	contacts := createContacts(4 * MaxBucketSize) // 10 

	me, msgId := makeRandomContact(), NewRandomID()
	ds := make(distanceSlice, 4*MaxBucketSize)
	lookupDist := k.NodeID.Xor(me.NodeID).PrefixLen()
	count := 0

	for _, con := range contacts {

		k.UpdateContacts(con)
		ds[count] = contactDistance{Con: con,
			Dist: abs(k.NodeID.Xor(con.NodeID).PrefixLen() - lookupDist)}
		count += 1
		startRpcServer(con)
	}
	//ds = ds[4*MaxBucketSize : len(ds)]
	sort.Sort(ds)

	//find which nodes should be in the result list
	tenDist, cutoff := ds[9].Dist, 10
	for ; cutoff < 4*MaxBucketSize; cutoff += 1 {
		if ds[cutoff].Dist > tenDist {
			break
		}
	}

	ds = ds[0:cutoff]

	req := FindNodeRequest{Sender: me, MsgID: msgId, NodeID: me.NodeID}
	res := new(FindNodeResult)
	err := k.FindNode(req, res)
	if err != nil {
		t.Error("Error in finding node")
		t.Fail()
	}
	checkMessageId(t, msgId, res.MsgID)

	if len(res.Nodes) != MaxBucketSize {
		t.Errorf("Returned %d nodes instead of %d\n", len(res.Nodes), MaxBucketSize)
		t.Fail()
	}

	for _, node := range res.Nodes {
		found := false
		for _, con := range ds {
			if con.Con.NodeID.Equals(node.NodeID) {
				found = true
				break
			}
		}
		if false == found {
			missingDist := k.NodeID.Xor(node.NodeID).PrefixLen()
			t.Errorf("Did not find at dist %d contact %v in result list\n", missingDist, node)
			t.Fail()
		}
	}
}

func TestFindKeyWithoutNodeHavingKey(t *testing.T) {
	k := NewKademlia()
	contacts := createContacts(MaxBucketSize) // 10 

	for _, con := range contacts {
		k.UpdateContacts(con)
	}

	me, msgId := makeRandomContact(), NewRandomID()
	findReq := FindValueRequest{Sender: me, MsgID: msgId, Key: msgId}
	findRes := new(FindValueResult)
	err := k.FindValue(findReq, findRes)
	if err != nil {
		t.Error("Error in finding node")
		t.Fail()
	}
	checkMessageId(t, msgId, findRes.MsgID)

	if findRes.Value != nil {
		t.Error("Returned value from key did not know")
		t.Fail()
	}
	if len(findRes.Nodes) != MaxBucketSize {
		t.Errorf("Returned %d nodes instead of %d\n", len(findRes.Nodes), MaxBucketSize)
		t.Fail()
	}

	for _, con := range contacts {
		found := false
		for _, node := range findRes.Nodes {
			if con.NodeID.Equals(node.NodeID) {
				found = true
				break
			}
		}
		if false == found {
			t.Errorf("Did not find contact %v in result list", con)
			t.Fail()
		}
	}
}
