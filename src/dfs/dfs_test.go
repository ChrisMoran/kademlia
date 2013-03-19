package dfs

import (
	"kademlia"
	"testing"
)

func TestFileSerializeNoData(t *testing.T) {
	f := FileINode{Name: "test", DataNodes: make([]kademlia.ID, 0)}

	b := f.Serialize()
	t.Logf("bytes %x %v", b, b)

	f2, e := DeserializeFile(b)
	if e != nil {
		t.Error("error deserializing f", e)
		t.Fail()
	}

	if f.Name != f2.Name {
		t.Error("Name is not the same f:", f.Name, "f2:", f2.Name)
		t.Fail()
	}

}

func TestFileSerializeWithData(t *testing.T) {
	f := FileINode{Name: "test", DataNodes: make([]kademlia.ID, 0)}

	for i := 0; i < 5; i++ {
		f.DataNodes = append(f.DataNodes, kademlia.NewRandomID())
	}

	b := f.Serialize()

	f2, e := DeserializeFile(b)
	if e != nil {
		t.Error("error deserializing f", e)
		t.Fail()
	}

	if f.Name != f2.Name {
		t.Error("Name is not the same")
		t.Fail()
	}

	for i := 0; i < 5; i++ {
		if f.DataNodes[i].Equals(f2.DataNodes[i]) == false {
			t.Error("Data id's are not the same")
			t.Fail()
		}
	}

}

func TestDirectorySerializeNoData(t *testing.T) {
	f := DirINode{Name: "test", ChildNodes: make([]kademlia.ID, 0)}

	b := f.Serialize()
	t.Logf("bytes %x %v", b, b)

	f2, e := DeserializeDir(b)
	if e != nil {
		t.Error("error deserializing f", e)
		t.Fail()
	}

	if f.Name != f2.Name {
		t.Error("Name is not the same f:", f.Name, "f2:", f2.Name)
		t.Fail()
	}

}

func TestDirectorySerializeWithData(t *testing.T) {
	f := DirINode{Name: "test", ChildNodes: make([]kademlia.ID, 0)}
	for i := 0; i < 5; i++ {
		f.ChildNodes = append(f.ChildNodes, kademlia.NewRandomID())
	}

	b := f.Serialize()
	t.Logf("bytes %x %v", b, b)

	f2, e := DeserializeDir(b)
	if e != nil {
		t.Error("error deserializing f", e)
		t.Fail()
	}

	if f.Name != f2.Name {
		t.Error("Name is not the same f:", f.Name, "f2:", f2.Name)
		t.Fail()
	}

	for i := 0; i < 5; i++ {
		if f.ChildNodes[i].Equals(f2.ChildNodes[i]) == false {
			t.Error("Data id's are not the same")
			t.Fail()
		}
	}
}
