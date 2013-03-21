package dfs

import (
	"bytes"
	"encoding/gob"
	"errors"
	"kademlia"
	"strings"
)

const FILE_INODE_TYPE = uint8(0x00)
const DIR_INODE_TYPE = uint8(0x01)
const FILE_CHUNK_SIZE = 4096

type DFS struct {
	Kadem  *kademlia.Kademlia
	Self   kademlia.Contact
	RootID kademlia.ID
}

type FileINode struct {
	Name      string
	DataNodes []kademlia.ID
}

type DirINode struct {
	Name       string
	ChildNodes []kademlia.ID
}

func NewDFS() *DFS {
	dfs := new(DFS)
	dfs.Kadem = kademlia.NewKademlia()
	return dfs
}

func (d *DFS) find(key kademlia.ID, update bool) ([]byte, error) {
	req := kademlia.FindValueRequest{
		UpdateTimestamp: update,
		Sender:          d.Self,
		MsgID:           kademlia.NewRandomID(),
		Key:             key,
	}

	res := new(kademlia.FindValueResult)
	err := d.Kadem.IterFindValue(req, res)

	if err != nil {
		return nil, err
	} else if res.Err != nil {
		return nil, res.Err
	} else if len(res.Value) == 0 {
		return nil, errors.New("Could not find value")
	}
	return res.Value, nil
}

func (d *DFS) store(key kademlia.ID, value []byte) error {
	req := kademlia.StoreRequest{
		Sender: d.Self,
		MsgID:  kademlia.NewRandomID(),
		Key:    key,
		Value:  value,
	}

	res := new(kademlia.StoreResult)

	storeTries, num := 3, 0

	for num == 0 && storeTries > 0 {
		_, num = d.Kadem.IterStore(req, res)
		storeTries -= 1
	}

	if num == 0 {
		return errors.New("did not store value on any neighbors")
	}
	return nil
}

func isFileINode(b []byte) bool {
	return uint8(b[0]) == FILE_INODE_TYPE
}

func isDirINode(b []byte) bool {
	return uint8(b[0]) == DIR_INODE_TYPE
}

func (f FileINode) Serialize() []byte {
	buf := new(bytes.Buffer)

	enc := gob.NewEncoder(buf)
	_ = enc.Encode(f)
	return append([]byte{FILE_INODE_TYPE}, buf.Bytes()...)
}

func DeserializeFile(b []byte) (*FileINode, error) {
	var f *FileINode
	if isFileINode(b) {
		buf := bytes.NewBuffer(b[1:])

		dec := gob.NewDecoder(buf)

		f = new(FileINode)
		f.DataNodes = make([]kademlia.ID, 0)
		dec.Decode(f)
	} else {
		return nil, errors.New("byte slice is not a file inode")
	}
	return f, nil
}

func (d DirINode) Serialize() []byte {
	buf := new(bytes.Buffer)

	enc := gob.NewEncoder(buf)
	_ = enc.Encode(d)

	return append([]byte{DIR_INODE_TYPE}, buf.Bytes()...)
}

func DeserializeDir(b []byte) (*DirINode, error) {
	var d *DirINode
	if isDirINode(b) {
		buf := bytes.NewBuffer(b[1:])

		dec := gob.NewDecoder(buf)

		d = new(DirINode)
		d.ChildNodes = make([]kademlia.ID, 0)
		dec.Decode(d)
	} else {
		return nil, errors.New("byte slice is not a dir inode")
	}
	return d, nil
}

func (d *DFS) Start(self kademlia.Contact, firstContactIp string, firstContactPort string) error {
	neighborCount, tries := 0, 3

	var err error
	for neighborCount == 0 && tries > 0 {
		err, neighborCount = d.Kadem.Start(self, firstContactIp, firstContactPort)
		tries -= 1
		if err != nil {
			return err
		}
	}

	if neighborCount == 0 {
		return errors.New("Could not establish any neighbor nodes")
	}

	d.Self = kademlia.Contact{NodeID: kademlia.CopyID(self.NodeID), Host: self.Host, Port: self.Port}
	d.RootID = kademlia.NewRandomID()

	root := DirINode{Name: "/", ChildNodes: make([]kademlia.ID, 0)}
	root_bytes := root.Serialize()

	err = d.store(d.RootID, root_bytes)
	return err
}

func (d *DFS) traverseDirectories(path string) (kademlia.ID, *DirINode, error) {
	curr_id := d.RootID

	curr_inode := new(DirINode)
	var temp_inode *DirINode

	data, err := d.find(curr_id, true)
	if err != nil {
		return curr_id, curr_inode, err
	}
	curr_inode, err = DeserializeDir(data)

	if err != nil {
		return curr_id, curr_inode, err
	}

	paths := strings.Split(path, "/")
	found_next := false

	for _, p := range paths {
		if len(p) > 0 { // not empty string
			found_next = false
			for _, i := range curr_inode.ChildNodes {
				data, err := d.find(i, false)
				if err != nil {
					return curr_id, curr_inode, err
				}
				if isDirINode(data) {
					temp_inode, _ = DeserializeDir(data)
					if p == temp_inode.Name {
						go d.find(i, true) // gross way of updating directory when traversing
						found_next = true
						curr_inode.Name = temp_inode.Name
						curr_inode.ChildNodes = make([]kademlia.ID, len(temp_inode.ChildNodes))
						copy(curr_inode.ChildNodes, temp_inode.ChildNodes)
						curr_id = i
						break
					}
				}
			}

			if found_next == false {
				return curr_id, curr_inode, errors.New("Could not find next path")
			}
		}
	}

	return curr_id, curr_inode, nil
}

func (d *DFS) MakeDirectory(parentPath string, dirName string) error {
	parent_id, parent_node, err := d.traverseDirectories(parentPath)
	if err != nil {
		return err
	}

	new_dir_id := kademlia.NewRandomID()
	new_dir := DirINode{Name: dirName, ChildNodes: make([]kademlia.ID, 0)}

	parent_node.ChildNodes = append(parent_node.ChildNodes, new_dir_id)

	err = d.store(new_dir_id, new_dir.Serialize())
	if err != nil {
		return err
	}

	err = d.store(parent_id, (*parent_node).Serialize())
	return err
}

func (d *DFS) ListDirectory(parentPath string) ([]string, error) {
	_, parent_node, err := d.traverseDirectories(parentPath)
	if err != nil {
		return make([]string, 0), err
	}

	ret := make([]string, len(parent_node.ChildNodes))
	f := new(FileINode)
	dn := new(DirINode)
	for _, p := range parent_node.ChildNodes {
		b, e := d.find(p, false)
		if e == nil {
			if isDirINode(b) {
				dn, _ = DeserializeDir(b)
				ret = append(ret, dn.Name)
			} else if isFileINode(b) {
				f, _ = DeserializeFile(b)
				ret = append(ret, strings.Join([]string{"file", f.Name}, " "))
			} else {
				ret = append(ret, "UNKNOWN!!!") // do something better
			}
		} else {
			ret = append(ret, "ERROR fetching value")
		}
	}
	return ret, nil
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func (d *DFS) storeFileContents(contents []byte) ([]kademlia.ID, error) {
	content_len := len(contents)
	chunk_count := int(content_len / FILE_CHUNK_SIZE)
	if (content_len % FILE_CHUNK_SIZE) != 0 {
		chunk_count += 1
	}
	start, end := 0, min(FILE_CHUNK_SIZE, content_len)

	ret := make([]kademlia.ID, 0)
	for i := 0; i < chunk_count; i++ {
		id := kademlia.NewRandomID()
		ret = append(ret, id)
		err := d.store(id, contents[start:end])
		if err != nil {
			return ret, err
		}
		start, end = end, min(end+FILE_CHUNK_SIZE, content_len)
	}
	return ret, nil
}

func (d *DFS) MakeFile(parentPath string, fileName string, contents []byte) error {
	parent_id, parent_node, err := d.traverseDirectories(parentPath)
	if err != nil {
		return err
	}

	file_ids, err := d.storeFileContents(contents)
	if err != nil {
		return err
	}

	f_id := kademlia.NewRandomID()
	f := FileINode{Name: fileName, DataNodes: make([]kademlia.ID, len(file_ids))}
	copy(f.DataNodes, file_ids)

	err = d.store(f_id, f.Serialize())
	if err != nil {
		return err
	}

	parent_node.ChildNodes = append(parent_node.ChildNodes, f_id)

	return d.store(parent_id, parent_node.Serialize())
}

func (d *DFS) getFileContents(fileINode *FileINode) ([]byte, error) {
	ret := make([]byte, 0)

	for _, id := range fileINode.DataNodes {
		b, e := d.find(id, true)
		if e != nil {
			return ret, e
		}
		ret = append(ret, b...)
	}
	return ret, nil
}

func (d *DFS) GetFile(parentPath string, fileName string) ([]byte, error) {
	_, parent_node, err := d.traverseDirectories(parentPath)
	if err != nil {
		return make([]byte, 0), err
	}

	f := new(FileINode)
	for _, id := range parent_node.ChildNodes {
		b, e := d.find(id, false)
		if e != nil {
			return make([]byte, 0), e
		}
		if isFileINode(b) {
			f, _ = DeserializeFile(b)
			if f.Name == fileName {
				go d.find(id, true) // hack to update file inode
				return d.getFileContents(f)
			}
		}
	}

	return make([]byte, 0), errors.New("Could not find file")
}
