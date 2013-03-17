package kademlia

import (
	"bytes"
	"encoding/gob"
	"errors"
	"strings"
	"time"
)

// DFS Inode and Content Block
type FileInode struct {
	Meta   MetaData
	Blocks []ID
}

type DirInode struct {
	Meta  MetaData
	Files map[string]ID
}

type FileContent struct {
	Content []byte
	Next    ID
}

type MetaData struct {
	Name         string
	Size         int
	LastRead     time.Time
	LastModified time.Time
	DeleteTime   time.Time
}

// Create File
type CreateFileRequest struct {
	Sender  Contact
	MsgID   ID
	Name    string
	DirKey  ID
	Content []byte
}

type CreateFileResult struct {
	MsgID ID
	Key   ID
	Err   error
}

// create a file by store its content on different nodes
// and insert its id to the inode dir's Nodes
func (k *Kademlia) CreateFile(cfReq CreateFileRequest, cfRes *CreateFileResult) {
	cfRes.MsgID = CopyID(cfReq.MsgID)

	fvReq := FindValueRequest{UpdateTimestamp: true,
		Sender: cfReq.Sender,
		MsgID:  CopyID(cfReq.MsgID),
		Key:    CopyID(cfReq.DirKey)}
	fvRes := new(FindValueResult)
	k.IterFindValue(fvReq, fvRes)
	if fvRes.Err != nil {
		cfRes.Err = fvRes.Err
		return
	}

	if len(fvRes.Nodes) > 0 {
		b = bytes.NewBuffer(fvRes.Value)
		dir := new(DirInode)
		gob.NewDecoder(b).Decode(dir)
		if _, ok := dir.Files[cfReq.Name]; ok {
			cfRes.Err = errors.New("Couldn't create file already exists")
			return
		}

		delReq := DeleteValueRequest{Sender: cfReq.Sender,
			MsgID: CopyID(cfReq.MsgID),
			Key:   CopyID(cfReq.DirKey)}
		delRes := new(DeleteValueResult)
		go k.IterDelete(delReq, delRes)

		// ToDo: cut big content into pieces and have multiple blocks
		fileContent := FileContent{Content: cfReq.Content, Next: nil}
		b := new(bytes.Buffer)
		gob.NewEncoder(b).Encode(fileContent)
		fileBlockValue := b.Bytes()
		fileBlockKey := FromBytes(fileBlockValue)

		storeReq := StoreRequest{Sender: cfReq.Sender,
			MsgID: CopyID(cfReq.MsgID),
			Key:   fileBlockKey,
			Value: fileBlockValue}
		storeRes := new(StoreResult)
		k.IterStore(storeReq, storeRes)
		if storeRes.Err != nil {
			cfRes.Err = storeRes.Err
			return
		}

		fileMeta := MetaData{Name: cfReq.Name,
			Size:         len(cfReq.Content),
			LastRead:     time.Now(),
			LastModified: time.Now(),
			DeleteTime:   nil}
		fileBlockKeys := make([]ID, 1)
		fileBlockKeys[0] = fileBlockKey
		fileInode := FileInode{Meta: fileMeta,
			Blocks: fileBlockKeys}
		b = new(bytes.Buffer)
		gob.NewEncoder(b).Encode(fileInode)
		fileInodeValue := b.Bytes()
		fileInodeKey := FromBytes(fileInodeValue)

		storeReq = StoreRequest{Sender: cfReq.Sender,
			MsgID: CopyID(cfReq.MsgID),
			Key:   fileInodeKey,
			Value: fileInodeValue}
		storeRes = new(StoreResult)
		k.IterStore(storeReq, storeRes)
		if storeRes.Err != nil {
			cfRes.Err = storeRes.Err
			return
		}

		dir.Meta.LastRead = time.Now()
		dir.Meta.LastModified = time.Now()
		dir.Files[cfReq.Name] = fileInodeKey
		b = new(bytes.Buffer)
		gob.NewEncoder(b).Encode(dir)
		dirInodeValue := b.Bytes()
		dirInodeKey := FromBytes(dirInodeValue)
		storeReq = StoreRequest{Sender: cfReq.Sender,
			MsgID: CopyID(cfReq.MsgID),
			Key:   dirInodeKey,
			Value: dirInodeValue}
		storeRes = new(StoreResult)
		k.IterStore(storeReq, storeRes)
		if storeRes.Err != nil {
			cfRes.Err = storeRes.Err
		}
		cfRes.Key = fileInodeKey
	} else {
		cfRes.Err = errors.New("Couldn't find directory inode with the given key")
	}
	return
}

// Create Directory
type CreateDirRequest struct {
	Sender Contact
	MsgID  ID
	Name   string
	DirKey ID
}

type CreateDirResult struct {
	MsgID ID
	Key   ID
	Err   error
}

func (k *Kademlia) CreateDir(cdReq CreateDirRequest, cdRes CreateDirResult) {
	cdRes.MsgID = CopyID(cdReq.MsgID)

	fvReq := FindValueRequest{UpdateTimestamp: true,
		Sender: cdReq.Sender,
		MsgID:  CopyID(cdReq.MsgID),
		Key:    CopyID(cdReq.DirKey)}
	fvRes := new(FindValueResult)
	k.IterFindValue(fvReq, fvRes)
	if fvRes.Err != nil {
		cdRes.Err = fvRes.Err
		return
	}

	if len(fvRes.Nodes) > 0 {
		b = bytes.NewBuffer(fvRes.Value)
		upperDir := new(DirInode)
		gob.NewDecoder(b).Decode(upperDir)
		if _, ok := upperDir.Files[cdReq.Name]; ok {
			cdRes.Err = errors.New("Couldn't create directory already exists")
			return
		}

		delReq := DeleteValueRequest{Sender: cdReq.Sender,
			MsgID: CopyID(cdReq.MsgID),
			Key:   CopyID(cdReq.DirKey)}
		delRes := new(DeleteValueResult)
		go k.IterDelete(delReq, delRes)

		meta := MetaData{Name: cdReq.Name,
			Size:         0,
			LastRead:     time.Now(),
			LastModified: time.Now(),
			DeleteTime:   nil}
		files := make(map[string]ID)
		files[".."] = cdReq.DirKey
		dirInode := DirInode{Meta: meta,
			Files: files}
		b := new(bytes.Buffer)
		gob.NewEncoder(b).Encode(dirInode)
		dirInodeValue := b.Bytes()
		dirInodeKey := FromBytes(dirInodeValue)
		storeReq := StoreRequest{Sender: cdReq.Sender,
			MsgID: CopyID(cdReq.MsgID),
			Key:   dirInodeKey,
			Value: dirInodeValue}
		storeRes := new(StoreResult)
		k.IterStore(storeReq, storeRes)
		if storeRes.Err != nil {
			cdRes.Err = storeRes.Err
			return
		}

		upperDir.Meta.LastRead = time.Now()
		upperDir.Meta.LastModified = time.Now()
		upperDir.Files[cdReq.Name] = dirInodeKey
		b = new(bytes.Buffer)
		gob.NewEncoder(b).Encode(upperDir)
		upperDirInodeValue := b.Bytes()
		upperDirInodeKey := FromBytes(upperDirInodeValue)
		storeReq = StoreRequest{Sender: cdReq.Sender,
			MsgID: CopyID(cdReq.MsgID),
			Key:   upperDirInodeKey,
			Value: upperDirInodeValue}
		storeRes = new(StoreResult)
		k.IterStore(storeReq, storeRes)
		if storeRes.Err != nil {
			cdRes.Err = storeRes.Err
		}
		cdRes.Key = dirInodeKey
	} else {
		cdRes.Err = errors.New("Couldn't find directory inode with the given key")
	}
	return
}

// Find File
type FindFileRequest struct {
	Sender    Contact
	MsgD      ID
	Path      string
	RootInode DirInode
	RootKey   ID
}

type FindFileResult struct {
	MsgID ID
	Inode FileInode
	Key   ID
	Err   error
}

func (k *Kademlia) FindFile(req FindFileRequest, res *FindFileResult) {
	res.MsgID = CopyID(req.MsgID)
	l := strings.Split(req.Path, "/") // Path, e.g. "/1/2/..."
	dirPath := strings.Join(l[:-1], "/")
	fileName := l[-1]

	fdReq := FindDirRequest{Sender: req.Sender,
		MsgID:      CopyID(req.MsgID),
		Path:       dirPath,
		StartInode: req.RootInode,
		StartKey:   req.RootKey}
	fdRes := new(FindDirResult)
	k.FindDir(fdReq, fdRes)
	if fdRes.Err != nil {
		res.Err = fdRes.Err
		return
	}

	if fdRes.Inode != nil {
		upperDir := fdRes.Inode
		if key, ok := upperDir.Files[fileName]; ok {
			res.Key = key
			fvReq := FindValueRequest{UpdateTimestamp: true,
				Sender: req.Sender,
				MsgID:  CopyID(req.MsgID),
				Key:    key}
			fvRes := new(FindValueResult)
			k.IterFindValue(fvReq, fvRes)
			if fvRes.Err != nil {
				res.Err = fvRes.Err
				return
			}

			b := bytes.NewBuffer(fvRes.Value)
			fileInode := new(FileInode)
			gob.NewDecoder(b).Decode(fileInode)
			res.Inode = fileInode
			// do we update LastRead attr in metadata?
		} else {
			res.Err = errors.New("File doesn't exist under the path provided")
		}
	} else {
		res.Err = errors.New("Couldn't find directory with path provided")
	}
	return
}

// Find Directory
type FindDirRequest struct {
	Sender     Contact
	MsgID      ID
	Path       string
	StartInode DirInode
	StartKey   ID
}

type FindDirResult struct {
	MsgID ID
	Inode DirInode
	Key   ID
	Err   error
}

func (k *Kademlia) FindDir(req FindDirRequest, res *FindDirResult) {
	res.MsgID = CopyID(req.MsgID)

	if len(l) == 0 {
		res.Inode = req.StartInode
		res.Key = req.StartKey
		return
	}

	// req.Path, e.g. "/1/2/...", "1/2/..."
	if req.Path[0] == "/" {
		if req.StartKey != nil {
			startInode := req.StartInode
			startKey := req.StartKey
		} else {
			rootRes := new(FindDirResult)
			k.FindRoot(req, rootRes)
			if rootRes.Err != nil {
				res.Err = rootRes.Err
				return
			}
			startInode := rootRes.Inode
			startKey := rooRes.Key
		}

		fdReq := FindDirRequest{Sender: req.Sender,
			MsgID:      CopyID(req.MsgID),
			Path:       req.Path[1:],
			StartInode: startInode,
			StartKey:   startKey}
		k.FindDir(fdReq, res)
	} else {
		dirs := strings.Split(req.Path, "/")
		if key, ok := req.StartInode.Files[dirs[0]]; ok {
			fvReq := FindValueRequest{UpdateTimestamp: true,
				Sender: req.Sender,
				MsgID:  CopyID(req.MsgID),
				Key:    key}
			fvRes := new(FindValueResult)
			k.IterFindValue(fvReq, fvRes)
			if fvRes.Err != nil {
				res.Err = fvRes.Err
				return
			}

			if len(fvRes.Nodes) > 0 {
				b := bytes.NewBuffer(fvRes.Value)
				dirInode := new(DirInode)
				gob.NewDecoder(b).Decode(dirInode)
				if len(dirs) > 1 {
					restPath := strings.Join(dirs[1:], "/")
				} else {
					restPath := ""
				}
				fdReq := FindDirRequest{Sender: req.Sender,
					MsgID:      CopyID(req.MsgID),
					Path:       restPath,
					StartInode: dirInode,
					StartKey:   key}
				k.FindDir(fdReq, res)
			} else {
				res.Err = errors.New("Couldn't find directory inode with key provided")
			}
		} else {
			res.Err = errors.New("The target directory is not under this path")
		}
	}
	return
}

// Note: this is only placeholder
// ToDo: find root directory and return its inode and key in dht
func (k *Kademlia) FindRoot(req FindDirRequest, res *FindDirResult) {
	res.MsgID = CopyID(req.MsgID)
	res.Inode = req.StartInode
	res.Key = req.StartKey
	return
}
