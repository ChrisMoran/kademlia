package kademlia
// Contains the core kademlia type. In addition to core state, this type serves
// as a receiver for the RPC methods, which is required by that package.

// Core Kademlia type. You can put whatever state you want in this.
type Kademlia struct {
	NodeID ID
}

func NewKademlia() *Kademlia {
    // TODO: Assign yourself a random ID and prepare other state here.	
	var inst &Kademlia = new(Kademlia)
	inst.ID = NewRandomID()
	
	return inst
}


