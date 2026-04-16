package node

import (
	"context"
	"maps"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nikitakosatka/hive/pkg/hive"
)

const (
	retransmitInterval        = 50 * time.Millisecond
	randomDelayIntervalMillis = 10
	gossipCount               = 3
)

// Version is a logical LWW version for one key.
// Ordering is lexicographic: (Counter, NodeID).
type Version struct {
	Counter uint64
	NodeID  string
}

// StateEntry stores one OR-Map key state.
type StateEntry struct {
	Value     string
	Tombstone bool
	Version   Version
}

// MapState is an exported snapshot representation used by Merge.
type MapState map[string]StateEntry

// CRDTMapNode is a state-based OR-Map with LWW values.
type CRDTMapNode struct {
	*hive.BaseNode

	mu         sync.RWMutex
	counter    atomic.Uint64
	state      MapState
	allNodeIDs []string
}

// NewCRDTMapNode creates a CRDT map node for the provided peer set.
func NewCRDTMapNode(id string, allNodeIDs []string) *CRDTMapNode {
	n := &CRDTMapNode{
		BaseNode:   hive.NewBaseNode(id),
		state:      make(MapState),
		allNodeIDs: allNodeIDs,
	}

	return n
}

// Start starts message processing and anti-entropy broadcast (flood/gossip).
func (n *CRDTMapNode) Start(ctx context.Context) error {
	// init n.Context()
	if err := n.BaseNode.Start(ctx); err != nil {
		return err
	}

	go n.doSync()

	return nil
}

func (n *CRDTMapNode) doSync() {
	ticker := time.NewTicker(retransmitInterval)
	defer ticker.Stop()

	for {
		t := rand.Intn(randomDelayIntervalMillis)
		time.Sleep(time.Duration(t) * time.Millisecond)

		curState := n.State()
		nodesCount := min(gossipCount, len(n.allNodeIDs))

		for range nodesCount {
			toNodeIdx := rand.Intn(len(n.allNodeIDs))

			if n.allNodeIDs[toNodeIdx] == n.ID() {
				toNodeIdx = (toNodeIdx + 1) % len(n.allNodeIDs)
			}

			n.Send(n.allNodeIDs[toNodeIdx], curState)
		}

		select {
		case <-ticker.C:
			continue
		case <-n.Context().Done():
			return
		}
	}
}

// Put writes a value with a fresh local version.
func (n *CRDTMapNode) Put(k, original string) {
	ctr := n.counter.Add(1)

	n.mu.Lock()
	defer n.mu.Unlock()

	n.state[k] = StateEntry{
		Value:     original,
		Tombstone: false,
		Version: Version{
			Counter: ctr,
			NodeID:  n.ID(),
		},
	}
}

// Get returns the current visible value for key k.
func (n *CRDTMapNode) Get(k string) (string, bool) {
	n.counter.Add(1)

	n.mu.RLock()
	defer n.mu.RUnlock()

	if v, ok := n.state[k]; ok && !v.Tombstone {
		return v.Value, true
	}
	return "", false
}

// Delete marks the key as removed via a tombstone.
func (n *CRDTMapNode) Delete(k string) {
	ctr := n.counter.Add(1)

	n.mu.Lock()
	defer n.mu.Unlock()

	if v, ok := n.state[k]; ok && !v.Tombstone {
		v.Tombstone = true
		v.Version.Counter = ctr
		v.Version.NodeID = n.ID()

		n.state[k] = v
	}
}

// Merge joins local state with a remote state snapshot.
func (n *CRDTMapNode) Merge(remote MapState) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for k, their := range remote {
		if mine, ok := n.state[k]; ok {
			theirIsBetter := (their.Version.Counter > mine.Version.Counter) ||
				(their.Version.Counter == mine.Version.Counter && their.Version.NodeID > mine.Version.NodeID)

			if theirIsBetter {
				n.state[k] = their
			}
		} else {
			n.state[k] = their
		}
	}

}

// State returns a copy of the full CRDT state.
func (n *CRDTMapNode) State() MapState {
	n.mu.RLock()
	defer n.mu.RUnlock()

	clone := maps.Clone(n.state)

	return clone
}

// ToMap returns a value-only map view without tombstones.
func (n *CRDTMapNode) ToMap() map[string]string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	res := make(map[string]string)

	for k, v := range n.state {
		if !v.Tombstone {
			res[k] = v.Value
		}
	}

	return res
}

// Receive applies remote state snapshots.
func (n *CRDTMapNode) Receive(msg *hive.Message) error {
	n.counter.Add(1)

	switch payload := msg.Payload.(type) {
	case MapState:
		n.Merge(payload)
	default:
		// ignore
	}

	return nil
}
