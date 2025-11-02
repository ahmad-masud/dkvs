package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/ahmad-masud/dkvs/kvstore"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// command represents a log entry for Raft.
type command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
	TTL   int64  `json:"ttl,omitempty"`
}

// kvFSM implements raft.FSM by applying commands to the provided Storage.
type kvFSM struct {
	store kvstore.Storage
}

func (f *kvFSM) Apply(l *raft.Log) interface{} {
	var cmd command
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		logger.WithField("subsys", "raft").WithError(err).Error("failed to unmarshal command")
		return err
	}

	switch cmd.Op {
	case "set":
		if cmd.TTL > 0 {
			f.store.SetWithTTL(cmd.Key, cmd.Value, time.Duration(cmd.TTL)*time.Second)
		} else {
			f.store.Set(cmd.Key, cmd.Value)
		}
		logger.WithFields(map[string]interface{}{"subsys": "raft", "apply": "set", "key": cmd.Key, "ttl": cmd.TTL}).Debug("FSM applied set")
	case "delete":
		f.store.Delete(cmd.Key)
		logger.WithFields(map[string]interface{}{"subsys": "raft", "apply": "delete", "key": cmd.Key}).Debug("FSM applied delete")
	default:
		logger.WithFields(map[string]interface{}{"subsys": "raft", "op": cmd.Op}).Warn("unknown op in FSM")
	}
	return nil
}

func (f *kvFSM) Snapshot() (raft.FSMSnapshot, error) {
	// If the underlying store supports snapshotting, use it.
	if ss, ok := f.store.(kvstore.SnapshotStore); ok {
		b, err := ss.Snapshot()
		if err != nil {
			return nil, err
		}
		logger.WithFields(map[string]interface{}{"subsys": "raft", "bytes": len(b)}).Info("FSM snapshot created")
		return &kvSnapshot{data: b}, nil
	}
	// otherwise return empty snapshot
	logger.WithField("subsys", "raft").Info("FSM snapshot (empty)")
	return &kvSnapshot{data: []byte{}}, nil
}

func (f *kvFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(rc); err != nil {
		return err
	}
	if ss, ok := f.store.(kvstore.SnapshotStore); ok {
		if err := ss.Restore(buf.Bytes()); err != nil {
			logger.WithField("subsys", "raft").WithError(err).Error("FSM restore failed")
			return err
		}
		logger.WithFields(map[string]interface{}{"subsys": "raft", "bytes": buf.Len()}).Info("FSM restore completed")
		return nil
	}
	// nothing to do
	return nil
}

type kvSnapshot struct {
	data []byte
}

func (s *kvSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *kvSnapshot) Release() {}

// initRaft initializes a Raft instance for the server using boltdb for stable/log storage.
func (s *Server) initRaft() error {
	if !s.raftEnabled {
		return nil
	}

	if s.storage == nil {
		s.storage = kvstore.New()
	}

	// ensure data dir exists
	if err := os.MkdirAll(s.raftDir, 0o750); err != nil {
		return fmt.Errorf("create raft dir: %w", err)
	}
	logger.WithFields(map[string]interface{}{"subsys": "raft", "dir": s.raftDir}).Info("raft data dir ready")

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(s.raftID)

	// Setup Raft communication
	addr := s.raftBind
	transport, err := raft.NewTCPTransport(addr, nil, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return fmt.Errorf("create transport: %w", err)
	}
	logger.WithFields(map[string]interface{}{"subsys": "raft", "bind": addr}).Info("raft TCP transport created")

	// Create the snapshot store
	snapDir := filepath.Join(s.raftDir, "snap")
	snaps, err := raft.NewFileSnapshotStore(snapDir, 2, os.Stderr)
	if err != nil {
		return fmt.Errorf("create snapshot store: %w", err)
	}
	logger.WithFields(map[string]interface{}{"subsys": "raft", "snap_dir": snapDir}).Info("raft snapshot store ready")

	// Create the BoltDB-backed log store
	boltPath := filepath.Join(s.raftDir, "raft.db")
	boltStore, err := raftboltdb.NewBoltStore(boltPath)
	if err != nil {
		return fmt.Errorf("create bolt store: %w", err)
	}
	logger.WithFields(map[string]interface{}{"subsys": "raft", "bolt": boltPath}).Info("raft bolt store opened")

	// Use Bolt also as stable store
	stableStore := boltStore
	// keep a reference so we can close it on shutdown to release file handles
	s.raftStore = boltStore

	fsm := &kvFSM{store: s.storage}

	r, err := raft.NewRaft(config, fsm, boltStore, stableStore, snaps, transport)
	if err != nil {
		return fmt.Errorf("new raft: %w", err)
	}
	s.raftInstance = r
	logger.WithFields(map[string]interface{}{"subsys": "raft", "id": s.raftID, "addr": s.raftBind}).Info("raft instance created")

	// Start a background compaction/snapshotter if requested.
	if s.snapshotThreshold > 0 {
		go func() {
			lastSnap := r.LastIndex()
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				last := r.LastIndex()
				if last-lastSnap >= uint64(s.snapshotThreshold) {
					if f := r.Snapshot(); f.Error() != nil {
						logger.WithField("subsys", "raft").WithError(f.Error()).Error("raft snapshot error")
						continue
					}
					lastSnap = last
					logger.WithFields(map[string]interface{}{"subsys": "raft", "index": lastSnap}).Info("snapshot taken")
				}
			}
		}()
	}

	// Bootstrap cluster only if explicitly requested.
	if s.raftBootstrap {
		if len(s.raftPeers) == 0 {
			configuration := raft.Configuration{Servers: []raft.Server{{ID: config.LocalID, Address: transport.LocalAddr()}}}
			future := r.BootstrapCluster(configuration)
			if err := future.Error(); err != nil {
				logger.WithField("subsys", "raft").WithError(err).Warn("raft bootstrap (single) error")
			}
			logger.WithFields(map[string]interface{}{"subsys": "raft", "mode": "single"}).Info("raft bootstrapped")
		} else {
			// Build server list including local and declared peers
			servers := make([]raft.Server, 0, len(s.raftPeers)+1)
			servers = append(servers, raft.Server{ID: config.LocalID, Address: transport.LocalAddr()})
			for i, p := range s.raftPeers {
				servers = append(servers, raft.Server{ID: raft.ServerID(fmt.Sprintf("peer-%d", i)), Address: raft.ServerAddress(p)})
			}
			configuration := raft.Configuration{Servers: servers}
			future := r.BootstrapCluster(configuration)
			if err := future.Error(); err != nil {
				// Bootstrapping can fail if already bootstrapped; log and continue
				logger.WithField("subsys", "raft").WithError(err).Warn("raft bootstrap error")
			}
			logger.WithFields(map[string]interface{}{"subsys": "raft", "mode": "multi", "servers": len(servers)}).Info("raft bootstrapped")
		}
	}

	return nil
}
