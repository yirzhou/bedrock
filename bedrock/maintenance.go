package bedrock

import (
	"log"
	"time"
)

// This loop runs in its own goroutine, launched by NewKVStore.
func (s *KVStore) maintenanceLoop() {
	compactionTicker := time.NewTicker(time.Duration(s.config.MaintenanceIntervalMs) * time.Millisecond)
	defer compactionTicker.Stop()

	for {
		select {
		case <-compactionTicker.C:

			// --- PRIORITY 1: CHECKPOINTING ---
			s.lock.Lock() // Lock to safely check the flag
			isCheckpointNeeded := s.checkpointNeeded
			s.lock.Unlock() // Unlock immediately

			if s.config.EnableCheckpoint && !s.config.EnableSyncCheckpoint && isCheckpointNeeded {
				log.Println("Memtable threshold reached, starting checkpoint...")
				if err := s.doCheckpoint(false); err != nil {
					log.Printf("Error during background checkpoint: %v", err)
				}
				// After a checkpoint, we can skip the compaction check for this cycle.
				continue
			}

			// --- PRIORITY 2: COMPACTION ---
			// (This logic is the same as we designed before)
			if s.config.EnableCompaction {
				if err := s.doCompaction(); err != nil {
					log.Printf("Error during background compaction: %v", err)
				}
			}

		case <-s.shutdownChan:
			return
		}
	}
}
