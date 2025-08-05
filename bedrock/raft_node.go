package bedrock

import (
	"log"
	"math/rand"
	"time"
)

// With this new structure, the flow of a Put request changes completely:

// A client sends a Put request to a RaftNode.

// If the node is not the leader, it rejects the request and redirects the client.

// If it is the leader, it takes the Put command and appends it to its own log (our WAL).

// It then sends this new log entry to all its peers.

// Once a majority of peers acknowledge the entry, the leader advances its commitIndex.

// It then applies the command to its own local kvStore.

// Finally, it returns success to the client.

// RaftNode represents a single server in the Raft cluster.
type (
	RaftNode struct {
		// --- Persistent Raft State (must be saved to disk) ---
		currentTerm uint64
		votedFor    string // The candidateId that this node voted for in the current term
		log         *WAL   // Our existing WAL is the perfect replicated log!

		clientRequestChan chan ClientRequest
		rpcChan           chan RPC

		// --- Volatile Raft State ---
		commitIndex uint64
		lastApplied uint64

		// --- Leader-Specific Volatile State ---
		nextIndex  map[string]uint64 // Maps peer ID to the next log index to send
		matchIndex map[string]uint64 // Maps peer ID to the highest log index known to be replicated

		// --- The State Machine ---
		// Our KVStore is the state machine that applies committed log entries.
		kvStore *KVStore

		// --- Other fields ---
		id    string   // The unique ID of this node
		peers []string // The IDs of the other nodes in the cluster
		state State    // The current state (Follower, Candidate, or Leader)
		// ... mutexes, channels, etc. ...
	}

	RPC struct {
		senderID string
		message  interface{}
	}

	ClientRequest struct {
		Command []byte
	}

	State        int
	VoteResponse struct {
		Term        uint64
		VoteGranted bool
	}

	Log interface {
		Append(command []byte)
		LastIndex() uint64
		GetEntries(index uint64) []LogRecordV2
		GetTerm(index uint64) uint64
	}

	VoteRequest struct {
		Term         uint64
		CandidateID  string
		LastLogIndex uint64
		LastLogTerm  uint64
	}
)

const (
	Follower State = iota
	Candidate
	Leader
)

// resetElectionTimer resets the election timer to a random timeout between 150ms and 300ms.
func (rn *RaftNode) resetElectionTimer() *time.Timer {
	randomTimeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	return time.NewTimer(randomTimeout)
}

func (rn *RaftNode) runFollower() {
	log.Printf("Node %s: Running in follower state for Term %d", rn.id, rn.currentTerm)
	electionTimer := rn.resetElectionTimer()
	for rn.state == Follower {
		select {
		case <-electionTimer.C:
			log.Printf("Node %s: Election timeout, becoming candidate for term %d", rn.id, rn.currentTerm+1)
			rn.state = Candidate
			return
		case rpc := <-rn.rpcChan:
			// Handle RPC. If it's a valid heartbeat, this function will reset the election timer.
			rn.handleRPC(rpc)
			electionTimer = rn.resetElectionTimer()
		}
	}
}

func (rn *RaftNode) sendAppendEntries(peer string, appendEntriesRequest AppendEntriesRequest) {
	rn.rpcChan <- RPC{message: appendEntriesRequest}
}

func (rn *RaftNode) handleAppendEntriesRequest(peerID string, request AppendEntriesRequest) {
	// 1. Reply false if the leader's term is older than our own.
	if request.Term < rn.currentTerm {
		// rn.sendAppendEntriesResponse(leaderID, false)
		return
	}

	// If we receive a request from a newer term, we must update our own term
	// and step down if we were a candidate or leader.
	if request.Term > rn.currentTerm {
		rn.currentTerm = request.Term
		rn.state = Follower
		rn.votedFor = ""
	}

	// 2. Perform the log consistency check.
	// Reply false if our log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm.
	if request.PrevLogIndex > 0 {
		term := rn.log.GetTerm(request.PrevLogIndex)
		if term != request.PrevLogTerm {
			// rn.sendAppendEntriesResponse(leaderID, false)
			return
		}
	}

	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that follow it.
	if len(request.Entries) > 0 {
		// This is the "log repair" step.
		// rn.log.TruncateAndAppend(request.Entries)
	}

	// 5. Update our commitIndex based on the leader's.
	if request.LeaderCommit > rn.commitIndex {
		lastNewEntryIndex := request.PrevLogIndex + uint64(len(request.Entries))
		rn.commitIndex = min(request.LeaderCommit, lastNewEntryIndex)
		// We would now apply entries from lastApplied up to commitIndex
		// to our local state machine (the KVStore).
	}

	// If we've passed all checks, the append was successful.
	// rn.sendAppendEntriesResponse(leaderID, true)
}

// handleRPC is the central dispatcher for all incoming RPC messages.
func (rn *RaftNode) handleRPC(rpc RPC) {
	// We use a type switch to determine the kind of message we received.
	switch msg := rpc.message.(type) {

	case AppendEntriesRequest:
		// We received a request from a leader to append entries (or a heartbeat).
		// This is where a follower's logic for handling leader messages goes.
		rn.handleAppendEntriesRequest(rpc.senderID, msg)

	case AppendEntriesResponse:
		// We received a response from a follower to our own AppendEntries request.
		// This is the leader's logic for updating follower progress.
		// This is the answer to your question.
		rn.handleAppendEntriesResponse(rpc.senderID, msg)

	case VoteRequest:
		// We received a request from a candidate asking for our vote.
		rn.handleRequestVoteRequest(rpc.senderID, msg)

	case VoteResponse:
		// We received a response from a follower to our own RequestVote request.
		// This is a candidate's logic for tallying votes.
		// We already handled this inside the runCandidate() loop, but a central
		// handler is also a clean way to do it.
		rn.handleRequestVoteResponse(rpc.senderID, msg)

	default:
		log.Printf("Node %s: Received unknown RPC type", rn.id)
	}
}
func (rn *RaftNode) handleAppendEntriesResponse(peerID string, response AppendEntriesResponse) {
	// First, if the follower has a higher term, we are no longer the leader.
	if response.Term > rn.currentTerm {
		rn.currentTerm = response.Term
		rn.state = Follower
		return
	}

	// If the response was successful, the follower's log is now consistent.
	if response.Success {
		// 1. Update our knowledge of the follower's log.
		rn.matchIndex[peerID] = rn.nextIndex[peerID] - 1
		rn.nextIndex[peerID] = rn.matchIndex[peerID] + 1
		// 2. Check if we can advance the commitIndex.
		rn.updateCommitIndex()
	} else {
		// The follower's log didn't match. Must backtrack.
		// 1. Decrement the nextIndex for this peer.
		rn.nextIndex[peerID]--
		// Retry: the next heartbeat will send the preceding entry.
	}
}

func (rn *RaftNode) updateCommitIndex() {
	// Start checking from the entry just after the current commitIndex.
	for N := rn.log.LastIndex(); N > rn.commitIndex; N-- {
		// We can only commit an entry from our own current term.
		if rn.log.GetTerm(N) != rn.currentTerm {
			break
		}
		// Count how many servers have this entry.
		matchCount := 1
		for _, peer := range rn.peers {
			if rn.matchIndex[peer] >= N {
				matchCount++
			}
		}

		// If it's on a majority of servers, we can commit it.
		if matchCount >= rn.majoritySize() {
			rn.commitIndex = N
			break
		}
	}
	// Now that commitIndex has advanced, we need to apply the newly committed entries to our own state machine.
	// ... apply logic would go here ...
}

func (rn *RaftNode) sendHeartbeats() {
	for _, peer := range rn.peers {
		// Get Follower's State: Look up the nextIndex for the current peer. This tells you which log entry you should send them next.
		nextIndex := rn.nextIndex[peer]
		// Prepare Log Entries: Get the slice of log entries from your own log, starting from that nextIndex.
		entries := rn.log.GetEntries(nextIndex)
		// Find prevLogTerm: Get the term of the log entry that comes just before nextIndex.
		prevLogTerm := rn.log.GetTerm(nextIndex - 1)

		// Construct RPC: Create an AppendEntries RPC struct, filling it with all the necessary information: the current term, the leader's ID, prevLogIndex, prevLogTerm, the actual log entries, and the leader's commitIndex.
		appendEntriesRequest := AppendEntriesRequest{
			Term:         rn.currentTerm,
			LeaderID:     rn.id,
			PrevLogIndex: nextIndex - 1,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rn.commitIndex,
		}
		// Dispatch RPC: Send this RPC to the peer in a separate goroutine so you can contact all peers in parallel.
		go rn.sendAppendEntries(peer, appendEntriesRequest) // TODO: Implement this
	}
}
func (rn *RaftNode) sendRequestVote(peer string) {}
func (rn *RaftNode) majoritySize() int           { return len(rn.peers)/2 + 1 }

func (rn *RaftNode) handleRequestVoteRequest(candidateID string, request VoteRequest) {
	// 1. Reply false if the candidate's term is older than our own.
	if request.Term < rn.currentTerm {
		// rn.sendVoteResponse(candidateID, false)
		return
	}

	// If we receive a request from a newer term, we must update our own term
	// and step down if we were a candidate or leader.
	if request.Term > rn.currentTerm {
		rn.currentTerm = request.Term
		rn.state = Follower
		rn.votedFor = "" // We haven't voted in this new, higher term yet.
	}

	// 2. Check if we've already voted in this term.
	// If votedFor is null or already for this candidate, we can proceed.
	if rn.votedFor != "" && rn.votedFor != candidateID {
		// We've already voted for someone else in this term.
		// rn.sendVoteResponse(candidateID, false)
		return
	}

	// 3. Check if the candidate's log is at least as up-to-date as ours.
	// This is the crucial election safety rule.
	ourLastLogTerm := rn.log.GetTerm(rn.log.LastIndex())
	ourLastLogIndex := rn.log.LastIndex()

	if request.LastLogTerm < ourLastLogTerm || (request.LastLogTerm == ourLastLogTerm && request.LastLogIndex < ourLastLogIndex) {
		// The candidate's log is older than ours. Deny the vote.
		// rn.sendVoteResponse(candidateID, false)
		return
	}

	// If we've passed all the checks, grant the vote.
	rn.votedFor = candidateID
	log.Printf("Node %s: Voted for %s in Term %d.", rn.id, candidateID, rn.currentTerm)
	// rn.sendVoteResponse(candidateID, true)
}

func (rn *RaftNode) handleRequestVoteResponse(peerID string, response VoteResponse) {
	votesReceived := 1
	if response.Term > rn.currentTerm {
		log.Printf("Node %s: Received higher term %d, stepping down to follower", rn.id, response.Term)
		rn.currentTerm = response.Term
		rn.state = Follower
		return
	}
	if response.VoteGranted {
		votesReceived++
		if votesReceived > rn.majoritySize() {
			log.Printf("Node %s: Won election for Term %d, becoming leader", rn.id, rn.currentTerm)
			rn.state = Leader
			return
		}
	}
}

func (rn *RaftNode) runCandidate() {
	rn.currentTerm++
	rn.votedFor = rn.id
	log.Printf("Node %s: Starting election for Term %d", rn.id, rn.currentTerm)
	votesReceived := 1
	// Send RequestVote RPCs to all peers
	for _, peer := range rn.peers {
		go rn.sendRequestVote(peer)
	}

	// Reset election timer for this election attempt
	electionTimer := rn.resetElectionTimer()
	for rn.state == Candidate {
		select {
		case <-electionTimer.C:
			log.Printf("Node %s: Election timeout, becoming candidate for term %d", rn.id, rn.currentTerm)
			return

		case rpc := <-rn.rpcChan:
			// Handle incoming RPC.
			switch msg := rpc.message.(type) {
			case VoteResponse:
				// BUGFIX: All vote tallying logic is now self-contained here.
				if msg.Term > rn.currentTerm {
					rn.currentTerm = msg.Term
					rn.state = Follower
					return
				}
				if msg.VoteGranted {
					votesReceived++
					log.Printf("Node %s: Vote received. Total votes: %d", rn.id, votesReceived)
					if votesReceived >= rn.majoritySize() {
						log.Printf("Node %s: Won election for Term %d. Becoming Leader.", rn.id, rn.currentTerm)
						rn.state = Leader
						return
					}
				}
			case AppendEntriesRequest:
				rn.handleAppendEntriesRequest(rpc.senderID, msg)
			}
		}
	}
}

func (rn *RaftNode) runLeader() {
	log.Printf("Node %s: Entering Leader state for Term %d.", rn.id, rn.currentTerm)
	// --- Leader logic: send heartbeats, handle client requests ---
	// ... to be implemented ...
	// Initialize its state: It needs to figure out where each follower is in the log.
	// nextIndex for each follower is the index of the next log entry to send to that follower.
	// Optimistically set it to the index of the last log entry + 1.
	for _, peer := range rn.peers {
		rn.nextIndex[peer] = rn.log.LastIndex() + 1
		rn.matchIndex[peer] = 0
	}

	// Leader must send heartbeats to maintain authority and prevent new elections.
	heartbeatTicker := time.NewTicker(50 * time.Millisecond)
	defer heartbeatTicker.Stop()

	// Send an initial heartbeat to all peers immediately upon election.
	rn.sendHeartbeats()

	// --- 2. The Leader's Main Loop ---
	for rn.state == Leader {
		select {
		case <-heartbeatTicker.C:
			rn.sendHeartbeats()
		case clientRequest := <-rn.clientRequestChan:
			rn.log.AppendTransaction(rn.currentTerm, clientRequest.Command)
			rn.sendHeartbeats()
		case rpc := <-rn.rpcChan:
			// Received a response from a follower or a request from another node.
			// A real implementation would handle AppendEntriesResponse and
			// update nextIndex, matchIndex, and the commitIndex accordingly.
			// It must also check if the RPC has a higher term, in which case
			// this leader must step down and become a follower.
			rn.handleRPC(rpc)
		}
	}

	// Send periodic heartbeats: It must constantly send messages to its followers to assert its authority and prevent new elections.
	for rn.state == Leader {
		select {
		case <-heartbeatTicker.C:
			rn.sendHeartbeats()
		}
	}
	// Handle client requests: It must take new commands, add them to its log, and replicate them.

	// Commit log entries: It must track the progress of replication and decide when an entry is safely on a majority of servers.
}

func (rn *RaftNode) Run() {
	for {
		switch rn.state {
		case Follower:
			// Two things to do in the follower state:
			// 1. Handles RPC requests from a leader or candidate
			// 2. Our election timer to fire
			rn.runFollower()
		case Candidate:
			rn.runCandidate()
		case Leader:
			rn.runLeader()
		}
	}
}
