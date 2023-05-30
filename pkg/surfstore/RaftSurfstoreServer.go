package surfstore

//
import (
	context "context"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation

	nextIndex      map[int64]int64
	matchIndex     map[int64]int64
	peers          []string
	id             int64
	commitIndex    int64
	lastApplied    int64
	pendingCommits ([]*chan bool)
	metaStore      *MetaStore
	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {

	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	timeoutDuration := time.Second

	ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancel()

	res, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}
	isSuccess := res.Flag
	for !isSuccess {
		res, err = s.SendHeartbeat(ctx, &emptypb.Empty{})
		if err != nil {
			return nil, err
		}
	}
	fileInfoMap := s.metaStore.FileMetaMap
	FIM := &FileInfoMap{FileInfoMap: fileInfoMap}
	return FIM, nil

}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	// panic("todo")
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	return s.metaStore.GetBlockStoreMap(ctx, hashes)
	// return nil, nil
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	// panic("todo")
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	return s.metaStore.GetBlockStoreAddrs(ctx, empty)
	// return nil, nil
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	// panic("todo")
	// return nil, nil
	s.isCrashedMutex.Lock()
	if s.isCrashed {
		s.isCrashedMutex.Unlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.Unlock()

	s.isLeaderMutex.Lock()
	if !s.isLeader {
		s.isLeaderMutex.Unlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.Unlock()

	// append entry to our log
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	s.log = append(s.log, &UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	})

	curr_committed := make(chan bool, 1)
	s.pendingCommits = append(s.pendingCommits, &curr_committed)
	timeDuration := time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeDuration)
	defer cancel()
	res, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}

	// Try until you get the majority votes
	for !res.Flag {
		res, err = s.SendHeartbeat(ctx, &emptypb.Empty{})
		if err != nil {
			return nil, err
		}
	}

	success := <-curr_committed
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	if success {
		log.Printf("UPDATEFile() line 143 -> Server%v: File is Commited", s.id)
		ver, err := s.metaStore.UpdateFile(ctx, filemeta)
		if err != nil {
			return ver, err
		} else {
			res, err2 := s.SendHeartbeat(ctx, &emptypb.Empty{})
			if err2 != nil {
				return &Version{Version: int32(-1)}, err2
			}
			if res.Flag {
				return ver, nil
			}
		}
	}

	return &Version{Version: int32(-1)}, fmt.Errorf("Error in Update File")
}

// 1. Reply false if term < currentTerm (Â§5.1)
// 2. Reply false if log doesnâ€™t contain an entry at prevLogIndex whose term
// matches prevLogTerm (Â§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (Â§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)

func appendchanneltoPendingCommits(s *RaftSurfstore) {
	for i := len(s.pendingCommits); i < len(s.log); i++ {
		curr_chan := make(chan bool, 1)
		s.pendingCommits = append(s.pendingCommits, &curr_chan)
	}
}
func updateMetaStoreFiles(s *RaftSurfstore, ctx context.Context) {
	for i := s.lastApplied + 1; i <= s.commitIndex; i++ {
		s.metaStore.UpdateFile(ctx, s.log[i].FileMetaData)
	}
}
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {

	s.isCrashedMutex.Lock()
	defer s.isCrashedMutex.Unlock()

	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	var eachReply AppendEntryOutput
	eachReply.ServerId = s.id
	eachReply.Term = s.term
	eachReply.Success = false

	if input.Term > s.term {
		log.Printf("AppendEntries() -> Server%v has lower term:%v (but given input is:%v) and is a not leader", s.id, s.term, input.Term)
		s.SetFollower(ctx, input.Term, &emptypb.Empty{})
	}

	if input.Term == s.term {
		if s.isLeader {
			s.SetFollower(ctx, input.Term, &emptypb.Empty{})
		}
		isNeg1PrevLogIndex := input.PrevLogIndex == -1
		isWithInRange := input.PrevLogIndex < int64(len(s.log))

		if isNeg1PrevLogIndex || (isWithInRange && input.PrevLogTerm == s.log[input.PrevLogIndex].Term) {

			curr_localLogInsertIndex := int(input.PrevLogIndex) + 1
			remoteEntries_Index := 0

			for {
				if remoteEntries_Index >= len(input.Entries) {
					break
				}
				// check if eof for locallog
				if curr_localLogInsertIndex >= len(s.log) {
					break
				}
				localTerm := s.log[curr_localLogInsertIndex].Term
				remoteEntriesTerm := input.Entries[remoteEntries_Index].Term

				if remoteEntriesTerm != localTerm {
					break
				}
				remoteEntries_Index++
				curr_localLogInsertIndex++

			}
			if len(input.Entries) > remoteEntries_Index {
				log.Printf("inside AppendEntries() -> Server%v: FoundNewEntries", s.id)

				// Append new entries to the locallog
				s.log = append(s.log[:curr_localLogInsertIndex], input.Entries[remoteEntries_Index:]...)

				// scrape the waiting old commits
				for i := curr_localLogInsertIndex; i < len(s.pendingCommits); i++ {
					*s.pendingCommits[i] <- false
				}

				// Append channel to pending commits to track their respective status
				appendchanneltoPendingCommits(s)
			}

			// update the commit Index
			isInputCommitGreater := input.LeaderCommit > s.commitIndex
			if isInputCommitGreater {
				s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))
				isScommitIndexHigher := s.commitIndex > s.lastApplied
				if isScommitIndexHigher {
					updateMetaStoreFiles(s, ctx)
					s.lastApplied = s.commitIndex
					log.Printf("AppendEntries()-> Server%v: Updated the last Applied to %v", s.id, s.lastApplied)
				}
			}
			eachReply.Success = true
		}
	}
	eachReply.Term = s.term
	return &eachReply, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	if s.isCrashed {
		s.isCrashedMutex.Unlock()
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeader = true
	s.term += 1
	s.isCrashedMutex.Unlock()

	for eachPeerIndex, _ := range s.peers {
		if eachPeerIndex == int(s.id) {
			continue
		}
		s.matchIndex[int64(eachPeerIndex)] = -1
		s.nextIndex[int64(eachPeerIndex)] = int64(len(s.log))
	}

	// Check if the Majority is Up
	res, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}

	// Keep Trying to Get Majority Servers Up
	timeoutDuration := time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancel()
	isNotRes := !res.Flag
	for isNotRes {
		res, err = s.SendHeartbeat(ctx, &emptypb.Empty{})
		if err != nil {
			return nil, err
		}
	}

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) SetFollower(ctx context.Context, term int64, _ *emptypb.Empty) (*Success, error) {
	s.term = term
	s.isLeader = false
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {

	log.Printf("SendHeartBeat() entered -> Server%v:", s.id)
	if !s.isLeader {
		log.Printf("Server%v: No Leader", s.id)
		return &Success{Flag: false}, ERR_NOT_LEADER
	}

	if s.isCrashed {
		log.Printf("Server%v: CRASHED", s.id)
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}

	s.isCrashedMutex.Lock()
	currentTerm := s.term
	s.isCrashedMutex.Unlock()

	// Make a channel

	talkedToPeerChan := make(chan bool)

	for eachPeerIndex, _ := range s.peers {
		if eachPeerIndex == int(s.id) {
			continue
		}
		go s.communicateWithPeer(currentTerm, int64(eachPeerIndex), talkedToPeerChan)
	}

	errCount := 0
	for i := 0; i < len(s.peers)-1; i++ {
		receiv := <-talkedToPeerChan
		isNotreceiv := !receiv
		if isNotreceiv {
			errCount += 1
		}
	}

	log.Printf("SendHeartBeat() Line 350 EXITING.... Server%v:", s.id)

	isTwiceErrorCont := 2*errCount >= len(s.peers)+1
	if isTwiceErrorCont {
		return &Success{Flag: false}, nil
	}

	return &Success{Flag: true}, nil
}
func (s *RaftSurfstore) communicateWithPeer(currentTerm int64, peerID int64, finished chan bool) {
	log.Println("INSIDE communicateWithPeer() ENTERED Line 356")
	s.isCrashedMutex.Lock()
	if s.isCrashed {
		s.isCrashedMutex.Unlock()
		finished <- false
		return
	}
	nextIndex := s.nextIndex[peerID]
	currLocalCommitIndex := s.commitIndex
	prevLogTerm := int64(-1)
	prevLogIndex := nextIndex - 1
	if prevLogIndex >= 0 {
		prevLogTerm = s.log[prevLogIndex].Term
	}
	entries := s.log[nextIndex:]
	s.isCrashedMutex.Unlock()

	conn, err := grpc.Dial(s.peers[peerID], grpc.WithInsecure())
	if err != nil {
		finished <- false
		return
	}
	NewRaftSurfstoreClientConn := NewRaftSurfstoreClient(conn)

	timeoutDuration := time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancel()
	eachReply, err := NewRaftSurfstoreClientConn.AppendEntries(ctx, &AppendEntryInput{
		Term:         currentTerm,
		PrevLogIndex: int64(prevLogIndex),
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: currLocalCommitIndex,
	})

	s.isCrashedMutex.Lock()
	if s.isCrashed {
		s.isCrashedMutex.Unlock()
		finished <- false
		return
	}
	s.isCrashedMutex.Unlock()

	if err == nil {
		s.isCrashedMutex.Lock()
		defer s.isCrashedMutex.Unlock()
		if eachReply.Term > currentTerm {
			// log.Printf("Local Server is has lower term; is not a leader")
			s.SetFollower(ctx, eachReply.Term, &emptypb.Empty{})
			finished <- true
			return
		}

		if s.isLeader && (currentTerm == eachReply.Term) {
			if eachReply.Success {
				// Update next and match index
				s.nextIndex[peerID] = nextIndex + int64(len(entries))
				s.matchIndex[peerID] = nextIndex + int64(len(entries)) - 1

				startingCommitIndex := s.commitIndex

				for i := s.commitIndex + 1; i < int64(len(s.log)); i++ {
					if s.log[i].Term == s.term {
						count := 1
						for eachPeerIndex, _ := range s.peers {
							if eachPeerIndex == int(s.id) {
								continue
							}
							if s.matchIndex[int64(eachPeerIndex)] >= i {
								count += 1
							}
						}
						if count*2 >= len(s.peers)+1 {
							s.commitIndex = i
						} else {
							break
						}
					}
				}

				// Apply the changes
				if s.commitIndex > startingCommitIndex && s.commitIndex > s.lastApplied {
					log.Printf("Server%v(t:%v,ci:%v)->communicateWithPeer: Commit Index incremented. Updating the local state machine", s.id, s.term, s.commitIndex)
					for i := s.lastApplied + 1; i <= s.commitIndex; i++ {
						*s.pendingCommits[i] <- true
					}
					log.Printf("Server%v(t:%v,ci:%v)->communicateWithPeer: Commit Index incremented. UPDATED the local state machine", s.id, s.term, s.commitIndex)
					s.lastApplied = s.commitIndex
				}

			} else {
				// decrement next index
				log.Printf("Local Server returned failed AE-> decrement nextIndex for peer %v", peerID)
				s.nextIndex[peerID] = nextIndex - 1
			}
		}

		finished <- true
	} else {
		finished <- false
	}
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
