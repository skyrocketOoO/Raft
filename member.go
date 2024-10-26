package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type State string

const (
	FollowerState  State = "follower"
	CandidateState State = "candidate"
	LeaderState    State = "leader"
	StopState      State = "stop"
	ShutDownState  State = "shutdown"
)

const (
	HeartBeatTimeoutInterval = 8
	ElectionTimeoutInterval  = 8
)

type HeartBeatRequest struct {
	Term int
	ID   int
}

type RequestVoteRequest struct {
	Term int
	ID   int
}

type VoteRequest struct {
	Term int
}

type ShutDownRequest struct{}

type LiveMembers struct {
	Members map[int]*Member
	Mutex   sync.RWMutex
}

type Member struct {
	ID               int
	State            State
	CurrentTerm      int
	HeartBeatTimeout time.Time
	ElectionTimeout  time.Time
	Tikects          int
	LiveMembers      *LiveMembers
	RequestChan      chan interface{}
	OnElection       bool
	Voted            bool
}

func NewMember(id int, liveMembers *LiveMembers) *Member {
	return &Member{
		ID:          id,
		State:       StopState,
		CurrentTerm: 0,
		LiveMembers: liveMembers,
		RequestChan: make(chan interface{}, 100),
		OnElection:  false,
	}
}

func (m *Member) Kill() {
	fmt.Printf("Member %d killed\n", m.ID)
	m.LiveMembers.Mutex.Lock()
	defer m.LiveMembers.Mutex.Unlock()
	delete(m.LiveMembers.Members, m.ID)
	m.State = ShutDownState
}

func (m *Member) RequestVotes() {
	m.LiveMembers.Mutex.RLock()
	defer m.LiveMembers.Mutex.RUnlock()

	for _, member := range m.LiveMembers.Members {
		if member.ID != m.ID {
			member.RequestChan <- RequestVoteRequest{
				Term: m.CurrentTerm,
				ID:   m.ID,
			}
		}
	}
}

func (m *Member) SendHeartBeats() {
	m.LiveMembers.Mutex.RLock()
	defer m.LiveMembers.Mutex.RUnlock()

	for _, member := range m.LiveMembers.Members {
		if member.ID != m.ID {
			member.RequestChan <- HeartBeatRequest{
				Term: m.CurrentTerm,
				ID:   m.ID,
			}
		}
	}
}

func (m *Member) StartAnElection() {
	fmt.Printf("Member %d: I want to be leader\n", m.ID)
	m.UpdateElectionTimeout()
	m.CurrentTerm++
	m.State = CandidateState
	m.RequestVotes()
	m.Voted = true
	m.Tikects = 1
}

func (m *Member) Vote(memberID int) {
	fmt.Printf("Member %d: Accept member %d to be leader\n", m.ID, memberID)
	m.UpdateElectionTimeout()
	m.LiveMembers.Members[memberID].RequestChan <- VoteRequest{
		Term: m.CurrentTerm,
	}
	m.Voted = true
}

func (m *Member) BeLeader() {
	fmt.Printf("Member %d voted to be leader: (%d >= %d/2)\n", m.ID, m.Tikects, len(m.LiveMembers.Members))
	m.State = LeaderState
	m.Tikects = 0
	m.OnElection = false
	m.Voted = false
}

func (m *Member) ExitElection() {
	m.Voted = false
	m.OnElection = false
	m.Tikects = 0
}

func (m *Member) UpdateElectionTimeout() {
	randElectionTimeoutInterval := time.Duration(3+rand.Intn(ElectionTimeoutInterval-3)) * time.Second
	m.ElectionTimeout = time.Now().Add(randElectionTimeoutInterval)
}

func (m *Member) UpdateHeartBeatTimeout() {
	randHeartBeatTimeoutInterval := time.Duration(3+rand.Intn(int(HeartBeatTimeoutInterval-3))) * time.Second
	m.HeartBeatTimeout = time.Now().Add(randHeartBeatTimeoutInterval)
}

func (m *Member) Run() {
	fmt.Printf("Member %d: Hi\n", m.ID)
	m.State = FollowerState
	m.UpdateElectionTimeout()

	for {
		// fmt.Printf("Member %d: %s %d %d\n", m.ID, m.State, m.CurrentTerm, len(m.RequestChan))
		switch m.State {
		case FollowerState:
			if m.OnElection {
				select {
				case request := <-m.RequestChan:
					switch req := request.(type) {
					case HeartBeatRequest:
						if req.Term < m.CurrentTerm {
							continue
						}
						// new Leader is born
						m.CurrentTerm = req.Term
						m.ExitElection()
						m.UpdateElectionTimeout()
					case RequestVoteRequest:
						if req.Term < m.CurrentTerm {
							continue
						} else if req.Term > m.CurrentTerm {
							m.CurrentTerm = req.Term
							m.Vote(req.ID)
						}
					}
				default:
					if m.ElectionTimeout.Before(time.Now()) {
						fmt.Printf("Member %d election timeout, go to term %d\n", m.ID, m.CurrentTerm+1)
						m.StartAnElection()
						continue
					}
				}
			} else {
				select {
				case request := <-m.RequestChan:
					switch req := request.(type) {
					case HeartBeatRequest:
						if req.Term < m.CurrentTerm {
							continue
						}
						m.UpdateElectionTimeout()
					case RequestVoteRequest:
						if req.Term <= m.CurrentTerm {
							continue
						}
						// enter election
						m.OnElection = true
						m.CurrentTerm = req.Term
						m.Vote(req.ID)
					}
				default:
					if m.HeartBeatTimeout.Before(time.Now()) {
						fmt.Printf("Member %d heartbeat timeout, go to term %d\n", m.ID, m.CurrentTerm+1)
						m.StartAnElection()
						continue
					}
				}
			}
		case CandidateState:
			select {
			case request := <-m.RequestChan:
				switch req := request.(type) {
				case HeartBeatRequest:
					if req.Term < m.CurrentTerm {
						continue
					}
					// new leader is born
					m.CurrentTerm = req.Term
					m.State = FollowerState
					m.ExitElection()
					m.UpdateElectionTimeout()
				case RequestVoteRequest:
					if req.Term < m.CurrentTerm {
						continue
					} else if req.Term > m.CurrentTerm {
						m.CurrentTerm = req.Term
						m.Vote(req.ID)
						m.State = FollowerState
						m.OnElection = true
					} else {
						// candidate already elected itself
						continue
					}
				case VoteRequest:
					if req.Term < m.CurrentTerm {
						continue
					}
					m.Tikects++
					if m.Tikects*2 >= len(m.LiveMembers.Members) {
						m.BeLeader()
						continue
					}
				}
			default:
				if m.ElectionTimeout.Before(time.Now()) {
					fmt.Printf("Member %d election timeout, go to term %d\n", m.ID, m.CurrentTerm+1)
					m.StartAnElection()
					continue
				}
			}
		case LeaderState:
			select {
			case request := <-m.RequestChan:
				switch req := request.(type) {
				case HeartBeatRequest:
					if req.Term <= m.CurrentTerm {
						continue
					}
					m.CurrentTerm = req.Term
					m.State = FollowerState
					m.UpdateHeartBeatTimeout()
				case RequestVoteRequest:
					if req.Term <= m.CurrentTerm {
						continue
					}
					m.CurrentTerm = req.Term
					m.State = CandidateState
					m.Vote(req.ID)
				}
			default:
				m.SendHeartBeats()
			}
		case ShutDownState:
			m.State = StopState
			return
		}

		time.Sleep(time.Millisecond * 100)
	}
}
