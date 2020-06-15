// A basic imlplementation of Bully algorithm
// When a pid is told that the leader is down, it pings the leader
// If the leader does not reply in 2 rounds, the election is started
// During the election, if the leader comes back up, it tells everyone that it is the leader

package bully

// msgType represents the type of messages that can be passed between nodes
type msgType int

// customized messages to be used for communication between nodes
const (
	ELECTION msgType = iota
	OK
	ALIVE
	YES
	COORDINATOR
	// TODO: add / change message types as needed
)

// Message is used to communicate with the other nodes
// DO NOT MODIFY THIS STRUCT
type Message struct {
	Pid   int
	Round int
	Type  msgType
}

// Bully runs the bully algorithm for leader election on a node
func Bully(pid int, leader int, checkLeader chan bool, comm map[int]chan Message, startRound <-chan int, quit <-chan bool, electionResult chan<- int) {

	// TODO: initialization code
	roundsSinceLeaderReplied := 0
	waitingForLeaderReply := false
	calledElection := false
	roundInWhichElectionCalled := -1
	lastRound := 0

	for {
		// quit / crash the program
		if <-quit {
			return
		}

		// start the round
		roundNum := <-startRound
		newMessages := getMessages(comm[pid], roundNum-1)

		// Check if I was a leader and I was down
		// If true, tell everyone I am back
		if lastRound != roundNum-1 && pid == leader {
			for _, c := range comm {
				c <- Message{pid, roundNum, COORDINATOR}
			}
		}

		for i := range newMessages {
			temp := newMessages[i]

			switch temp.Type {
			case COORDINATOR:
				leader = temp.Pid
				electionResult <- leader
				roundInWhichElectionCalled = -1
				calledElection = false
				break

			case YES:
				// Do Nothing
				break

			case ALIVE:
				comm[temp.Pid] <- Message{pid, roundNum, YES}
				break

			case ELECTION:
				// Check if I am the highest pid, tell others I am leader
				if isHighestPid(pid, comm) {
					for _, c := range comm {
						c <- Message{pid, roundNum, COORDINATOR}
					}
					continue
				}
				// Tell the node that sent election message that I will takeover the election
				comm[temp.Pid] <- Message{pid, roundNum, OK}

				// Call election
				hasSentMessageToSomeone := false
				if !calledElection {
					calledElection = true
					roundInWhichElectionCalled = roundNum
					for p, c := range comm {
						if p > pid {
							c <- Message{pid, roundNum, ELECTION}
							hasSentMessageToSomeone = true
						}
					}
					// If I did not send message to anyone, then I am the leader
					if !hasSentMessageToSomeone {
						for _, c := range comm {
							c <- Message{pid, roundNum, COORDINATOR}
							calledElection = false
							roundInWhichElectionCalled = -1
						}
					}
				}
				break

			case OK:
				calledElection = false
				roundInWhichElectionCalled = -1
				break
			}
		}

		// If no one replied to my election call after 2 rounds, I am the leader
		if calledElection && roundNum-roundInWhichElectionCalled >= 2 {
			calledElection = false
			roundInWhichElectionCalled = -1
			for _, c := range comm {
				c <- Message{pid, roundNum, COORDINATOR}
			}
		}

		// Check if leader is down
		if len(checkLeader) > 0 {
			<-checkLeader
			newMessage := Message{pid, roundNum, ALIVE}
			comm[leader] <- newMessage
			waitingForLeaderReply = true
			roundsSinceLeaderReplied = 0
		}

		// If leader has not replied after checking and 2 rounds have passed
		// Initiate election
		if waitingForLeaderReply && roundsSinceLeaderReplied >= 2 {
			waitingForLeaderReply = false
			roundsSinceLeaderReplied = 0

			hasSentMessageToSomeone := false
			if !calledElection {
				calledElection = true
				roundInWhichElectionCalled = roundNum
				for p, c := range comm {
					if p > pid {
						c <- Message{pid, roundNum, ELECTION}
						hasSentMessageToSomeone = true
					}
				}
				if !hasSentMessageToSomeone {
					for _, c := range comm {
						c <- Message{pid, roundNum, COORDINATOR}
						calledElection = false
						roundInWhichElectionCalled = -1
					}
				}
			}
		}

		roundsSinceLeaderReplied++
		lastRound = roundNum
	}
}

// assumes messages from previous rounds have been read already
func getMessages(msgs chan Message, round int) []Message {
	var result []Message

	// check if there are messages to be read
	if len(msgs) > 0 {
		var m Message

		// read all messages belonging to the corresponding round
		for m = <-msgs; m.Round == round; m = <-msgs {
			result = append(result, m)
			if len(msgs) == 0 {
				break
			}
		}

		// insert back message from any other round
		if m.Round != round {
			msgs <- m
		}
	}
	return result
}

func isHighestPid(pid int, comm map[int]chan Message) bool {
	for p := range comm {
		if p > pid {
			return false
		}
	}
	return true
}

// TODO: helper functions
