package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: go run . <num_members>")
		return
	}
	numMembers, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Invalid number of members:", err)
		return
	}

	liveMembers := LiveMembers{
		Members: make(map[int]*Member),
		Mutex:   sync.RWMutex{},
	}
	fmt.Printf("Starting quorum with %d members\n", numMembers)
	for i := 0; i < numMembers; i++ {
		liveMembers.Members[i] = NewMember(i, &liveMembers)
	}
	liveMembers.Mutex.Lock()
	for _, member := range liveMembers.Members {
		go member.Run()
	}
	liveMembers.Mutex.Unlock()

	curMember := numMembers
	lowestMember := (numMembers + 1) / 2

	scanner := bufio.NewScanner(os.Stdin)
	for {
		if curMember < lowestMember {
			fmt.Printf("Quorum failed: live %d < loweset boundary %d/2\n", curMember, numMembers)
			return
		}

		fmt.Print("> ")
		if scanner.Scan() {
			command := scanner.Text()
			if len(command) >= 7 && command[:4] == "kill" {
				cmdSplit := strings.Split(command, " ")
				idString := cmdSplit[1][1:]
				id, err := strconv.Atoi(idString)
				if err != nil {
					panic(err.Error())
				}
				liveMembers.Members[id].Kill()
				curMember--
			}
		}
	}
}
