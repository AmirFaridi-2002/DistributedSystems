package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

const (
	defaultHost = "localhost"
	defaultPort = 9999
)

func main() {
	address := fmt.Sprintf("%s:%d", defaultHost, defaultPort)
	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Printf("Failed to connect to server: %s\n", err)
		return
	}
	defer conn.Close()

	fmt.Printf("Connected to server at %s\n", address)

	go func() {
		reader := bufio.NewReader(conn)
		for {
			response, err := reader.ReadString('\n')
			if err != nil {
				fmt.Println("Disconnected from server.")
				return
			}
			fmt.Print("Server response: " + response)
		}
	}()

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Enter commands (e.g.,")
	fmt.Println("  Put:key:value")
	fmt.Println("  Get:key")
	fmt.Println("  Delete:key")
	fmt.Println("  Update:key:oldValue:newValue")
	fmt.Println("):")
	for scanner.Scan() {
		command := scanner.Text()
		if len(command) == 0 {
			continue
		}
		_, err := conn.Write([]byte(command + "\n"))
		if err != nil {
			fmt.Printf("Failed to send command to server: %s\n", err)
			break
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading input: %s\n", err)
	}
}
