package main

import (
	proto "auction/grpc"
	"bufio"
	"context"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var id int64
var wait_group = &sync.WaitGroup{}

// A list, containing the three main servers. If all of these are down then clients are unable to connect
// However, if the client has connected, then the list gets updated to be all active servers
var servers = []string{
	"localhost:8080",
	"localhost:8081",
	"localhost:8082",
}

func main() {

	client := connect()
	// Assumes that the server that was established connection to has not immediately crashed
	response, _ := client.GetClientId(context.Background(), &proto.Empty{})
	id = response.Id
	// Setup file logging
	f, err := os.OpenFile(fmt.Sprintf("clientlog%d.txt", id), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	log.SetOutput(f)

	log.Printf("Client started with id %d\n", id)

	wait_group.Add(1)
	go get_input(bufio.NewReader(bufio.NewReader(os.Stdin)), client)
	wait_group.Wait()
}

func get_input(reader *bufio.Reader, client proto.AuctionClient) {
	defer wait_group.Done()
	println("Type 'state' to get auction state, type 'bid' followed by a space and a number to bid")
	for {
		input, err := reader.ReadString('\n')
		if err != nil {
			log.Fatal(err)
		}

		input = strings.TrimSpace(input)
		log.Printf("Got input '%v'\n", input)

		if input == ".quit" {
			log.Printf("Closing client %d\n", id)
			return
		} else if input == "state" {
			result, err := client.Result(context.Background(), &proto.Empty{})
			if err != nil {
				log.Println("Connection lost. Reconnecting and retrying...")
				client = connect()
				result, err = client.Result(context.Background(), &proto.Empty{})
			}
			if result.AuctionOver {
				fmt.Printf("Auction over. %d won with a bid of %d\n", result.HighestBidder, result.HighestBid)
				log.Printf("Auction over. %d won with a bid of %d\n", result.HighestBidder, result.HighestBid)
			} else {
				fmt.Printf("Auction running. %d is the highest bidder with a bid of %d\n", result.HighestBidder, result.HighestBid)
				log.Printf("Auction running. %d is the highest bidder with a bid of %d\n", result.HighestBidder, result.HighestBid)
			}
		} else if strings.Split(input, " ")[0] == "bid" {
			var bid int64 = 0
			if len(strings.Split(input, " ")) > 1 {
				bid, _ = strconv.ParseInt(strings.Split(input, " ")[1], 10, 64)
			}
			response, err := client.Bid(context.Background(), &proto.BidAmount{
				Bidder:    id,
				BidAmount: bid,
			})

			if err != nil {
				log.Println("Connection lost. Reconnecting and retrying...")
				client = connect()
				response, err = client.Bid(context.Background(), &proto.BidAmount{
					Bidder:    id,
					BidAmount: bid,
				})
			}
			if len(response.ErrorMessage) > 0 {
				fmt.Println(response.ErrorMessage)
				log.Printf("Got error message %v\n", response.ErrorMessage)
			} else {
				if response.Accepted {
					fmt.Printf("Bid of %d accepted\n", bid)
					log.Printf("Bid of %d accepted\n", bid)
				} else {
					fmt.Printf("Bid of %d denied\n", bid)
					log.Printf("Bid of %d denied\n", bid)
				}
			}
		}

		if err != nil {
			log.Println(err)
		}
	}
}

func connect() proto.AuctionClient {
	// Infinite loop if no server is running, but in that case we got bigger problems
	for {
		for _, server := range servers {
			conn, err := grpc.Dial(server, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("Server not found: %v\n", server)
				continue
			}

			log.Printf("Connecting to server %v\n", server)
			client := proto.NewAuctionClient(conn)

			// Update fallback server list
			response, err := client.GetReplicaList(context.Background(), &proto.Empty{})
			if err != nil {
				log.Println("Connection failed")
				continue
			}
			if len(response.Replicas) > 0 {
				servers = response.Replicas
				for _, s := range servers {
					fmt.Println(s)
				}
			}

			log.Printf("Connected!")

			return client
		}
	}
}
