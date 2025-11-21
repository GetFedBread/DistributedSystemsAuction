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

func main() {
	println("Starting client")
	conn, err := grpc.NewClient("localhost:8080", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	client := proto.NewAuctionClient(conn)
	wait_group.Add(1)
	go get_input(bufio.NewReader(bufio.NewReader(os.Stdin)), client)
	wait_group.Wait()
}

func get_input(reader *bufio.Reader, client proto.AuctionClient) {
	defer wait_group.Done()
	for {
		input, err := reader.ReadString('\n')
		if err != nil {
			log.Fatal(err)
		}

		input = strings.TrimSpace(input)
		if input == ".quit" {
			log.Printf("Closing client %d\n", id)
			return
		} else if input == "state" {
			result, err := client.Result(context.Background(), &proto.Empty{})
			if err != nil {
				log.Println(err)
			}
			if result.AuctionOver {
				fmt.Printf("Auction over. %d won with a bid of %d\n", result.HighestBidder, result.HighestBid)
			} else {
				fmt.Printf("Auction running. %d is the highest bidder with a bid of %d\n", result.HighestBidder, result.HighestBid)
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
				log.Println(err)
			} else {
				if response.Accepted {
					fmt.Printf("Bid of %d accepted\n", bid)
				} else {
					fmt.Printf("Bid of %d denied\n", bid)
				}
			}
		}

		if err != nil {
			log.Println(err)
		}
	}
}
