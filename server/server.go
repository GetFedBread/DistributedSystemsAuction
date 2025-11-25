package main

import (
	proto "auction/grpc"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type AuctionService struct {
	proto.UnimplementedAuctionServer
	mutex           sync.Mutex
	servers         map[int64]proto.AuctionClient
	replicas        []*proto.ReplicaConnection
	leader_id       int64
	id              int64
	next_client     int64
	highest_bid     int64
	highest_bidder  int64
	auction_running bool
	timestamp       int64
}

func main() {
	server := &AuctionService{
		id:              0,
		leader_id:       0,
		timestamp:       0,
		next_client:     0,
		highest_bid:     0,
		highest_bidder:  -1, // -1 means no bidder
		auction_running: false,
		servers:         make(map[int64]proto.AuctionClient),
		replicas:        make([]*proto.ReplicaConnection, 0),
	}

	server.start_server()
}

func (s *AuctionService) start_server() {
	grpc_server := grpc.NewServer()

	var listener net.Listener
	var err error

	for {
		//as long as it can't connect keep increasing port number by 1
		port := fmt.Sprintf(":%d", 8080+s.id)
		listener, err = net.Listen("tcp", port)
		if err == nil {
			break
		}
		//create clients
		conn, err := grpc.NewClient("localhost"+port, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatal(err)
		}
		s.servers[s.id] = proto.NewAuctionClient(conn)
		s.replicas = append(s.replicas, &proto.ReplicaConnection{
			Id:   s.id,
			Port: 8080 + s.id,
		})
		s.id++
	}
	// Setup file logging
	f, err := os.OpenFile(fmt.Sprintf("serverlog%d.txt", s.id), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	log.SetOutput(f)

	if s.id != s.leader_id {
		state, err := s.servers[s.leader_id].ReplicaConnected(context.Background(), &proto.ReplicaConnection{
			Id:   s.id,
			Port: 8080 + s.id,
		})
		if err != nil {
			log.Fatal(err)
		}
		// Setup the replica with the leaders state
		s.UpdateReplica(context.Background(), state)
	}

	proto.RegisterAuctionServer(grpc_server, s)
	log.Println("Server started on " + listener.Addr().String())
	go s.shutdown_logger(grpc_server)
	if s.id == s.leader_id {
		go s.start_auctioning(true, false)
	}
	go s.leader_monitor()
	err = grpc_server.Serve(listener)

	if err != nil {
		log.Fatal(err)
	}
}

func (s *AuctionService) leader_monitor() {
	for {
		// Kill the monitor if this server is the leader
		if s.id == s.leader_id {
			return
		}
		_, err := s.servers[s.leader_id].WellnessCheck(context.Background(), &proto.Empty{})

		// if err != nil then the leader is down, so a new leader must be chosen
		if err != nil {
			log.Println("Leader is gone, starting election")
			s.StartElection(context.Background(), &proto.ReplicaIdentity{
				Id:        s.id,
				Timestamp: s.timestamp,
			})
		}
		time.Sleep(2 * time.Second)
	}
}

func (s *AuctionService) start_auctioning(running bool, keep_values bool) {
	if running {
		log.Println("Auction started")
		s.mutex.Lock()
		s.auction_running = true
		if !keep_values {
			s.highest_bid = 0
			s.highest_bidder = -1
		}
		s.timestamp += 1
		s.mutex.Unlock()
		s.UpdateReplicas()
		time.Sleep(time.Millisecond * time.Duration(10000))
		go s.start_auctioning(false, false)
	} else {
		log.Println("Auction over")
		s.mutex.Lock()
		s.auction_running = false
		s.timestamp += 1
		s.mutex.Unlock()
		s.UpdateReplicas()
		time.Sleep(time.Second * 5)
		go s.start_auctioning(true, false)
	}
}

func (s *AuctionService) shutdown_logger(grpc_server *grpc.Server) {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	<-stop
	log.Printf("Server stopped with logical time stamp %d\n", s.timestamp)
	log.Println("---------------------------------------------------------------")
	grpc_server.GracefulStop()
}

func (s *AuctionService) update_timestamp(timestamp_in int64) {
	s.mutex.Lock()
	if s.timestamp < timestamp_in {
		s.timestamp = timestamp_in
	}
	s.timestamp += 1
	s.mutex.Unlock()
}

func (s *AuctionService) GetClientId(ctx context.Context, _ *proto.Empty) (*proto.Id, error) {
	response := &proto.Id{
		Id: s.next_client,
	}
	s.next_client += 1
	return response, nil
}

func (s *AuctionService) Bid(ctx context.Context, bid *proto.BidAmount) (*proto.BidAck, error) {
	if s.id != s.leader_id {
		log.Println("Propogating request to leader")
		bid.Timestamp = s.timestamp + 1
		return s.servers[s.leader_id].Bid(ctx, bid)
	}
	if !s.auction_running {
		return &proto.BidAck{Accepted: false, ErrorMessage: "Auction not running"}, nil
	}
	s.update_timestamp(bid.Timestamp)

	if s.highest_bid < bid.BidAmount {
		log.Printf("Accepting bid from %d for %d", bid.Bidder, bid.BidAmount)
		s.highest_bid = bid.BidAmount
		s.highest_bidder = bid.Bidder

		s.UpdateReplicas()

		return &proto.BidAck{Accepted: true}, nil
	}
	log.Printf("Declining bid from %d for %d", bid.Bidder, bid.BidAmount)
	return &proto.BidAck{Accepted: false}, nil
}

func (s *AuctionService) Result(ctx context.Context, _ *proto.Empty) (*proto.AuctionResult, error) {
	return &proto.AuctionResult{
		HighestBid:    s.highest_bid,
		HighestBidder: s.highest_bidder,
		AuctionOver:   !s.auction_running,
		Timestamp:     s.timestamp,
	}, nil
}

func (s *AuctionService) GetReplicaList(ctx context.Context, _ *proto.Empty) (*proto.ReplicaList, error) {
	response := &proto.ReplicaList{
		Replicas: []string{},
	}
	s.mutex.Lock()
	for _, replica := range s.replicas {
		response.Replicas = append(response.Replicas, fmt.Sprintf("localhost:%d", replica.Port))
	}
	s.mutex.Unlock()
	return response, nil
}

func (s *AuctionService) ReplicaConnected(ctx context.Context, replica_info *proto.ReplicaConnection) (*proto.ReplicaState, error) {
	if s.id != s.leader_id {
		// Propogate to leader
		return s.servers[s.leader_id].ReplicaConnected(ctx, replica_info)
	}
	_, occupied := s.servers[replica_info.Id]
	if occupied {
		return nil, errors.New("server id already occupied")
	}
	host_url := fmt.Sprintf("localhost:%d", replica_info.Port)
	conn, err := grpc.NewClient(host_url, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	s.mutex.Lock()
	s.servers[replica_info.Id] = proto.NewAuctionClient(conn)
	s.replicas = append(s.replicas, replica_info)
	s.mutex.Unlock()

	s.UpdateReplicas()

	return s.GetState(), nil
}

func (s *AuctionService) UpdateReplica(ctx context.Context, state *proto.ReplicaState) (*proto.Empty, error) {
	s.mutex.Lock()

	log.Println("Updating replica")
	s.auction_running = !state.AuctionFinished
	s.highest_bid = state.HighestBid
	s.highest_bidder = state.HighestBidder
	s.timestamp = state.Identity.Timestamp

	// Add missing replicas
	for _, replica := range state.Replicas {
		_, contains := s.servers[replica.Id]
		if !contains && replica.Id != s.id {
			s.replicas = append(s.replicas, replica)
			port := fmt.Sprintf(":%d", replica.Port)
			conn, err := grpc.NewClient("localhost"+port, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Fatal(err)
			}
			s.servers[replica.Id] = proto.NewAuctionClient(conn)
		}
	}

	s.mutex.Unlock()
	return &proto.Empty{}, nil
}

func (s *AuctionService) StartElection(ctx context.Context, identity *proto.ReplicaIdentity) (*proto.ElectionResponse, error) {
	if identity.Id > s.id {
		return &proto.ElectionResponse{SenderGreater: true}, nil
	}

	go func() {
		// Find potential candidates greater than this server
		s.mutex.Lock()
		var candidates []proto.AuctionClient
		for id, client := range s.servers {
			if id > s.id {
				candidates = append(candidates, client)
			}
		}
		s.mutex.Unlock()

		// Check those candidates
		for _, candidate := range candidates {
			_, err := candidate.StartElection(context.Background(), &proto.ReplicaIdentity{
				Id:        s.id,
				Timestamp: s.timestamp,
			})
			if err == nil {
				// A better candidate will handle the election
				return
			}
		}

		// This replica won the election
		s.leader_id = s.id

		// Find newest state amongst the replicas and use that state
		newest_state := s.GetState()
		for id, server := range s.servers {
			state, err := server.ElectionFinished(context.Background(), &proto.Id{Id: s.id})
			if err != nil {
				log.Printf("No response from %d during election\n", id)
				continue
			}

			if state.Identity.Timestamp > newest_state.Identity.Timestamp {
				newest_state = state
			}
		}
		s.UpdateReplica(context.Background(), newest_state)

		// Re-startup the auction if it was running
		go s.start_auctioning(s.auction_running, true)

		s.UpdateReplicas()
	}()
	return &proto.ElectionResponse{SenderGreater: identity.Id > s.id}, nil
}

func (s *AuctionService) ElectionFinished(ctx context.Context, leader *proto.Id) (*proto.ReplicaState, error) {
	s.leader_id = leader.Id
	return s.GetState(), nil
}

func (s *AuctionService) WellnessCheck(ctx context.Context, _ *proto.Empty) (*proto.Empty, error) {
	return &proto.Empty{}, nil
}

func (s *AuctionService) GetState() *proto.ReplicaState {
	s.mutex.Lock()
	state := &proto.ReplicaState{
		Identity: &proto.ReplicaIdentity{
			Id:        s.id,
			Timestamp: s.timestamp,
		},
		Replicas:        s.replicas,
		HighestBidder:   s.highest_bidder,
		HighestBid:      s.highest_bid,
		AuctionFinished: !s.auction_running,
	}
	s.mutex.Unlock()
	return state
}

func (s *AuctionService) UpdateReplicas() {
	log.Println("Updating replicas")
	state := s.GetState()
	for _, server := range s.servers {
		go server.UpdateReplica(context.Background(), state)
	}
}
