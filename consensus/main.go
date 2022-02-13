package main

import (
	. "consensus/raft"
	"net"
)

func main() {
	ready := make(chan interface{})
	server := NewServer(22, []int{33}, ready)
	server.Serve()

	server.ConnectToPeer(33, net.JoinHostPort("192.168.0.208", "8888"))
	//server.ConnectToPeer(44, net.JoinHostPort("192.168.0.208", "8888"))

	ready <- t
}
