package discovery

import (
	"net"
	"time"
)

// Settings are the settings that can be specified for
// doing peer discovery.
type Settings struct {
	// Limit is the number of peers to discover, use < 1 for unlimited.
	Limit int
	// Port is the port to broadcast on (the peers must also broadcast using the same port).
	// The default port is 9999.
	Port string
	// MulticastAddress specifies the multicast address.
	// You should be able to use any of 224.0.0.0/4 or ff00::/8.
	// By default it uses the Simple Service Discovery Protocol
	// address (239.255.255.250 for IPv4 or ff02::c for IPv6).
	MulticastAddress string
	// Payload is the bytes that are sent out with each broadcast. Must be short.
	Payload []byte
	// PayloadFunc is the function that will be called to dynamically generate payload
	// before every broadcast. If this pointer is nil `Payload` field will be broadcasted instead.
	PayloadFunc func() []byte
	// Delay is the amount of time between broadcasts. The default delay is 1 second.
	Delay time.Duration
	// TimeLimit is the amount of time to spend discovering, if the limit is not reached.
	// A negative limit indiciates scanning until the limit was reached or, if an
	// unlimited scanning was requested, no timeout.
	// The default time limit is 10 seconds.
	TimeLimit time.Duration
	// StopChan is a channel to stop the peer discvoery immediatley after reception.
	StopChan chan struct{}
	// AllowSelf will allow discovery the local machine (default false)
	AllowSelf bool
	// DisableBroadcast will not allow sending out a broadcast
	DisableBroadcast bool
	// IPVersion specifies the version of the Internet Protocol (default IPv4)
	IPVersion IPVersion
	// Notify will be called each time a new peer was discovered.
	// The default is nil, which means no notification whatsoever.
	Notify func(Discovered)

	portNum                 int
	multicastAddressNumbers net.IP
}

// IPVersion specifies the version of the Internet Protocol to be used.
type IPVersion uint

const (
	IPv4 IPVersion = 4
	IPv6 IPVersion = 6
)
