package discovery

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
)

// Discover will use the created settings to scan for LAN peers. It will return
// an array of the discovered peers and their associate payloads. It will not
// return broadcasts sent to itself.
func Discover(settings ...Settings) (discoveries []Discovered, err error) {
	s := Settings{}
	if len(settings) > 0 {
		s = settings[0]
	}
	p, err := NewPeerDiscovery(s)
	if err != nil {
		return
	}

	p.RLock()
	address := net.JoinHostPort(p.settings.MulticastAddress, p.settings.Port)
	portNum := p.settings.portNum
	tickerDuration := p.settings.Delay
	timeLimit := p.settings.TimeLimit
	p.RUnlock()

	ifaces, err := filterInterfaces(p.settings.IPVersion == IPv4)
	if err != nil {
		return
	}
	if len(ifaces) == 0 {
		err = fmt.Errorf("no available multicast interface found")
		return
	}

	// Open up an UDP connection
	c, err := net.ListenPacket(fmt.Sprintf("udp%d", p.settings.IPVersion), address)
	if err != nil {
		return
	}
	defer c.Close()

	group := p.settings.multicastAddressNumbers

	// ipv{4,6} have an own PacketConn, which does not implement net.PacketConn
	var broadcastConn NetPacketConn
	if p.settings.IPVersion == IPv4 {
		broadcastConn = PacketConn4{ipv4.NewPacketConn(c)}
	} else {
		broadcastConn = PacketConn6{ipv6.NewPacketConn(c)}
	}

	for i := range ifaces {
		broadcastConn.JoinGroup(&ifaces[i], &net.UDPAddr{IP: group, Port: portNum})
	}

	go func() {
		handleSignals()

		if !s.DisableBroadcast {
			payload := []byte("Leave")
			if p.settings.PayloadFunc != nil {
				payload = p.settings.PayloadFunc()
			}
			// write to multicast
			broadcast(broadcastConn, payload, ifaces, &net.UDPAddr{IP: group, Port: portNum})
		}

		os.Exit(0)
	}()
	go p.listen() // Open udp listener for incoming broadcast message
	go func() {
		for {
			time.Sleep(3 * time.Second)
			p.Lock()
			fmt.Println("{List of peers:}")
			fmt.Println(p.peers)
			p.Unlock()
		}
	}()

	ticker := time.NewTicker(tickerDuration)
	defer ticker.Stop()
	start := time.Now()

	for {
		p.RLock()
		// reach max. discover limit
		if len(p.received) >= p.settings.Limit && p.settings.Limit > 0 {
			p.exit = true
		}
		p.RUnlock()

		if !s.DisableBroadcast {
			payload := p.settings.Payload
			if p.settings.PayloadFunc != nil {
				payload = p.settings.PayloadFunc()
			}
			// write to multicast
			broadcast(broadcastConn, payload, ifaces, &net.UDPAddr{IP: group, Port: portNum})
		}

		select {
		case <-p.settings.StopChan:
			p.exit = true
		case <-ticker.C:
		}

		if p.exit || timeLimit > 0 && time.Since(start) > timeLimit {
			if !s.DisableBroadcast {
				payload := []byte("Leave")
				if p.settings.PayloadFunc != nil {
					payload = p.settings.PayloadFunc()
				}
				// write to multicast
				broadcast(broadcastConn, payload, ifaces, &net.UDPAddr{IP: group, Port: portNum})
			}
			break
		}
	}

	/*
		if !s.DisableBroadcast {
			payload := p.settings.Payload
			if p.settings.PayloadFunc != nil {
				payload = p.settings.PayloadFunc()
			}
			// send out broadcast that is finished
			broadcast(broadcastConn, payload, ifaces, &net.UDPAddr{IP: group, Port: portNum})
		}
	*/

	p.RLock()
	discoveries = make([]Discovered, len(p.received))
	i := 0
	for ip, payload := range p.received {
		discoveries[i] = Discovered{
			Address: ip,
			Payload: payload,
		}
		i++
	}
	p.RUnlock()
	return
}

// peerDiscovery is the object that can do the discovery for finding LAN peers.
type peerDiscovery struct {
	settings Settings

	received map[string][]byte
	peers    []string
	sync.RWMutex
	exit bool
}

// NewPeerDiscovery returns a new peerDiscovery object which can be used to discover peers.
// The settings are optional. If any setting is not supplied, then defaults are used.
// See the Settings for more information.
func NewPeerDiscovery(settings Settings) (p *peerDiscovery, err error) {
	p = new(peerDiscovery)
	p.Lock()
	defer p.Unlock()

	// NewPeerDiscovery settings
	p.settings = settings

	// defaults
	if p.settings.Port == "" {
		p.settings.Port = "9999"
	}
	if p.settings.IPVersion == 0 {
		p.settings.IPVersion = IPv4
	}
	if p.settings.MulticastAddress == "" {
		if p.settings.IPVersion == IPv4 {
			p.settings.MulticastAddress = "239.255.255.250"
		} else {
			p.settings.MulticastAddress = "ff02::c"
		}
	}
	if len(p.settings.Payload) == 0 {
		p.settings.Payload = []byte("hi")
	}
	if p.settings.Delay == 0 {
		p.settings.Delay = 1 * time.Second
	}
	if p.settings.TimeLimit == 0 {
		p.settings.TimeLimit = 10 * time.Second
	}
	if p.settings.StopChan == nil {
		p.settings.StopChan = make(chan struct{})
	}
	p.received = make(map[string][]byte)
	p.settings.multicastAddressNumbers = net.ParseIP(p.settings.MulticastAddress)
	if p.settings.multicastAddressNumbers == nil {
		err = fmt.Errorf("Multicast Address %s could not be converted to an IP",
			p.settings.MulticastAddress)
		return
	}
	p.settings.portNum, err = strconv.Atoi(p.settings.Port)
	if err != nil {
		return
	}

	if p.settings.Notify == nil {
		p.settings.Notify = func(d Discovered) {
			fmt.Printf("[Broadcast] Received message %s from %s\n", d.Payload, d.Address)
		}
	}
	return
}

type NetPacketConn interface {
	JoinGroup(ifi *net.Interface, group net.Addr) error
	SetMulticastInterface(ini *net.Interface) error
	SetMulticastTTL(int) error
	ReadFrom(buf []byte) (int, net.Addr, error)
	WriteTo(buf []byte, dst net.Addr) (int, error)
}

// filterInterfaces returns a list of valid network interfaces
func filterInterfaces(ipv4 bool) (ifaces []net.Interface, err error) {
	allIfaces, err := net.Interfaces()
	if err != nil {
		return
	}
	ifaces = make([]net.Interface, 0, len(allIfaces))
	for i := range allIfaces {
		iface := allIfaces[i]
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagBroadcast == 0 {
			// interface is down or does not support broadcasting
			continue
		}
		addrs, _ := iface.Addrs()
		supported := false
		for j := range addrs {
			addr := addrs[j].(*net.IPNet)
			if addr == nil || addr.IP == nil {
				continue
			}
			isv4 := addr.IP.To4() != nil
			if isv4 == ipv4 {
				// IP family matches, go on and use interface
				supported = true
				break
			}
		}
		if supported {
			ifaces = append(ifaces, iface)
		}
	}
	return
}

func broadcast(broadcastConn NetPacketConn, payload []byte, ifaces []net.Interface, dst net.Addr) {
	for i := range ifaces {
		if errMulticast := broadcastConn.SetMulticastInterface(&ifaces[i]); errMulticast != nil {
			continue
		}
		broadcastConn.SetMulticastTTL(2)
		if _, errMulticast := broadcastConn.WriteTo([]byte(payload), dst); errMulticast != nil {
			continue
		}
	}
}

const (
	// https://en.wikipedia.org/wiki/User_Datagram_Protocol#Packet_structure
	maxDatagramSize = 66507
)

// Listen binds to the UDP address and port given and writes packets received
// from that address to a buffer which is passed to a hander
func (p *peerDiscovery) listen() (recievedBytes []byte, err error) {
	p.RLock()
	address := net.JoinHostPort(p.settings.MulticastAddress, p.settings.Port)
	portNum := p.settings.portNum
	allowSelf := p.settings.AllowSelf
	timeLimit := p.settings.TimeLimit
	p.RUnlock()
	localIPs := getLocalIPs()

	// get interfaces
	ifaces, err := net.Interfaces()
	if err != nil {
		return
	}
	// log.Println(ifaces)

	// Open up a connection
	c, err := net.ListenPacket(fmt.Sprintf("udp%d", p.settings.IPVersion), address)
	if err != nil {
		return
	}
	defer c.Close()

	group := p.settings.multicastAddressNumbers
	var broadcastConn NetPacketConn
	if p.settings.IPVersion == IPv4 {
		broadcastConn = PacketConn4{ipv4.NewPacketConn(c)}
	} else {
		broadcastConn = PacketConn6{ipv6.NewPacketConn(c)}
	}

	for i := range ifaces {
		broadcastConn.JoinGroup(&ifaces[i], &net.UDPAddr{IP: group, Port: portNum})
	}

	start := time.Now()
	// Loop forever reading from the socket
	for {
		buffer := make([]byte, maxDatagramSize)
		var (
			n       int
			src     net.Addr
			errRead error
		)
		n, src, errRead = broadcastConn.ReadFrom(buffer)
		if errRead != nil {
			err = errRead
			return
		}

		srcHost, _, _ := net.SplitHostPort(src.String())

		if _, ok := localIPs[srcHost]; ok && !allowSelf {
			continue
		}

		p.Lock()
		payload := buffer[:n]
		if _, ok := p.received[srcHost]; !ok {
			p.received[srcHost] = payload
		}

		if string(payload) == "Live" && !contains(p.peers, srcHost) {
			p.peers = append(p.peers, srcHost)
			log.Printf("New peer %s joins!\n", srcHost)
		} else if string(payload) == "Leave" && contains(p.peers, srcHost) {
			p.peers, err = remove(p.peers, srcHost)
			if err != nil {
				log.Println(err)
			} else {
				log.Printf("Peer %s left!\n", srcHost)
			}
		}
		p.Unlock()

		/*
			if p.settings.Notify != nil {
				p.settings.Notify(Discovered{
					Address: srcHost,
					Payload: buffer[:n],
				})
			}
		*/

		p.RLock()
		if len(p.received) >= p.settings.Limit && p.settings.Limit > 0 {
			p.RUnlock()
			break
		}
		if p.exit || timeLimit > 0 && time.Since(start) > timeLimit {
			p.RUnlock()
			break
		}
		p.RUnlock()
	}

	return
}

// getLocalIPs returns the local ip address
func getLocalIPs() (ips map[string]struct{}) {
	ips = make(map[string]struct{})
	ips["localhost"] = struct{}{}
	ips["127.0.0.1"] = struct{}{}
	ips["::1"] = struct{}{}

	ifaces, err := net.Interfaces()
	if err != nil {
		return
	}

	for _, iface := range ifaces {
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, address := range addrs {
			ip, _, err := net.ParseCIDR(address.String())
			if err != nil {
				// log.Printf("Failed to parse %s: %v", address.String(), err)
				continue
			}

			ips[ip.String()+"%"+iface.Name] = struct{}{}
			ips[ip.String()] = struct{}{}
		}
	}
	return
}

type PacketConn4 struct {
	*ipv4.PacketConn
}

// ReadFrom wraps the ipv4 ReadFrom without a control message
func (pc4 PacketConn4) ReadFrom(buf []byte) (int, net.Addr, error) {
	n, _, addr, err := pc4.PacketConn.ReadFrom(buf)
	return n, addr, err
}

// WriteTo wraps the ipv4 WriteTo without a control message
func (pc4 PacketConn4) WriteTo(buf []byte, dst net.Addr) (int, error) {
	return pc4.PacketConn.WriteTo(buf, nil, dst)
}

type PacketConn6 struct {
	*ipv6.PacketConn
}

// ReadFrom wraps the ipv6 ReadFrom without a control message
func (pc6 PacketConn6) ReadFrom(buf []byte) (int, net.Addr, error) {
	n, _, addr, err := pc6.PacketConn.ReadFrom(buf)
	return n, addr, err
}

// WriteTo wraps the ipv6 WriteTo without a control message
func (pc6 PacketConn6) WriteTo(buf []byte, dst net.Addr) (int, error) {
	return pc6.PacketConn.WriteTo(buf, nil, dst)
}

// SetMulticastTTL wraps the hop limit of ipv6
func (pc6 PacketConn6) SetMulticastTTL(i int) error {
	return pc6.SetMulticastHopLimit(i)
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	log.Println("signal received!")
}

func remove(s []string, i string) ([]string, error) {
	for idx, e := range s {
		if e == i {
			s[idx] = s[len(s)-1]
			return s[:len(s)-1], nil
		}
	}
	return nil, errors.New("Do not find target string")
}

// Discovered is the structure of the discovered peers,
// which holds their local address (port removed) and
// a payload if there is one.
type Discovered struct {
	// Address is the local address of a discovered peer.
	Address string
	// Payload is the associated payload from discovered peer.
	Payload []byte
}

func (d Discovered) String() string {
	return fmt.Sprintf("address: %s, payload: %s", d.Address, d.Payload)
}
