package config

import (
	"net"
	"net/netip"
	"strconv"
	"strings"
)

// listenAddressesOverlap reports whether two host:port listen addresses can
// not both be bound: the same port (port 0 is "any free port" and never
// collides) and hosts that share an interface. A wildcard host (empty,
// 0.0.0.0, ::) overlaps every host; the name localhost overlaps the loopback
// addresses; an IPv4-mapped IPv6 address is its IPv4
// address; any other host compares by its lower-cased spelling. It judges what
// the socket would bind, not the string, so ":8010" and "127.0.0.1:8010"
// collide. An address that is not host:port never overlaps (the caller has
// already refused it).
func listenAddressesOverlap(a, b string) bool {
	hostA, portA, okA := splitListen(a)
	hostB, portB, okB := splitListen(b)
	if !okA || !okB || portA != portB || portA == "0" {
		return false
	}
	if isWildcardHost(hostA) || isWildcardHost(hostB) {
		return true
	}
	// "localhost" resolves to a loopback address (either family): it overlaps
	// every loopback address and itself; two distinct addresses never do.
	if (hostA == "localhost" && isLoopbackName(hostB)) || (hostB == "localhost" && isLoopbackName(hostA)) {
		return true
	}
	return hostA == hostB
}

func splitListen(address string) (host, port string, ok bool) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", "", false
	}
	if number, err := strconv.ParseUint(port, 10, 16); err == nil {
		port = strconv.FormatUint(number, 10)
	}
	host = strings.ToLower(host)
	if zone := strings.IndexByte(host, '%'); zone >= 0 {
		host = host[:zone]
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		host = ip.Unmap().String()
	}
	return host, port, true
}

func isWildcardHost(host string) bool {
	if host == "" {
		return true
	}
	ip, err := netip.ParseAddr(host)
	return err == nil && ip.IsUnspecified()
}

func isLoopbackName(host string) bool {
	if host == "localhost" {
		return true
	}
	ip, err := netip.ParseAddr(host)
	return err == nil && ip.IsLoopback()
}
