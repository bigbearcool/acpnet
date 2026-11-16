package main

import (
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"
)

// validateClientLocalMCPTarget accepts only literal client loopback addresses.
// It deliberately rejects DNS names, implicit ports, credentials and fragments.
// Validation alone does not create or authorize a forwarding connection.
func validateClientLocalMCPTarget(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Opaque != "" || u.User != nil || u.Fragment != "" {
		return errors.New("invalid client-local MCP target")
	}
	host := u.Hostname()
	ip := net.ParseIP(host)
	if ip == nil || (host != "127.0.0.1" && host != "::1") {
		return errors.New("MCP target must use a literal loopback address")
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil || port < 1 || port > 65535 {
		return errors.New("MCP target requires a valid explicit port")
	}
	return nil
}

// isClientLocalMCPURL recognizes loopback/unspecified IPs, localhost names,
// and ambiguous numeric IPv4 forms without performing DNS resolution. Other
// names are passed through even if they resolve to the client loopback.
func isClientLocalMCPURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u == nil {
		return false
	}
	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	if host == "" {
		return false
	}
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return true
	}
	ip := net.ParseIP(host)
	if ip == nil {
		// Non-canonical numeric IPv4 forms are resolver-dependent. Reject all
		// of them rather than pass a possible loopback address to the host.
		parts := strings.Split(host, ".")
		if len(parts) > 4 {
			return false
		}
		for _, part := range parts {
			if part == "" {
				return false
			}
			if _, err := strconv.ParseUint(part, 0, 32); err != nil {
				return false
			}
		}
		return true
	}
	return ip.IsLoopback() || ip.IsUnspecified()
}
