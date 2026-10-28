package main

import "testing"

func TestIsLoopback(t *testing.T) {
	for addr, want := range map[string]bool{
		"127.0.0.1:7070": true, "127.1.2.3:1": true, "localhost:7070": true, "[::1]:7070": true,
		":7070": false, "0.0.0.0:7070": false, "[::]:7070": false, "10.0.0.5:7070": false,
		"example.com:7070": false, "192.168.1.1": false,
	} {
		if got := isLoopback(addr); got != want {
			t.Errorf("isLoopback(%q) = %v, want %v", addr, got, want)
		}
	}
}
