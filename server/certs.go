package server

import (
	"context"
	"crypto/tls"
	"sync"
	"sync/atomic"
	"time"
)

// Certs serves a TLS key pair from files and reloads it when they change,
// so a renewed certificate needs no restart. A bad pair (mismatched or
// half-written) is refused and the previous one stays.
type Certs struct {
	cert, key string
	cur       atomic.Pointer[tls.Certificate]

	mu     sync.Mutex
	stamps [2]fileStamp
}

// LoadCerts loads the pair once.
func LoadCerts(certFile, keyFile string) (*Certs, error) {
	c := &Certs{cert: certFile, key: keyFile}
	if _, err := c.Reload(); err != nil {
		return nil, err
	}
	return c, nil
}

// Reload re-reads the pair if either file changed.
func (c *Certs) Reload() (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	a, err := stampOf(c.cert)
	if err != nil {
		return false, err
	}
	b, err := stampOf(c.key)
	if err != nil {
		return false, err
	}
	if c.cur.Load() != nil && [2]fileStamp{a, b} == c.stamps {
		return false, nil
	}
	kp, err := tls.LoadX509KeyPair(c.cert, c.key)
	if err != nil {
		return false, err
	}
	c.cur.Store(&kp)
	c.stamps = [2]fileStamp{a, b}
	return true, nil
}

// GetCertificate is for tls.Config.GetCertificate.
func (c *Certs) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	return c.cur.Load(), nil
}

// Watch reloads every interval until ctx ends.
func (c *Certs) Watch(ctx context.Context, every time.Duration, logf func(string, ...any)) {
	watch(ctx, every, func() {
		if ok, err := c.Reload(); err != nil {
			logf("freshdb: TLS certificate: %v (keeping the previous one)", err)
		} else if ok {
			logf("freshdb: TLS certificate reloaded")
		}
	})
}
