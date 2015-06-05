/*
 * SSL session pool using in-memory or memcache backend.
 *
 * The in-memory backend has no limit on memory consumption and is useful only
 * for demonstration or testing SSL resumption on a single server.
 * 
 * The memcache backend supports multiple memcache pools (presumably in different
 * datacenters).  It works by first checking the local DC pool; on a miss in that
 * pool, it then makes a cross-DC lookup to the remote DC pool; on a hit, it will
 * add it into the local DC pool.  We never write to the remote datacenter.
 *
 * For now, it's only useful for two datacenters as it proceeds to check the remote
 * datacenters serially.  It should be extended to handle each memcache client/pool
 * in a separate goroutine to easily do parallel queries to the remote DCs
 */
package sslsessionpool

import (
	"bytes"
	"crypto/rand"
	"encoding/gob"
	"encoding/hex"
	"github.com/bradfitz/gomemcache/memcache"
	"sync"
	"time"
	tls "github.com/elorimer/gotls"
)

/*
 * The ServerSessionCache interface has a GenerateId method that is used when
 * generating new IDs.  We don't need anything special, just random bytes
 */
func GenericGenerateId() (sessionId []byte, err error) {
	sessionId = make([]byte, 32)
	_, err = rand.Read(sessionId)
	return
}

/*
 * tls.ServerSessionState implements GobEncode and GobDecode for serialization.
 * We need this in multiple places so it's put in a function
 */
func deserializeSession(encoded []byte) (session *tls.ServerSessionState, err error) {
	buffer := bytes.NewBuffer(encoded)
	obj := new(tls.ServerSessionState)
	decoder := gob.NewDecoder(buffer)
	if err := decoder.Decode(obj); err == nil {
		return obj, nil
	}

	return nil, err
}


/*
 * In-memory session pool.  Mostly just for testing.  Uses a map for storage so
 * unbounded growth (no expiring sessions).
 */
type MemorySessionPool struct {
	lock		sync.Mutex				// map operations are not atomic
	data		map[string]tls.ServerSessionState
}

func NewMemorySessionPool() *MemorySessionPool {
	return &MemorySessionPool { data: make(map[string]tls.ServerSessionState) }
}

func (cache *MemorySessionPool) Get(sessionId []byte) (*tls.ServerSessionState, bool) {
	cache.lock.Lock()
	defer cache.lock.Unlock()

	sessionIdStr := hex.EncodeToString(sessionId)

	if state, found := cache.data[sessionIdStr]; found {
		return &state, true
	}

	return nil, false
}

func (cache *MemorySessionPool) Put(sessionId []byte, sessionState *tls.ServerSessionState) {
	cache.lock.Lock()
	defer cache.lock.Unlock()

	sessionIdStr := hex.EncodeToString(sessionId)
	cache.data[sessionIdStr] = *sessionState
}

func (cache *MemorySessionPool) GenerateId() (sessionId []byte, err error) {
	return GenericGenerateId()
}


/*
 * Memcache backed session pool
 *
 */

// a pool is just a list of hostname:port strings (we don't handle weights)
type ServerPool		[]string
type LogFuncT		func (string, ...interface{})

type MemcacheSessionPool struct {
	// From the docs: "Client is a memcache client. It is safe for unlocked use by
	// multiple concurrent goroutines."
	local			*memcache.Client		// local DC memcache pool
	remotes			[]*memcache.Client		// remote DC memcache pools
	itemDuration	int32					// seconds
	// logging interface is very basic.
	log				LogFuncT
}

// Timeouts for network operations.  This is the connection timeout as well as
// the read/write timeout
const (
	LocalDCTimeout	= 10 * time.Millisecond		// timeout for I/O operations in the Local DC
	RemoteDCTimeout	= 25 * time.Millisecond		// timeout for I/O operations in the remote DCs
)

// duration is the session expiration in seconds
func NewMemcacheSessionPool(localPool ServerPool, remotePools []ServerPool, duration int32, log LogFuncT) *MemcacheSessionPool {
	localClient := memcache.New(localPool...)
	localClient.Timeout = LocalDCTimeout
	remoteClients := make([]*memcache.Client, 0)
	for _, p := range remotePools {
		c := memcache.New(p...)
		c.Timeout = RemoteDCTimeout
		remoteClients = append(remoteClients, c)
	}

	if log == nil {
		log = func (string, ...interface{}) { }
	}
	return &MemcacheSessionPool { local: localClient, remotes: remoteClients,
								  itemDuration: duration, log: log }
}

func (cache *MemcacheSessionPool) Get(sessionId []byte) (*tls.ServerSessionState, bool) {
	sessionIdStr := hex.EncodeToString(sessionId)
	cache.log("GET key (local): %s", sessionIdStr)
	/* First check the local datacenter cache. */
	data, err := cache.local.Get(sessionIdStr)
	if err == nil {
		// found it; decode and done
		session, err := deserializeSession(data.Value)
		if err != nil {
			cache.log("session cache deserialize failed: " + err.Error())
			return nil, false
		}
		cache.log("session cache hit.")
		return session, true
	}

	/* Not found in the local datacenter cache; check the remote datacenters
	 * in parallel */
	cache.log("session not found in local DC: " + err.Error())
	for i, remote := range cache.remotes {
		cache.log("checking remote session pool #%d", i)
		data, err := remote.Get(sessionIdStr)
		if err == nil {
			// found it; decode and add it to our local datacenter
			session, err := deserializeSession(data.Value)
			if err != nil {
				cache.log("session cache deserialize failed: " + err.Error())
				return nil, false
			}
			cache.local.Set(&memcache.Item{Key: sessionIdStr, Value: data.Value})
			return session, true
		}
		// Not found, try the next datacenter
	}

	cache.log("session not found")
	return nil, false
}

func (cache *MemcacheSessionPool) Put(sessionId []byte, sessionState *tls.ServerSessionState) {
	sessionIdStr := hex.EncodeToString(sessionId)
	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	if err := encoder.Encode(sessionState); err != nil {
		cache.log("failed to serialize session: " + err.Error())
		return
	}

	cache.log("inserting into session cache %s (%d bytes)", sessionIdStr, len(buffer.Bytes()))
	err := cache.local.Set(&memcache.Item{Key: sessionIdStr, Value: buffer.Bytes(),
	                               Expiration: cache.itemDuration})
	if err != nil {
		cache.log("error inserting: " + err.Error())
	}
}

func (cache *MemcacheSessionPool) GenerateId() (sessionId []byte, err error) {
	return GenericGenerateId()
}
