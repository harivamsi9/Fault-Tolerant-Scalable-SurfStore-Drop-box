package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) (string, error) {
	// panic("todo")
	responsibleServer := ""
	var serverHashes []string
	ConsistentHR := c.ServerMap
	for eachServerHashKey := range ConsistentHR {
		serverHashes = append(serverHashes, eachServerHashKey)
	}
	sort.Strings(serverHashes)
	//  now we need to find the first server with larger hash value than blockHash (blockId)
	// for _, dat :=
	for i := 0; i < len(serverHashes); i++ {
		if serverHashes[i] > blockId {
			responsibleServer = ConsistentHR[serverHashes[i]]
			break
		}
	}
	if responsibleServer == "" {
		responsibleServer = ConsistentHR[serverHashes[0]]
	}
	return responsibleServer, nil
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	// panic("todo")
	c := ConsistentHashRing{}
	c.ServerMap = make(map[string]string)

	for _, eachServerAddr := range serverAddrs {
		serverHash := c.Hash("blockstore" + eachServerAddr)
		c.ServerMap[serverHash] = eachServerAddr
	}

	return &c

}
