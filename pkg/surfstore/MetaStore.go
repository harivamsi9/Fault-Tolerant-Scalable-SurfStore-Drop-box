package surfstore

import (
	context "context"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
	sMtx sync.Mutex
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	file_name := fileMetaData.Filename
	version := fileMetaData.Version
	m.sMtx.Lock()
	_, flag := m.FileMetaMap[file_name]
	if flag {
		check_ver := m.FileMetaMap[file_name].Version + 1
		if version == check_ver {
			m.FileMetaMap[file_name] = fileMetaData
		} else {
			version = -1
		}
	} else {
		m.FileMetaMap[file_name] = fileMetaData
	}
	m.sMtx.Unlock()
	return &Version{Version: version}, nil
}

var _ MetaStoreInterface = new(MetaStore)

// func NewMetaStore(blockStoreAddr string) *MetaStore {
// 	return &MetaStore{
// 		FileMetaMap:    map[string]*FileMetaData{},
// 		BlockStoreAddr: blockStoreAddr,
// 	}
// }
// func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
// 	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
// }

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}

// func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
// 	m.sMtx.Lock()
// 	defer m.sMtx.Unlock()

// 	blockHashes := blockHashesIn.Hashes
// 	if len(blockHashes) == 0 {
// 		return nil, errors.New("no block hashes provided")
// 	}

// 	//     // Create a map to store the block store addresses for each hash
// 	blockStoreAddrMap := make(map[string][]string)

// 	//     for _, hash := range blockHashes {
// 	//         // Find the block store address for the hash using the consistent hash ring
// 	//         blockStoreAddr, err := m.ConsistentHashRing.GetNode(hash)
// 	//         if err != nil {
// 	//             return nil, err
// 	//         }

// 	//         // Add the block store address to the map
// 	//         blockStoreAddrMap[hash] = []string{blockStoreAddr}
// 	//     }

// 	// return &BlockStoreMap{BlockStoreAddrMap: blockStoreAddrMap}, nil
// }

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	// panic("todo")
	m.sMtx.Lock()
	// defer m.sMtx.Unlock()
	blockStoreMap := make(map[string]*BlockHashes)
	blockHashes := blockHashesIn.Hashes

	for _, hash := range blockHashes {
		responsibleServer, err := m.ConsistentHashRing.GetResponsibleServer(hash)
		if err != nil {
			return nil, err
		}
		_, isOk := blockStoreMap[responsibleServer]
		if isOk {
			blockStoreMap[responsibleServer].Hashes = append(blockStoreMap[responsibleServer].Hashes, hash)
		} else {
			blockStoreMap[responsibleServer] = &BlockHashes{}
			blockStoreMap[responsibleServer].Hashes = []string{hash}
		}

	}
	m.sMtx.Unlock()
	return &BlockStoreMap{BlockStoreMap: blockStoreMap}, nil

}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	// panic("todo")
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil

}

// func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {

// 	blockStoreMap := make(map[string]*BlockHashes)
// 	blockHashes := blockHashesIn.Hashes

// 	for _, hash := range blockHashes {
// 		responsibleServer, err := m.ConsistentHashRing.GetResponsibleServer(hash)
// 		if err != nil {
// 			return nil, err
// 		}
// 		_, isOk := blockStoreMap[responsibleServer]

// 		if isOk {
// 			blockStoreMap[responsibleServer].Hashes = append(blockStoreMap[responsibleServer].Hashes, hash)
// 		} else {
// 			blockStoreMap[responsibleServer] = &BlockHashes{}
// 			blockStoreMap[responsibleServer].Hashes = []string{hash}
// 		}

// 	}

// 	return &BlockStoreMap{BlockStoreMap: blockStoreMap}, nil
// }

// func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
//     m.sMtx.Lock()
//     defer m.sMtx.Unlock()

//     addrs := make([]string, len(m.BlockStoreAddrs))
//     copy(addrs, m.BlockStoreAddrs)

//     return &BlockStoreAddrs{Addrs: addrs}, nil
// }
