package surfstore

import (
	context "context"
	"fmt"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	NewBlockStoreClientOfConn := NewBlockStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	putBlock, err := NewBlockStoreClientOfConn.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		return err
	}
	*succ = putBlock.Flag
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	NewBlockStoreClientOfConn := NewBlockStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	hasBlocks, err := NewBlockStoreClientOfConn.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = hasBlocks.Hashes

	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	index := 0
	gotConnection := false
	for !gotConnection {

		if index >= len(surfClient.MetaStoreAddrs) {
			return fmt.Errorf("Majority of the cluster crashed")

		}
		conn, err := grpc.Dial(surfClient.MetaStoreAddrs[index], grpc.WithInsecure())
		if err != nil {
			index += 1
			// return err
			continue
		}
		NewRaftSurfStoreClientOfConn := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		get_file_info_map, err := NewRaftSurfStoreClientOfConn.GetFileInfoMap(ctx, &emptypb.Empty{})

		// if err == ERR_SERVER_CRASHED || err == ERR_NOT_LEADER {
		// 	index = (index + 1) % len(surfClient.MetaStoreAddrs)
		// 	continue
		// }
		if err != nil {
			index += 1
			// conn.Close()
			// return err
			continue
		}
		*serverFileInfoMap = get_file_info_map.FileInfoMap
		return conn.Close()
	}
	return fmt.Errorf("error in getting RAFT Leader")
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	index := 0
	gotConnection := false
	for !gotConnection {

		if index >= len(surfClient.MetaStoreAddrs) {
			return fmt.Errorf("Majority of the cluster crashed")

		}
		conn, err := grpc.Dial(surfClient.MetaStoreAddrs[index], grpc.WithInsecure())
		if err != nil {
			index += 1
			// return err
			continue
		}
		NewMetaStoreClientOfConn := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		version_updateFile, err := NewMetaStoreClientOfConn.UpdateFile(ctx, fileMetaData)
		// if err == ERR_SERVER_CRASHED || err == ERR_NOT_LEADER {
		// 	index = (index + 1) % len(surfClient.MetaStoreAddrs)
		// 	continue
		// }
		if err != nil {
			index += 1
			conn.Close()
			// return err
			continue
		}
		*latestVersion = version_updateFile.Version

		return conn.Close()
	}
	return fmt.Errorf("error in getting RAFT Leader")
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	// panic("todo")
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	NewBlockStoreClientOfConn := NewBlockStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	blockhashes, err := NewBlockStoreClientOfConn.GetBlockHashes(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashes = blockhashes.Hashes
	return conn.Close()
}

// func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
// 	conn, err := grpc.Dial(surfclient.MetaStoreAddrs[0], grpc.WithTransportCredentials(insecure.NewCredentials()))
// 	if err != nil {
// 		return err
// 	}
// 	NewMetaStoreClientOfConn := NewMetaStoreClient(conn)

// 	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
// 	defer cancel()
// 	blockAddr, err := NewMetaStoreClientOfConn.GetBlockStoreAddr(ctx, &emptypb.Empty{})
// 	if err != nil {
// 		conn.Close()
// 		return err
// 	}
// 	*blockStoreAddr = blockAddr.Addr
// 	return conn.Close()
// }

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	// panic("todo")
	index := 0
	gotConnection := false
	for !gotConnection {
		if index >= len(surfClient.MetaStoreAddrs) {
			return fmt.Errorf("Majority of the cluster crashed")

		}

		conn, err := grpc.Dial(surfClient.MetaStoreAddrs[index], grpc.WithInsecure())
		if err != nil {
			index += 1
			// return err
			continue
		}
		NewMetaStoreClientOfConn := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		blockHashes, err := NewMetaStoreClientOfConn.GetBlockStoreMap(ctx, &BlockHashes{Hashes: blockHashesIn})
		// if err == ERR_SERVER_CRASHED || err == ERR_NOT_LEADER {
		// 	index = (index + 1) % len(surfClient.MetaStoreAddrs)
		// 	continue
		// }
		if err != nil {
			index += 1
			continue
		}

		tempBlockStoreMap := make(map[string][]string)
		blockHashes_BlockStoreMap := blockHashes.BlockStoreMap
		for eachKey := range blockHashes_BlockStoreMap {
			tempBlockStoreMap[eachKey] = blockHashes_BlockStoreMap[eachKey].Hashes
		}
		*blockStoreMap = tempBlockStoreMap

		// close the connection
		return conn.Close()
	}
	return fmt.Errorf("error in getting RAFT Leader")
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	// panic("todo")
	index := 0
	gotConnection := false

	for !gotConnection {
		conn, err := grpc.Dial(surfClient.MetaStoreAddrs[index], grpc.WithInsecure())
		if err != nil {
			index += 1
			// return err
			continue
		}
		// NewMetaStoreClientOfConn := NewMetaStoreClient(conn)
		NewMetaStoreClientOfConn := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		blockHashes, err := NewMetaStoreClientOfConn.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
		if err != nil {
			index += 1
			// conn.Close()
			// return err
			continue
		}
		*blockStoreAddrs = blockHashes.BlockStoreAddrs
		return conn.Close()
	}
	return fmt.Errorf("error in getting RAFT Leader")
}

var _ ClientInterface = new(RPCClient)

func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
