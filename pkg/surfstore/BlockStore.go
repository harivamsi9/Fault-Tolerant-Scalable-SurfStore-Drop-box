package surfstore

import (
	context "context"
	"fmt"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
	smtx sync.Mutex
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	bs.smtx.Lock()
	block_obj, ok := bs.BlockMap[blockHash.Hash]
	if !ok {
		return nil, fmt.Errorf("BlockNotFoundOfHash: %v", blockHash.Hash)
	}

	bs.smtx.Unlock()
	return block_obj, nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block_obj *Block) (*Success, error) {
	hashBlock := GetBlockHashString(block_obj.BlockData)
	bs.smtx.Lock()
	bs.BlockMap[hashBlock] = block_obj
	bs.smtx.Unlock()
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
// checkcheck
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	var hashes []string
	for _, eachHash := range blockHashesIn.Hashes {
		bs.smtx.Lock()
		if _, ok := bs.BlockMap[eachHash]; ok {
			hashes = append(hashes, eachHash)
		}
		bs.smtx.Unlock()
	}
	return &BlockHashes{Hashes: hashes}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}

// TODO: Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	// panic("todo")
	bs.smtx.Lock()
	// defer bs.smtx.Unlock()

	// Create a new BlockHashes object
	blockHashesObj := &BlockHashes{Hashes: []string{}}
	// hashes := make([]string, len(bs.BlockMap))
	// copy(hashes, bs.blockHashes)

	for eachKey := range bs.BlockMap {
		blockHashesObj.Hashes = append(blockHashesObj.Hashes, eachKey)
	}
	bs.smtx.Unlock()
	return blockHashesObj, nil
}
