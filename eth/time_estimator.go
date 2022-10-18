package eth

import "sync"

type pivotBlock struct {
	time         uint64
	timePerBlock uint64
}
type TimeEstimator struct {
	client      *ClientPool
	gapSize     uint64
	pivotBlocks map[uint64]pivotBlock
	mu          sync.RWMutex
}

//Note: blockchain should have at least gapSize blocks already
func NewTimeEstimator(client *ClientPool, gapSize uint64) *TimeEstimator {
	return &TimeEstimator{
		client:      client,
		gapSize:     gapSize,
		pivotBlocks: map[uint64]pivotBlock{},
	}
}

const nearbyGap = uint64(20)

//blocks generation below this block will quite uncertain in frequency
const minBlock = uint64(200)

func (te *TimeEstimator) getPivotBlock(blockNumber uint64) uint64 {
	pivotBlock := blockNumber - blockNumber%te.gapSize
	if pivotBlock < minBlock {
		pivotBlock = minBlock
	}
	return pivotBlock
}

func (te *TimeEstimator) BlockTime(blockNumber uint64) uint64 {
	if blockNumber <= minBlock {
		return te.client.BlockTime(blockNumber)
	}
	pivotBlockNumber := te.getPivotBlock(blockNumber)
	te.mu.RLock()
	_, ok := te.pivotBlocks[pivotBlockNumber]
	te.mu.RUnlock()
	if !ok {
		pivotBlock := pivotBlock{
			time: te.client.BlockTime(pivotBlockNumber),
		}
		nearbyBlock := pivotBlockNumber - nearbyGap
		if pivotBlockNumber <= nearbyGap {
			nearbyBlock = pivotBlockNumber + nearbyGap
			nearbyBlockTime := te.client.BlockTime(nearbyBlock)
			pivotBlock.timePerBlock = (nearbyBlockTime - pivotBlock.time) / nearbyGap
		} else {
			nearbyBlockTime := te.client.BlockTime(nearbyBlock)
			pivotBlock.timePerBlock = (pivotBlock.time - nearbyBlockTime) / nearbyGap
		}
		te.mu.Lock()
		te.pivotBlocks[pivotBlockNumber] = pivotBlock
		te.mu.Unlock()
	}

	te.mu.RLock()
	pivotBlock := te.pivotBlocks[pivotBlockNumber]
	te.mu.RUnlock()
	return (blockNumber-pivotBlockNumber)*pivotBlock.timePerBlock + pivotBlock.time
}
