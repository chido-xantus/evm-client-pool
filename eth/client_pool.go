package eth

import (
	"context"
	"fmt"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/go-resty/resty/v2"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"math/big"
	"strings"
	"sync"
	"time"
)

type (
	ClientPool struct {
		clients []*Client
		counter int
		mu      sync.Mutex
		config  Config
	}
	GetBlockTimeResponse struct {
		Result struct {
			Timestamp string `json:"timestamp"`
		} `json:"result"`
	}
)

func NewBasicClientPool(cfg Config) (*ClientPool, error) {
	rpcsUrls := strings.Split(cfg.RpcUrls, ",")
	clients := make([]*Client, len(rpcsUrls))
	for i, rpcUrl := range rpcsUrls {
		var err error
		clients[i], err = NewClient(rpcUrl, "")
		if err != nil {
			return nil, errors.Wrap(err, "unable to init new client")
		}
	}
	return &ClientPool{
		clients: clients,
		config:  cfg,
	}, nil
}

// GetClient block until a client is available
func (pool *ClientPool) GetClient() *Client {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	for {
		for i := 0; i < len(pool.clients); i++ {
			client := pool.clients[pool.counter]
			pool.counter = (pool.counter + 1) % len(pool.clients)
			if client.IsAvailable() {
				return client
			}
		}
		logrus.Infof("all clients are down, sleep for 1 minute")
		time.Sleep(time.Minute)
	}
}

func (pool *ClientPool) GetClients(numClients int) ([]*Client, error) {
	if numClients > len(pool.clients) {
		return nil, errors.New(fmt.Sprintf("numClients must be less than client pool size: %d", len(pool.clients)))
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	for availableClients := pool.countAvailableClients(); availableClients < numClients; {
		logrus.Infof("Request %d clients but only %d available. Sleep for 1 minute", numClients, availableClients)
		time.Sleep(time.Minute)
	}

	clients := make([]*Client, 0, numClients)
	for len(clients) < numClients {
		client := pool.clients[pool.counter]
		pool.counter = (pool.counter + 1) % len(pool.clients)
		if client.IsAvailable() {
			clients = append(clients, client)
		}
	}
	return clients, nil
}

func (pool *ClientPool) countAvailableClients() int {
	counter := 0
	for _, client := range pool.clients {
		if client.IsAvailable() {
			counter++
		}
	}
	return counter
}

func (pool *ClientPool) RunOp(ctx context.Context, op func(client *Client) error) {
	for ctx.Err() == nil {
		client := pool.GetClient()
		err := op(client)
		if err == nil {
			return
		}
	}
}

func (pool *ClientPool) GetLatestBlock() uint64 {
	for {
		client := pool.GetClient()
		maxBlock, err := client.BlockNumber(context.Background())
		if err != nil {
			client.MarkError(err)
			logrus.Errorf("get max block error: %v", err)
			continue
		}
		return maxBlock
	}
}

func (pool *ClientPool) GetLogs(filterQuery ethereum.FilterQuery, fromBlock, toBlock uint64, numProof int) ([]types.Log, error) {
	if numProof <= 1 {
		return pool.getLogs(filterQuery, fromBlock, toBlock)
	}

	var wg sync.WaitGroup
	wg.Add(numProof)
	results := make([][]types.Log, numProof)
	resultsLocker := sync.Mutex{}
	availableClients, err := pool.GetClients(numProof)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to get clients")
	}

	for i := 0; i < numProof; i++ {
		go func(index int) {
			defer wg.Done()
			logs, getLogsError := pool.getLogs(filterQuery, fromBlock, toBlock, availableClients[index])
			if getLogsError != nil {
				err = errors.Wrapf(getLogsError, "getLogs {%d} error", index)
			}
			resultsLocker.Lock()
			results[index] = logs
			resultsLocker.Unlock()
		}(i)
	}

	wg.Wait()
	if err != nil {
		return nil, err
	}

	consistent := true
	for i := 0; i < numProof; i++ {
		if i < numProof-1 {
			consistent = pool.compareListsLogs(results[i], results[i+1])
			if !consistent {
				break
			}
		}
	}
	if consistent {
		return results[0], nil
	} else {
		for i := 0; i < numProof; i++ {
			logrus.Infof("\t[Consistency error trace] Block range: [%d, %d]: Client %s returned %d logs", fromBlock, toBlock, availableClients[i].endpoint, len(results[i]))
		}
		return nil, errors.New("Consistency error")
	}
}

func (pool *ClientPool) compareListsLogs(logs1 []types.Log, logs2 []types.Log) (equal bool) {
	if len(logs1) != len(logs2) {
		logrus.Debugf("logs1 (%d items) is not equal to logs2 (%d items)", len(logs1), len(logs2))
		return false
	}

	for i := 0; i < len(logs1); i++ {
		if !pool.compareLogs(logs1[i], logs2[i]) {
			logrus.Debugf("logs1[%d] is not equal to logs2[%d]. \n\tlog1: %v\n\tlog2: %v", i, i, logs1[i], logs2[i])
			return false
		}
	}
	return true
}

func (pool *ClientPool) compareLogs(log1 types.Log, log2 types.Log) (equal bool) {
	return log1.TxHash == log2.TxHash &&
		log1.Index == log2.Index
}

func (pool *ClientPool) getLogs(filterQuery ethereum.FilterQuery, fromBlock, toBlock uint64, specificClient ...*Client) ([]types.Log, error) {
	if fromBlock > toBlock {
		return []types.Log{}, nil
	}

	var client *Client
	if len(specificClient) > 0 && specificClient[0] != nil {
		client = specificClient[0]
	} else {
		client = pool.GetClient()
	}
	logs, err := client.FilterLogs(context.Background(), filterQuery)
	if err != nil {
		client.MarkError(err)
		logrus.Errorf("Fetch logs [%d to %d] on endpoint %v error: %v", fromBlock, toBlock, client.endpoint, err)
	}
	if isLogTooLargeError(err) && toBlock > fromBlock {
		midBlockNumber := fromBlock + (toBlock-fromBlock)/2
		log1, err1 := pool.getLogs(filterQuery, fromBlock, midBlockNumber)
		if err1 != nil {
			return nil, err1
		}
		log2, err2 := pool.getLogs(filterQuery, midBlockNumber+1, toBlock)
		if err2 != nil {
			return nil, err2
		}
		logs = append(log1, log2...)
		return logs, nil
	}
	return logs, errors.Wrap(err, "client endpoint: "+client.endpoint)
}

func (pool *ClientPool) BlockTime(blockNumber uint64) uint64 {
	if pool.config.ManualBlockTime {
		return pool.manualBlockTime(blockNumber)
	}
	return pool.rpcBlockTime(blockNumber)
}

func (pool *ClientPool) rpcBlockTime(blockNumber uint64) uint64 {
	for {
		ethClient := pool.GetClient()
		block, err := ethClient.BlockByNumber(context.Background(), big.NewInt(int64(blockNumber)))
		if err != nil {
			logrus.Infof("error requesting blocktime from node, backing off. BlockNumber: %v Endpoint: %v, Err: %v,", blockNumber, ethClient.endpoint, err)
			ethClient.MarkError(err)
			continue
		}
		return block.Time()
	}
}

func (pool *ClientPool) manualBlockTime(blockNumber uint64) uint64 {
	for {
		ethClient := pool.GetClient()
		url := ethClient.endpoint
		body := map[string]interface{}{
			"jsonrpc": "2.0",
			"method":  "eth_getBlockByNumber",
			"params": []interface{}{
				DecimalToHex(int64(blockNumber)),
				true,
			},
			"id": 0,
		}
		client := resty.New()
		res, err := client.R().
			SetHeader("Content-Type", "application/json").
			SetBody(body).
			SetResult(GetBlockTimeResponse{}).
			Post(url)
		if err != nil {
			logrus.Infof("error manual requesting blocktime from node, backing off. BlockNumber: %v Endpoint: %v, Err: %v,", blockNumber, ethClient.endpoint, err)
			ethClient.MarkError(err)
			continue
		}
		if res.IsError() {
			logrus.Infof("error manual requesting blocktime from node, status code error. BlockNumber: %v Endpoint: %v, Err: %v,", blockNumber, ethClient.endpoint, string(res.Body()))
			ethClient.MarkError(err)
			continue
		}
		data := res.Result().(*GetBlockTimeResponse)
		result, err := HexToInt(data.Result.Timestamp)
		if err != nil {
			logrus.Infof("error manual requesting blocktime from node, hex to int. BlockNumber: %v Endpoint: %v, Err: %v,", blockNumber, ethClient.endpoint, err)
			ethClient.MarkError(err)
			continue
		}
		return uint64(result)
	}

}

func (pool *ClientPool) GetToBlock(fromRange int, maxToBlock int) int {
	if fromRange < maxToBlock {
		return fromRange
	}
	return maxToBlock
}
