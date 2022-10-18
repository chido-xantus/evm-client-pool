package eth

import (
	"strings"

	"github.com/ethereum/go-ethereum/rpc"
)

func isLogTooLargeError(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := err.(rpc.Error); (ok && err.(rpc.Error).ErrorCode() == -32005 && strings.Contains(err.Error(), "10000")) || // rate limit error infura
		strings.Contains(err.Error(), "limit exceeded") || // rate limit getblockio
		strings.Contains(err.Error(), "range too large") { // rate limit cloudflare-eth.com
		return true
	}
	return false
}
