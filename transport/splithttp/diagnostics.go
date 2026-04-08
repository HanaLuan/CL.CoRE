package splithttp

import (
	"sync/atomic"

	"github.com/metacubex/mihomo/log"
)

var (
	splitHTTPDiagClientIDs atomic.Uint64
	splitHTTPDiagConnIDs   atomic.Uint64
	splitHTTPDiagWriterIDs atomic.Uint64
	splitHTTPDiagReqIDs    atomic.Uint64

	splitHTTPDiagActiveClients atomic.Int64
	splitHTTPDiagActiveConns   atomic.Int64
	splitHTTPDiagActiveWriters atomic.Int64
	splitHTTPDiagActiveH1Conns atomic.Int64
	splitHTTPDiagInFlightOpen  atomic.Int64
	splitHTTPDiagInFlightPost  atomic.Int64
)

func splitHTTPDiagLog(config *SplitHTTPConfig, format string, v ...any) {
	if config == nil || !config.RequestLog {
		return
	}
	log.Infoln("[splithttp-diag] "+format, v...)
}

func splitHTTPDiagSnapshot() string {
	return "clients=%d conns=%d writers=%d h1=%d open=%d post=%d"
}
