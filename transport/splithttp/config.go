package splithttp

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"math/rand"
	"net"
	"sort"
	"strconv"
	"strings"

	"github.com/metacubex/http"
)

const (
	PlacementQueryInHeader = "queryInHeader"
	PlacementCookie        = "cookie"
	PlacementHeader        = "header"
	PlacementQuery         = "query"
	PlacementPath          = "path"
	PlacementBody          = "body"
	PlacementAuto          = "auto"
)

type PaddingMethod string

const (
	PaddingMethodRepeatX  PaddingMethod = "repeat-x"
	PaddingMethodTokenish PaddingMethod = "tokenish"
)

type RangeConfig struct {
	From int
	To   int
}

func (r RangeConfig) rand() int {
	if r.To <= 0 {
		return 0
	}
	if r.From <= 0 {
		r.From = r.To
	}
	if r.To < r.From {
		r.To = r.From
	}
	if r.From == r.To {
		return r.From
	}
	return r.From + rand.Intn(r.To-r.From+1)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

type XmuxConfig struct {
	MaxConcurrency   *RangeConfig
	MaxConnections   *RangeConfig
	CMaxReuseTimes   *RangeConfig
	HMaxRequestTimes *RangeConfig
	HMaxReusableSecs *RangeConfig
	HKeepAlivePeriod int
}

type SplitHTTPConfig struct {
	Host               string
	Path               string
	ALPN               []string
	ClientKey          string
	DownloadConfig     *SplitHTTPConfig
	DialAddr           string
	DialTransport      func(ctx context.Context, httpVersion string) (net.Conn, error)
	H3PacketDial       func(ctx context.Context, rAddr *net.UDPAddr) (net.PacketConn, error)
	H1UploadDial       func(ctx context.Context) (net.Conn, error)
	TLSServerName      string
	Headers            http.Header
	MaxUploadSize      int
	MaxConcurrentPosts int
	Mode               string
	Xmux               *XmuxConfig
	TLS                bool

	XPaddingBytes        *RangeConfig
	XPaddingObfsMode     bool
	XPaddingKey          string
	XPaddingHeader       string
	XPaddingPlacement    string
	XPaddingMethod       string
	UplinkHTTPMethod     string
	ScMaxEachPostBytes   *RangeConfig
	ScMinPostsInterval   *RangeConfig
	ScMaxBufferedPosts   int
	ScStreamUpServerSec  *RangeConfig
	NoGRPCHeader         bool
	NoSSEHeader          bool
	SessionPlacement     string
	SessionKey           string
	SeqPlacement         string
	SeqKey               string
	UplinkDataPlacement  string
	UplinkDataKey        string
	UplinkChunkSize      *RangeConfig
	ServerMaxHeaderBytes int
	RequestLog           bool
	TryQUIC              bool
}

func (c *SplitHTTPConfig) GetNormalizedXmux() XmuxConfig {
	if c.Xmux == nil {
		return XmuxConfig{}
	}
	return *c.Xmux
}

func (c XmuxConfig) GetNormalizedMaxConcurrency() RangeConfig {
	if c.MaxConcurrency == nil || c.MaxConcurrency.To <= 0 {
		return RangeConfig{}
	}
	return *c.MaxConcurrency
}

func (c XmuxConfig) GetNormalizedMaxConnections() RangeConfig {
	if c.MaxConnections == nil || c.MaxConnections.To <= 0 {
		return RangeConfig{}
	}
	return *c.MaxConnections
}

func (c XmuxConfig) GetNormalizedCMaxReuseTimes() RangeConfig {
	if c.CMaxReuseTimes == nil || c.CMaxReuseTimes.To <= 0 {
		return RangeConfig{}
	}
	return *c.CMaxReuseTimes
}

func (c XmuxConfig) GetNormalizedHMaxRequestTimes() RangeConfig {
	if c.HMaxRequestTimes == nil || c.HMaxRequestTimes.To <= 0 {
		return RangeConfig{}
	}
	return *c.HMaxRequestTimes
}

func (c XmuxConfig) GetNormalizedHMaxReusableSecs() RangeConfig {
	if c.HMaxReusableSecs == nil || c.HMaxReusableSecs.To <= 0 {
		return RangeConfig{}
	}
	return *c.HMaxReusableSecs
}

func (c *SplitHTTPConfig) HasALPN(token string) bool {
	if token == "" {
		return false
	}
	token = strings.ToLower(token)
	for _, p := range c.ALPN {
		if strings.ToLower(strings.TrimSpace(p)) == token {
			return true
		}
	}
	return false
}

func (c *SplitHTTPConfig) HasTCPFallback() bool {
	if len(c.ALPN) == 0 {
		return true
	}
	return c.HasALPN("h2") || c.HasALPN("http/1.1") || c.HasALPN("http/1.0")
}

func (c *SplitHTTPConfig) IsHTTPProtoAllowed(proto string) bool {
	if len(c.ALPN) == 0 {
		return true
	}
	allowed := map[string]bool{}
	for _, p := range c.ALPN {
		switch strings.ToLower(strings.TrimSpace(p)) {
		case "h3":
			allowed["HTTP/3"] = true
		case "h2":
			allowed["HTTP/2"] = true
			allowed["HTTP/1.1"] = true
		case "http/1.1":
			allowed["HTTP/1.1"] = true
			// User-required behavior: when declared as HTTP/1.1, still allow/try h2 upgrade.
			allowed["HTTP/2"] = true
		case "http/1.0":
			allowed["HTTP/1.0"] = true
		}
	}
	if len(allowed) == 0 {
		return true
	}
	for ap := range allowed {
		if strings.HasPrefix(proto, ap) {
			return true
		}
	}
	return false
}

func (c *SplitHTTPConfig) GetNormalizedPath() string {
	pathAndQuery := strings.SplitN(c.Path, "?", 2)
	path := pathAndQuery[0]
	if path == "" || path[0] != '/' {
		path = "/" + path
	}
	if path[len(path)-1] != '/' {
		path += "/"
	}
	return path
}

func (c *SplitHTTPConfig) GetRequestHeader() http.Header {
	header := http.Header{}
	for k, vv := range c.Headers {
		for _, v := range vv {
			header.Add(k, v)
		}
	}
	applyDefaultFetchHeaders(header)
	return header
}

func applyDefaultFetchHeaders(header http.Header) {
	if header.Get("User-Agent") == "" {
		header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36")
	}
	if header.Get("Sec-CH-UA") == "" {
		header.Set("Sec-CH-UA", "\"Not=A?Brand\";v=\"8\", \"Chromium\";v=\"145\", \"Google Chrome\";v=\"145\"")
	}
	if header.Get("Sec-CH-UA-Mobile") == "" {
		header.Set("Sec-CH-UA-Mobile", "?0")
	}
	if header.Get("Sec-CH-UA-Platform") == "" {
		header.Set("Sec-CH-UA-Platform", "\"Windows\"")
	}
	if header.Get("DNT") == "" {
		header.Set("DNT", "1")
	}
	if header.Get("Accept-Language") == "" {
		header.Set("Accept-Language", "en-US,en;q=0.9")
	}
	if header.Get("Accept") == "" {
		header.Set("Accept", "*/*")
	}
	if header.Get("Cache-Control") == "" {
		header.Set("Cache-Control", "no-cache")
	}
	if header.Get("Pragma") == "" {
		header.Set("Pragma", "no-cache")
	}
	header.Set("Sec-Fetch-Mode", "cors")
	header.Set("Sec-Fetch-Dest", "empty")
	header.Set("Sec-Fetch-Site", "same-origin")
	if header.Get("Priority") == "" {
		header.Set("Priority", "u=1, i")
	}
}

func (c *SplitHTTPConfig) WriteResponseHeader(writer http.ResponseWriter, requestMethod string, requestHeader http.Header) {
	if writer == nil {
		return
	}
	origin := requestHeader.Get("Origin")
	if origin == "" {
		writer.Header().Set("Access-Control-Allow-Origin", "*")
	} else {
		writer.Header().Set("Access-Control-Allow-Origin", origin)
	}

	if c.GetNormalizedSessionPlacement() == PlacementCookie ||
		c.GetNormalizedSeqPlacement() == PlacementCookie ||
		c.XPaddingPlacement == PlacementCookie ||
		c.GetNormalizedUplinkDataPlacement() == PlacementCookie {
		writer.Header().Set("Access-Control-Allow-Credentials", "true")
	}

	if requestMethod == http.MethodOptions {
		if requestedMethod := requestHeader.Get("Access-Control-Request-Method"); requestedMethod != "" {
			writer.Header().Set("Access-Control-Allow-Methods", requestedMethod)
		} else {
			writer.Header().Set("Access-Control-Allow-Methods", "*")
		}
		if requestedHeaders := requestHeader.Get("Access-Control-Request-Headers"); requestedHeaders != "" {
			writer.Header().Set("Access-Control-Allow-Headers", requestedHeaders)
		} else {
			writer.Header().Set("Access-Control-Allow-Headers", "*")
		}
	}
}

func (c *SplitHTTPConfig) BuildResponseXPadding() XPaddingConfig {
	padding := XPaddingConfig{
		Length: randInRange(c.GetNormalizedXPaddingBytes()),
		Method: PaddingMethodRepeatX,
		Placement: XPaddingPlacement{
			Placement: PlacementHeader,
			Header:    "X-Padding",
		},
	}
	if c.XPaddingObfsMode {
		padding.Method = PaddingMethod(c.XPaddingMethod)
		padding.Placement = XPaddingPlacement{
			Placement: c.XPaddingPlacement,
			Key:       c.XPaddingKey,
			Header:    c.XPaddingHeader,
		}
	}
	return padding
}

func (c *SplitHTTPConfig) GetNormalizedXPaddingBytes() RangeConfig {
	if c.XPaddingBytes == nil || c.XPaddingBytes.To <= 0 {
		return RangeConfig{From: 100, To: 1000}
	}
	if c.XPaddingBytes.From <= 0 {
		return RangeConfig{From: 1, To: c.XPaddingBytes.To}
	}
	return *c.XPaddingBytes
}

func (c *SplitHTTPConfig) GetNormalizedUplinkHTTPMethod() string {
	if c.UplinkHTTPMethod == "" {
		return "POST"
	}
	return strings.ToUpper(c.UplinkHTTPMethod)
}

func (c *SplitHTTPConfig) GetNormalizedScMaxEachPostBytes() RangeConfig {
	if c.ScMaxEachPostBytes != nil && c.ScMaxEachPostBytes.To > 0 {
		return *c.ScMaxEachPostBytes
	}
	if c.MaxUploadSize > 0 {
		return RangeConfig{From: c.MaxUploadSize, To: c.MaxUploadSize}
	}
	return RangeConfig{From: 1000000, To: 1000000}
}

func (c *SplitHTTPConfig) GetNormalizedScMinPostsInterval() RangeConfig {
	if c.ScMinPostsInterval != nil && c.ScMinPostsInterval.To > 0 {
		return *c.ScMinPostsInterval
	}
	return RangeConfig{From: 30, To: 30}
}

func (c *SplitHTTPConfig) GetNormalizedScMaxBufferedPosts() int {
	if c.ScMaxBufferedPosts > 0 {
		return c.ScMaxBufferedPosts
	}
	if c.MaxConcurrentPosts > 0 {
		return c.MaxConcurrentPosts
	}
	return 30
}

func (c *SplitHTTPConfig) GetNormalizedScStreamUpServerSecs() RangeConfig {
	if c.ScStreamUpServerSec != nil && c.ScStreamUpServerSec.To > 0 {
		return *c.ScStreamUpServerSec
	}
	return RangeConfig{From: 20, To: 80}
}

func (c *SplitHTTPConfig) GetNormalizedUplinkChunkSize() RangeConfig {
	if c.UplinkChunkSize == nil || c.UplinkChunkSize.To <= 0 {
		switch c.GetNormalizedUplinkDataPlacement() {
		case PlacementCookie:
			return RangeConfig{From: 2 * 1024, To: 3 * 1024}
		case PlacementHeader:
			return RangeConfig{From: 3 * 1000, To: 4 * 1000}
		default:
			return c.GetNormalizedScMaxEachPostBytes()
		}
	}
	if c.UplinkChunkSize.From < 64 {
		return RangeConfig{From: 64, To: maxInt(64, c.UplinkChunkSize.To)}
	}
	return *c.UplinkChunkSize
}

func (c *SplitHTTPConfig) GetNormalizedServerMaxHeaderBytes() int {
	if c.ServerMaxHeaderBytes <= 0 {
		return 8192
	}
	return c.ServerMaxHeaderBytes
}

func (c *SplitHTTPConfig) GetNormalizedSessionPlacement() string {
	if c.SessionPlacement == "" {
		return PlacementPath
	}
	return c.SessionPlacement
}

func (c *SplitHTTPConfig) GetNormalizedSeqPlacement() string {
	if c.SeqPlacement == "" {
		return PlacementPath
	}
	return c.SeqPlacement
}

func (c *SplitHTTPConfig) GetNormalizedUplinkDataPlacement() string {
	if c.UplinkDataPlacement == "" {
		return PlacementBody
	}
	return c.UplinkDataPlacement
}

func (c *SplitHTTPConfig) GetNormalizedSessionKey() string {
	if c.SessionKey != "" {
		return c.SessionKey
	}
	switch c.GetNormalizedSessionPlacement() {
	case PlacementHeader:
		return "X-Session"
	case PlacementCookie, PlacementQuery:
		return "x_session"
	default:
		return ""
	}
}

func (c *SplitHTTPConfig) GetNormalizedSeqKey() string {
	if c.SeqKey != "" {
		return c.SeqKey
	}
	switch c.GetNormalizedSeqPlacement() {
	case PlacementHeader:
		return "X-Seq"
	case PlacementCookie, PlacementQuery:
		return "x_seq"
	default:
		return ""
	}
}

func (c *SplitHTTPConfig) GetNormalizedUplinkDataKey() string {
	if c.UplinkDataKey != "" {
		return c.UplinkDataKey
	}
	if c.GetNormalizedUplinkDataPlacement() == PlacementHeader || c.GetNormalizedUplinkDataPlacement() == PlacementAuto {
		return "X-Data"
	}
	if c.GetNormalizedUplinkDataPlacement() == PlacementCookie {
		return "x_data"
	}
	return ""
}

func (c *SplitHTTPConfig) ExtractMetaFromRequest(req *http.Request, path string) (sessionID string, seqStr string) {
	sessionPlacement := c.GetNormalizedSessionPlacement()
	seqPlacement := c.GetNormalizedSeqPlacement()
	sessionKey := c.GetNormalizedSessionKey()
	seqKey := c.GetNormalizedSeqKey()

	var subpath []string
	pathPart := 0
	if sessionPlacement == PlacementPath || seqPlacement == PlacementPath {
		restPath := strings.TrimPrefix(req.URL.Path, path)
		restPath = strings.TrimPrefix(restPath, "/")
		if restPath != "" {
			subpath = strings.Split(restPath, "/")
		}
	}

	switch sessionPlacement {
	case PlacementPath:
		if len(subpath) > pathPart {
			sessionID = subpath[pathPart]
			pathPart++
		}
	case PlacementQuery:
		sessionID = req.URL.Query().Get(sessionKey)
	case PlacementHeader:
		sessionID = req.Header.Get(sessionKey)
	case PlacementCookie:
		if cookie, err := req.Cookie(sessionKey); err == nil {
			sessionID = cookie.Value
		}
	}

	switch seqPlacement {
	case PlacementPath:
		if len(subpath) > pathPart {
			seqStr = subpath[pathPart]
			pathPart++
		}
	case PlacementQuery:
		seqStr = req.URL.Query().Get(seqKey)
	case PlacementHeader:
		seqStr = req.Header.Get(seqKey)
	case PlacementCookie:
		if cookie, err := req.Cookie(seqKey); err == nil {
			seqStr = cookie.Value
		}
	}

	return sessionID, seqStr
}

func (c *SplitHTTPConfig) ApplyMetaToRequest(req *http.Request, sessionID string, seqStr string) {
	if sessionID != "" {
		switch c.GetNormalizedSessionPlacement() {
		case PlacementPath:
			req.URL.Path = appendToPath(req.URL.Path, sessionID)
		case PlacementQuery:
			q := req.URL.Query()
			q.Set(c.GetNormalizedSessionKey(), sessionID)
			req.URL.RawQuery = q.Encode()
		case PlacementHeader:
			req.Header.Set(c.GetNormalizedSessionKey(), sessionID)
		case PlacementCookie:
			req.AddCookie(&http.Cookie{Name: c.GetNormalizedSessionKey(), Value: sessionID})
		}
	}

	if seqStr != "" {
		switch c.GetNormalizedSeqPlacement() {
		case PlacementPath:
			req.URL.Path = appendToPath(req.URL.Path, seqStr)
		case PlacementQuery:
			q := req.URL.Query()
			q.Set(c.GetNormalizedSeqKey(), seqStr)
			req.URL.RawQuery = q.Encode()
		case PlacementHeader:
			req.Header.Set(c.GetNormalizedSeqKey(), seqStr)
		case PlacementCookie:
			req.AddCookie(&http.Cookie{Name: c.GetNormalizedSeqKey(), Value: seqStr})
		}
	}
}

func (c *SplitHTTPConfig) FillStreamRequest(req *http.Request, sessionID string) {
	req.Header = c.GetRequestHeader()
	length := randInRange(c.GetNormalizedXPaddingBytes())
	padding := XPaddingConfig{Length: length}
	if c.XPaddingObfsMode {
		padding.Placement = XPaddingPlacement{
			Placement: c.XPaddingPlacement,
			Key:       c.XPaddingKey,
			Header:    c.XPaddingHeader,
			RawURL:    req.URL.String(),
		}
		padding.Method = PaddingMethod(c.XPaddingMethod)
	} else {
		padding.Placement = XPaddingPlacement{
			Placement: PlacementQueryInHeader,
			Key:       "x_padding",
			Header:    "Referer",
			RawURL:    req.URL.String(),
		}
		padding.Method = PaddingMethodRepeatX
	}
	c.ApplyXPaddingToRequest(req, padding)
	c.ApplyMetaToRequest(req, sessionID, "")

	if req.Body != nil && !c.NoGRPCHeader && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/grpc")
	}
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", "Go-http-client/2.0")
	}
}

func (c *SplitHTTPConfig) FillPacketRequest(req *http.Request, sessionID string, seqStr string, payload []byte) {
	placement := c.GetNormalizedUplinkDataPlacement()
	if placement == PlacementBody || placement == PlacementAuto {
		req.Header = c.GetRequestHeader()
		req.Body = io.NopCloser(bytes.NewReader(payload))
		req.ContentLength = int64(len(payload))
	} else {
		req.Header = c.GetRequestHeader()
		s := encodePayload(payload)
		switch placement {
		case PlacementHeader:
			req.Header.Set(c.GetNormalizedUplinkDataKey()+"-0", s)
		case PlacementCookie:
			req.AddCookie(&http.Cookie{Name: c.GetNormalizedUplinkDataKey() + "_0", Value: s})
		}
	}

	length := randInRange(c.GetNormalizedXPaddingBytes())
	padding := XPaddingConfig{Length: length}
	if c.XPaddingObfsMode {
		padding.Placement = XPaddingPlacement{
			Placement: c.XPaddingPlacement,
			Key:       c.XPaddingKey,
			Header:    c.XPaddingHeader,
			RawURL:    req.URL.String(),
		}
		padding.Method = PaddingMethod(c.XPaddingMethod)
	} else {
		padding.Placement = XPaddingPlacement{
			Placement: PlacementQueryInHeader,
			Key:       "x_padding",
			Header:    "Referer",
			RawURL:    req.URL.String(),
		}
		padding.Method = PaddingMethodRepeatX
	}
	c.ApplyXPaddingToRequest(req, padding)
	c.ApplyMetaToRequest(req, sessionID, seqStr)

	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", "Go-http-client/2.0")
	}
}

func encodePayload(payload []byte) string {
	if len(payload) == 0 {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(payload)
}

func (c *SplitHTTPConfig) HeaderSummary(req *http.Request) string {
	keys := make([]string, 0, len(req.Header))
	for k := range req.Header {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(req.Header))
	for _, k := range keys {
		vv := req.Header[k]
		parts = append(parts, k+"="+strings.Join(vv, "|"))
	}
	return strings.Join(parts, ";")
}

func (c *SplitHTTPConfig) MetaSummary(req *http.Request) string {
	return "sessionPlacement=" + c.GetNormalizedSessionPlacement() + ",seqPlacement=" + c.GetNormalizedSeqPlacement() + ",uplinkDataPlacement=" + c.GetNormalizedUplinkDataPlacement() + ",path=" + req.URL.Path + ",query=" + req.URL.RawQuery + ",contentLength=" + strconv.FormatInt(req.ContentLength, 10)
}
