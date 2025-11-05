package server

import (
	"bytes"
	"context"
	stdlog "log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// logger is the package-wide logger configured for colorful output (timestamps optional).
var logger = logrus.New()

// stdLogOut is a shared router used to capture stdlib and other stderr writes
// and forward them into logrus with level-aware routing.
var stdLogOut *stdLogRouter

func init() {
	// Text formatter with colors; timestamps enabled by default
	logger.SetFormatter(&logrus.TextFormatter{
		ForceColors:      true,
		FullTimestamp:    true,
		DisableTimestamp: false,
		TimestampFormat:  time.RFC3339Nano,
		PadLevelText:     true,
	})
	// Default to debug for rich visibility; can be overridden via KVSTORE_LOG
	logger.SetLevel(logrus.DebugLevel)
	if lvl := strings.TrimSpace(os.Getenv("KVSTORE_LOG")); lvl != "" {
		if v, err := logrus.ParseLevel(strings.ToLower(lvl)); err == nil {
			logger.SetLevel(v)
		}
	}

	// Redirect the standard library logger output to our custom writer so
	// third-party packages that use the stdlib logger (e.g. HashiCorp raft)
	// emit consistent, colored output through logrus. The router attempts to
	// detect a level token in the stdlib message and forward to logrus at the
	// matching level (ERROR/WARN/INFO/DEBUG). If none is found it defaults to
	// Info.
	stdLogOut = newStdLogRouter()
	stdlog.SetOutput(stdLogOut)
	// Avoid stdlib's timestamp prefix since logrus will add timestamps.
	stdlog.SetFlags(0)
}

// SetLogTimestamps toggles timestamp display in log output at runtime.
// Call early in program startup (e.g., from main) to affect global logging.
func SetLogTimestamps(enabled bool) {
	// Preserve other formatter settings while flipping timestamp flags
	tf := &logrus.TextFormatter{
		ForceColors:      true,
		FullTimestamp:    enabled,
		DisableTimestamp: !enabled,
		TimestampFormat:  time.RFC3339Nano,
		PadLevelText:     true,
	}
	logger.SetFormatter(tf)
}

// stdLogRouter routes lines written to the standard library logger into
// logrus at an appropriate level. It's safe for concurrent use.
type stdLogRouter struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func newStdLogRouter() *stdLogRouter { return &stdLogRouter{} }

func (r *stdLogRouter) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	n, err := r.buf.Write(p)
	if err != nil {
		return n, err
	}

	for {
		b := r.buf.Bytes()
		idx := bytes.IndexByte(b, '\n')
		if idx < 0 {
			break
		}
		// extract one line (without trailing newline)
		line := string(b[:idx])
		// consume the line + newline
		r.buf.Next(idx + 1)

		r.routeLine(strings.TrimSpace(line))
	}

	return n, nil
}

func (r *stdLogRouter) routeLine(line string) {
	if line == "" {
		return
	}
	// Normalize: strip common leading timestamp + bracketed level so messages
	// forwarded to logrus use the new formatting rather than preserve the
	// original timestamp/level tokens.
	norm := line
	// If line starts with a digit, it likely has an RFC3339-like timestamp.
	if len(norm) > 0 && norm[0] >= '0' && norm[0] <= '9' {
		// Try to strip up to a closing ']' (e.g. "2025-... [DEBUG] msg")
		if idx := strings.Index(norm, "]"); idx > 0 && idx < 80 {
			// remove leading timestamp and bracket portion
			norm = strings.TrimSpace(norm[idx+1:])
		} else {
			// fallback: remove first token (up to first space) if it contains a 'T' (timestamp)
			if sp := strings.Index(norm, " "); sp > 0 {
				if strings.Contains(norm[:sp], "T") {
					norm = strings.TrimSpace(norm[sp+1:])
				}
			}
		}
	}

	lower := strings.ToLower(strings.TrimSpace(norm))

	// Remove leading bracketed level like [debug], or plain prefixes like DEBUG:
	if strings.HasPrefix(lower, "[") {
		if rb := strings.Index(lower, "]"); rb > 0 {
			lower = strings.TrimSpace(lower[rb+1:])
			norm = strings.TrimSpace(norm[rb+1:])
		}
	}
	for _, p := range []string{"debug:", "debug ", "error:", "error ", "warn:", "warn ", "warning:", "warning "} {
		if strings.HasPrefix(lower, p) {
			lower = strings.TrimSpace(lower[len(p):])
			norm = strings.TrimSpace(norm[len(p):])
			break
		}
	}

	// Route by detected severity tokens (check the original lower-cased copy)
	switch {
	case strings.Contains(lower, "error") || strings.HasPrefix(lower, "error"):
		logger.WithField("source", "stdlib").Error(norm)
	case strings.Contains(lower, "warn") || strings.HasPrefix(lower, "warn"):
		logger.WithField("source", "stdlib").Warn(norm)
	case strings.Contains(lower, "debug") || strings.HasPrefix(lower, "debug"):
		logger.WithField("source", "stdlib").Debug(norm)
	default:
		logger.WithField("source", "stdlib").Info(norm)
	}
}

// loggingInterceptor logs every unary gRPC request with method, peer, status, and duration.
func loggingInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		start := time.Now()
		// peer info
		var peerAddr string
		if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
			peerAddr = p.Addr.String()
		}
		// auth header if present (scrub value length only)
		md, _ := metadata.FromIncomingContext(ctx)
		auth := md.Get("authorization")
		authLen := 0
		if len(auth) > 0 {
			authLen = len(auth[0])
		}
		// pre-log
		logger.WithFields(logrus.Fields{
			"subsys":     "grpc",
			"method":     info.FullMethod,
			"peer":       peerAddr,
			"auth_bytes": authLen,
		}).Debug("request start")

		resp, err := handler(ctx, req)

		dur := time.Since(start)
		st, _ := status.FromError(err)
		entry := logger.WithFields(logrus.Fields{
			"subsys":   "grpc",
			"method":   info.FullMethod,
			"peer":     peerAddr,
			"duration": dur.String(),
		})
		if err != nil {
			entry = entry.WithField("code", st.Code().String())
			entry.WithError(err).Warn("request end with error")
		} else {
			entry.Info("request end ok")
		}
		return resp, err
	}
}
