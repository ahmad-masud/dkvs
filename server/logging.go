package server

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// logger is the package-wide logger configured for colorful, timestamped output.
var logger = logrus.New()

func init() {
	// Text formatter with colors and full timestamps
	logger.SetFormatter(&logrus.TextFormatter{
		ForceColors:     true,
		FullTimestamp:   true,
		TimestampFormat: time.RFC3339Nano,
		PadLevelText:    true,
	})
	// Default to debug for rich visibility; can be overridden via KVSTORE_LOG
	logger.SetLevel(logrus.DebugLevel)
	if lvl := strings.TrimSpace(os.Getenv("KVSTORE_LOG")); lvl != "" {
		if v, err := logrus.ParseLevel(strings.ToLower(lvl)); err == nil {
			logger.SetLevel(v)
		}
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
