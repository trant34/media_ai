package controlplane

import (
	"log/slog"
	"time"

	"github.com/gin-gonic/gin"
)

// ginLogger là middleware ghi log mỗi HTTP request bằng slog.
// Level: DEBUG cho 2xx/3xx, WARN cho 4xx, ERROR cho 5xx.
func ginLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		if q := c.Request.URL.RawQuery; q != "" {
			path += "?" + q
		}

		c.Next()

		latency := time.Since(start)
		status := c.Writer.Status()

		attrs := []any{
			"method",     c.Request.Method,
			"path",       path,
			"status",     status,
			"latency_ms", latency.Milliseconds(),
			"ip",         c.ClientIP(),
			"bytes",      c.Writer.Size(),
		}
		if id := c.GetHeader("X-Request-ID"); id != "" {
			attrs = append(attrs, "request_id", id)
		}
		if err := c.Errors.ByType(gin.ErrorTypePrivate).String(); err != "" {
			attrs = append(attrs, "error", err)
		}

		switch {
		case status >= 500:
			slog.Error("http request", attrs...)
		case status >= 400:
			slog.Warn("http request", attrs...)
		default:
			slog.Debug("http request", attrs...)
		}
	}
}
