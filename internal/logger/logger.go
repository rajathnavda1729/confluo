// Package logger provides a small structured logger interface with level and optional JSON output.
// Used by consumer, delay queue, timeout manager, and main for parseable logs (e.g. in log aggregators).
package logger

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// Level is the minimum level to log.
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

func parseLevel(s string) Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return LevelDebug
	case "info", "":
		return LevelInfo
	case "warn", "warning":
		return LevelWarn
	case "error":
		return LevelError
	default:
		return LevelInfo
	}
}

// Logger is the interface used for structured logging. Keyvals are alternating key, value (e.g. "topic", "x", "partition", 0).
type Logger interface {
	Debug(msg string, keyvals ...interface{})
	Info(msg string, keyvals ...interface{})
	Warn(msg string, keyvals ...interface{})
	Error(msg string, keyvals ...interface{})
}

// impl is the internal implementation (level + format).
type impl struct {
	level  Level
	format string // "json" or "text"
	mu     sync.Mutex
}

// Global is the default logger used when no logger is injected. Set by NewFromEnv or New.
var Global Logger = &impl{level: LevelInfo, format: "text"}

var defaultLoggerMu sync.RWMutex

// SetGlobal sets the global logger (e.g. from NewFromEnv). Safe for concurrent use.
func SetGlobal(l Logger) {
	defaultLoggerMu.Lock()
	defer defaultLoggerMu.Unlock()
	Global = l
}

// NewFromEnv creates a logger from environment: LOG_LEVEL (debug|info|warn|error), LOG_FORMAT (text|json).
func NewFromEnv() Logger {
	level := parseLevel(os.Getenv("LOG_LEVEL"))
	format := strings.ToLower(strings.TrimSpace(os.Getenv("LOG_FORMAT")))
	if format != "json" {
		format = "text"
	}
	l := &impl{level: level, format: format}
	SetGlobal(l)
	return l
}

// New creates a logger with the given level and format ("text" or "json").
func New(level Level, format string) Logger {
	if format != "json" {
		format = "text"
	}
	return &impl{level: level, format: format}
}

func (l *impl) log(level Level, levelStr, msg string, keyvals ...interface{}) {
	if level < l.level {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.format == "json" {
		l.writeJSON(levelStr, msg, keyvals)
	} else {
		l.writeText(levelStr, msg, keyvals)
	}
}

func (l *impl) writeText(levelStr, msg string, keyvals ...interface{}) {
	ts := time.Now().Format(time.RFC3339)
	buf := fmt.Sprintf("%s %s %s", ts, strings.ToUpper(levelStr), msg)
	for i := 0; i+1 < len(keyvals); i += 2 {
		buf += fmt.Sprintf(" %v=%v", keyvals[i], keyvals[i+1])
	}
	fmt.Fprintln(os.Stderr, buf)
}

func (l *impl) writeJSON(levelStr, msg string, keyvals ...interface{}) {
	m := map[string]interface{}{
		"ts":    time.Now().UTC().Format(time.RFC3339Nano),
		"level": levelStr,
		"msg":   msg,
	}
	for i := 0; i+1 < len(keyvals); i += 2 {
		k, ok := keyvals[i].(string)
		if !ok {
			k = fmt.Sprintf("%v", keyvals[i])
		}
		m[k] = keyvals[i+1]
	}
	enc := json.NewEncoder(os.Stderr)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(m)
}

func (l *impl) Debug(msg string, keyvals ...interface{}) { l.log(LevelDebug, "debug", msg, keyvals...) }
func (l *impl) Info(msg string, keyvals ...interface{})   { l.log(LevelInfo, "info", msg, keyvals...) }
func (l *impl) Warn(msg string, keyvals ...interface{})   { l.log(LevelWarn, "warn", msg, keyvals...) }
func (l *impl) Error(msg string, keyvals ...interface{})   { l.log(LevelError, "error", msg, keyvals...) }

// Nop returns a no-op logger that discards all output.
func Nop() Logger { return nopLogger{} }

type nopLogger struct{}

func (nopLogger) Debug(msg string, keyvals ...interface{}) {}
func (nopLogger) Info(msg string, keyvals ...interface{})  {}
func (nopLogger) Warn(msg string, keyvals ...interface{})  {}
func (nopLogger) Error(msg string, keyvals ...interface{}) {}
