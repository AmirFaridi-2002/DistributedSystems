package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARNING
	ERROR
)

const (
	ColorReset   = "\033[0m"
	ColorRed     = "\033[31m"
	ColorGreen   = "\033[32m"
	ColorYellow  = "\033[33m"
	ColorMagenta = "\033[35m"
)

type Logger struct {
	mu        sync.Mutex
	file      *os.File
	level     LogLevel
	printflag bool
}

func NewLogger(level LogLevel, filename string, printflag bool) *Logger {
	logDir := filepath.Dir(filename)
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		fmt.Printf("Error creating log directory %s: %v\n", logDir, err)
		os.Exit(1)
	}

	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Printf("Error opening log file %s: %v\n", filename, err)
		os.Exit(1)
	}

	return &Logger{
		file:      f,
		level:     level,
		printflag: printflag,
	}
}

func (l *Logger) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.file.Close(); err != nil {
		fmt.Printf("Error closing log file: %v\n", err)
	}
}

func (l *Logger) log(level LogLevel, color string, levelStr string, format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a...)
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	rawMsg := fmt.Sprintf("%s [%s] %s\n", timestamp, levelStr, msg)
	finalMsg := fmt.Sprintf("%s%s%s", color, rawMsg, ColorReset)

	l.mu.Lock()
	defer l.mu.Unlock()

	if _, err := l.file.WriteString(finalMsg); err != nil {
		fmt.Printf("Error writing to log file: %v\n", err)
	}

	if l.printflag {
		fmt.Print(finalMsg)
	}
}

func (l *Logger) Info(format string, a ...interface{}) {
	if l.level <= INFO {
		l.log(INFO, ColorGreen, "INFO", format, a...)
	}
}

func (l *Logger) Warn(format string, a ...interface{}) {
	if l.level <= WARNING {
		l.log(WARNING, ColorYellow, "WARN", format, a...)
	}
}

func (l *Logger) Error(format string, a ...interface{}) {
	if l.level <= ERROR {
		l.log(ERROR, ColorRed, "ERROR", format, a...)
	}
}
