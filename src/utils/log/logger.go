package log

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/sirupsen/logrus"
)

var (
	logger LoggingInterface

	loggingModule = flag.String("loggingModule",
		"file",
		"Flag enable one of available logging module (file, console)")
	logLevel = flag.String("logLevel",
		"info",
		"Set logging level (debug, info, error, warning, fatal)")
	logFileDir = flag.String("logFileDir",
		defaultLogDir,
		"The flag to specify logging directory. The flag is only supported if logging module is file")
)

const (
	defaultLogDir   = "/var/log/huawei"
	timestampFormat = "2006-01-02 15:04:05.000000"
)

// LoggingInterface is an interface exposes logging functionality
type LoggingInterface interface {
	Debugf(format string, args ...interface{})

	Debugln(args ...interface{})

	Infof(format string, args ...interface{})

	Infoln(args ...interface{})

	Warningf(format string, args ...interface{})

	Warningln(args ...interface{})

	Errorf(format string, args ...interface{})

	Errorln(args ...interface{})

	Fatalf(format string, args ...interface{})

	Fatalln(args ...interface{})

	flushable

	closable
}

// Closable is an interface for closing logging streams.
// The interface should be implemented by hooks.
type closable interface {
	close()
}

// Flushable is an interface to commit current content of logging stream
type flushable interface {
	flush()
}

type loggerImpl struct {
	*logrus.Logger
	hooks     []logrus.Hook
	formatter logrus.Formatter
}

var _ LoggingInterface = &loggerImpl{}

func parseLogLevel() (logrus.Level, error) {
	switch *logLevel {
	case "debug":
		return logrus.DebugLevel, nil
	case "info":
		return logrus.InfoLevel, nil
	case "warning":
		return logrus.WarnLevel, nil
	case "error":
		return logrus.ErrorLevel, nil
	case "fatal":
		return logrus.FatalLevel, nil
	default:
		return logrus.FatalLevel, fmt.Errorf("invalid logging level [%v]", logLevel)
	}
}

// InitLogging configures logging. Logs are written to a log file or stdout/stderr.
// Since logrus doesn't support multiple writers, each log stream is implemented as a hook.
func InitLogging(logName string) error {
	var tmpLogger loggerImpl

	// initialize logrus in wrapper
	tmpLogger.Logger = logrus.New()

	// No output except for the hooks
	tmpLogger.Logger.SetOutput(ioutil.Discard)

	// set logging level
	level, err := parseLogLevel()
	if err != nil {
		return err
	}
	tmpLogger.Logger.SetLevel(level)

	// initialize log formatter
	formatter := &PlainTextFormatter{TimestampFormat: timestampFormat, pid: os.Getpid()}

	hooks := make([]logrus.Hook, 0)
	switch *loggingModule {
	case "file":
		logFilePath := fmt.Sprintf("%s/%s", *logFileDir, logName)
		// Write to the log file
		logFileHook, err := newFileHook(logFilePath, formatter)
		if err != nil {
			return fmt.Errorf("could not initialize logging to file: %v", err)
		}
		hooks = append(hooks, logFileHook)
	case "console":
		// Write to stdout/stderr
		logConsoleHook, err := newConsoleHook(formatter)
		if err != nil {
			return fmt.Errorf("could not initialize logging to console: %v", err)
		}
		hooks = append(hooks, logConsoleHook)
	default:
		return fmt.Errorf("invalid logging module [%v]. Support only 'file' or 'console'", loggingModule)
	}

	tmpLogger.hooks = hooks
	for _, hook := range tmpLogger.hooks {
		// initialize logrus with hooks
		tmpLogger.Logger.AddHook(hook)
	}

	logger = &tmpLogger
	return nil
}

// PlainTextFormatter is a formatter to ensure formatted logging output
type PlainTextFormatter struct {
	// TimestampFormat to use for display when a full timestamp is printed
	TimestampFormat string

	// process identity number
	pid int
}

var _ logrus.Formatter = &PlainTextFormatter{}

// Format ensure unified and formatted logging output
func (f *PlainTextFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	b := entry.Buffer
	if entry.Buffer == nil {
		b = &bytes.Buffer{}
	}

	_, _ = fmt.Fprintf(b, "%s %d %s%s\n", entry.Time.Format(f.TimestampFormat), f.pid, getLogLevel(entry.Level), entry.Message)

	return b.Bytes(), nil
}

func getLogLevel(level logrus.Level) string {
	switch level {
	case logrus.DebugLevel:
		return "[DEBUG]: "
	case logrus.InfoLevel:
		return "[INFO]: "
	case logrus.WarnLevel:
		return "[WARNING]: "
	case logrus.ErrorLevel:
		return "[ERROR]: "
	case logrus.FatalLevel:
		return "[FATAL]: "
	default:
		return "[UNKNOWN]: "
	}
}

// Debugf ensures output of formatted debug logs
func Debugf(format string, args ...interface{}) {
	logger.Debugf(format, args...)
}

// Debugln ensures output of Debug logs
func Debugln(args ...interface{}) {
	logger.Debugln(args...)
}

// Infof ensures output of formatted info logs
func Infof(format string, args ...interface{}) {
	logger.Infof(format, args...)
}

// Infoln ensures output of info logs
func Infoln(args ...interface{}) {
	logger.Infoln(args...)
}

// Warningf ensures output of formatted warning logs
func Warningf(format string, args ...interface{}) {
	logger.Warningf(format, args...)
}

// Warningln ensures output of warning logs
func Warningln(args ...interface{}) {
	logger.Warningln(args...)
}

// Errorf ensures output of formatted error logs
func Errorf(format string, args ...interface{}) {
	logger.Errorf(format, args...)
}

// Errorln ensures output of error logs
func Errorln(args ...interface{}) {
	logger.Errorln(args...)
}

// Fatalf ensures output of formatted fatal logs
func Fatalf(format string, args ...interface{}) {
	logger.Fatalf(format, args...)
}

// Fatalln ensures output of fatal logs
func Fatalln(args ...interface{}) {
	logger.Fatalln(args...)
}

func (logger *loggerImpl) flush() {
	for _, hook := range logger.hooks {
		flushable, ok := hook.(flushable)
		if ok {
			flushable.flush()
		}
	}
}

func (logger *loggerImpl) close() {
	for _, hook := range logger.hooks {
		flushable, ok := hook.(closable)
		if ok {
			flushable.close()
		}
	}
}

// Flush ensures to commit current content of logging stream
func Flush() {
	logger.flush()
}

// Close ensures closing output stream
func Close() {
	logger.close()
}
