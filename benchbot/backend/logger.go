package backend

import (
	"log"
	"os"
)

type Logger struct {
	Debug *log.Logger
	Info  *log.Logger
	Error *log.Logger
	f     *os.File
}

func NewLogger(filePath string) (*Logger, error) {
	fileFlag := os.O_RDWR | os.O_CREATE | os.O_APPEND
	file, err := os.OpenFile(filePath, fileFlag, 0666)
	if err != nil {
		return nil, err
	}

	logFlag := log.Ldate | log.Ltime | log.Lshortfile
	log := &Logger{
		Debug: log.New(file, "[DEBUG] ", logFlag),
		Info:  log.New(file, "[INFO] ", logFlag),
		Error: log.New(file, "[ERROR] ", logFlag),
		f:     file,
	}

	return log, nil
}

func (log *Logger) Close() {
	log.f.Close()
}

func (log *Logger) Log(format string, v ...interface{}) {
	log.Info.Printf(format, v...)
}

func (log *Logger) LogD(format string, v ...interface{}) {
	log.Debug.Printf(format, v...)
}

func (log *Logger) LogE(format string, v ...interface{}) {
	log.Error.Printf(format, v...)
}
