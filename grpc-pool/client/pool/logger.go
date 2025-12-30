package pool

type PoolOption func(*Pool)
type NopLogger struct{}


type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type LogLevel int

const (
    Debug LogLevel = iota
    Info
    Warn
    Error
)

func (NopLogger) Debug(string, ...any) {}
func (NopLogger) Info(string, ...any)  {}
func (NopLogger) Warn(string, ...any)  {}
func (NopLogger) Error(string, ...any) {}


func WithLogger(l Logger) PoolOption {
    return func(p *Pool) {
        if l != nil {
			p.logger = l
        }
    }
}

func WithLogLevel(level LogLevel) PoolOption {
	return func(p *Pool){
		p.logLevel = level
	}
}
