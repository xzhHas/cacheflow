package mq

type Message struct {
    Schema   string            `json:"schema"`
    Table    string            `json:"table"`
    Action   string            `json:"action"`
    Before   map[string]any    `json:"before"`
    After    map[string]any    `json:"after"`
    Attempts int               `json:"attempts"`
}

type Producer interface {
    Publish(Message) error
}

type Consumer interface {
    Start(func(Message) error) error
    Stop() error
}
