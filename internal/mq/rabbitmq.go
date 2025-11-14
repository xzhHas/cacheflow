package mq

import (
    "context"
    "encoding/json"

    amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQ struct {
    url       string
    exchange  string
    queue     string
    routing   string
    conn      *amqp.Connection
    ch        *amqp.Channel
    consumerC chan struct{}
}

func NewRabbitMQ(url, exchange, queue, routing string) (*RabbitMQ, error) {
    conn, err := amqp.Dial(url)
    if err != nil {
        return nil, err
    }
    ch, err := conn.Channel()
    if err != nil {
        conn.Close()
        return nil, err
    }
    if exchange != "" {
        if err := ch.ExchangeDeclare(exchange, "direct", true, false, false, false, nil); err != nil {
            ch.Close()
            conn.Close()
            return nil, err
        }
    }
    _, err = ch.QueueDeclare(queue, true, false, false, false, nil)
    if err != nil {
        ch.Close()
        conn.Close()
        return nil, err
    }
    if exchange != "" {
        if err := ch.QueueBind(queue, routing, exchange, false, nil); err != nil {
            ch.Close()
            conn.Close()
            return nil, err
        }
    }
    return &RabbitMQ{url: url, exchange: exchange, queue: queue, routing: routing, conn: conn, ch: ch, consumerC: make(chan struct{}, 1)}, nil
}

func (r *RabbitMQ) Publish(m Message) error {
    b, err := json.Marshal(m)
    if err != nil {
        return err
    }
    return r.ch.PublishWithContext(context.Background(), r.exchange, r.routing, false, false, amqp.Publishing{ContentType: "application/json", Body: b})
}

func (r *RabbitMQ) Start(h func(Message) error) error {
    msgs, err := r.ch.Consume(r.queue, "", false, false, false, false, nil)
    if err != nil {
        return err
    }
    go func() {
        for d := range msgs {
            var m Message
            if err := json.Unmarshal(d.Body, &m); err == nil {
                _ = h(m)
            }
            d.Ack(false)
        }
        r.consumerC <- struct{}{}
    }()
    return nil
}

func (r *RabbitMQ) Stop() error {
    r.ch.Close()
    r.conn.Close()
    return nil
}
