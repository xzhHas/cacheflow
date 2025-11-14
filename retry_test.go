package cacheflow

import (
    "errors"
    "testing"

    "github.com/go-redis/redismock/v9"
    "github.com/xzhHas/cacheflow/internal/mq"
)

type recProd struct{ msgs []mq.Message }

func (r *recProd) Publish(m mq.Message) error { r.msgs = append(r.msgs, m); return nil }

func TestEnqueueOnRedisError(t *testing.T) {
    c, mock := redismock.NewClientMock()
    s := New()
    s.SetConfig(Config{
        Redis: RedisConfig{Addr: "127.0.0.1:6379", DB: 0},
        Tables: []TableConfig{
            {
                DB:    "user",
                Table: "users",
                Key: func(row map[string]any) string {
                    return "user:1"
                },
                Strategy: CacheAside,
            },
        },
        Retry: RetryConfig{Enable: true, MaxAttempts: 1, BackoffMillis: 1},
    })
    s.rc = c
    rp := &recProd{}
    s.producer = rp
    s.retryEnabled = true
    mock.ExpectDel("user:1").SetErr(errors.New("x"))
    s.OnDelete("user", "users", map[string]any{"id": 1})
    select {
    case e := <-s.Events():
        if e.Action == "delete" && e.Schema == "user" && e.Table == "users" {
        } else {
            t.Fatal("wrong event")
        }
    default:
        t.Fatal("no event emitted")
    }
    if len(rp.msgs) != 1 {
        t.Fatal("no retry message")
    }
}