package cacheflow

import (
    "testing"

    "github.com/go-redis/redismock/v9"
)

func TestDispatchDelete(t *testing.T) {
    c, mock := redismock.NewClientMock()
    s := New()
    s.SetConfig(Config{
        Redis: struct {
            Addr     string
            Password string
            DB       int
        }{
            Addr:     "127.0.0.1:6379",
            Password: "",
            DB:       0,
        },
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
    })
    s.rc = c
    mock.ExpectDel("user:1").SetVal(1)
    s.OnUpdate("user", "users", map[string]any{"id": 1}, map[string]any{"id": 1})
    if err := mock.ExpectationsWereMet(); err != nil {
        t.Fatal(err)
    }
}