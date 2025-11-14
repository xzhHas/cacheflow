package strategy

import (
    "testing"

    "github.com/go-redis/redismock/v9"
)

func TestDeleteStrategy(t *testing.T) {
    c, mock := redismock.NewClientMock()
    s := &DeleteStrategy{
        Client: c,
        Key: func(row map[string]any) string {
            return "user:1"
        },
    }
    mock.ExpectDel("user:1").SetVal(1)
    _ = s.OnUpdate(map[string]any{"id": 1}, map[string]any{"id": 1})
    if err := mock.ExpectationsWereMet(); err != nil {
        t.Fatal(err)
    }
}
