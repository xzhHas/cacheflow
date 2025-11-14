package strategy

import (
    "context"
    "encoding/json"
    "time"

    "github.com/redis/go-redis/v9"
    "github.com/xzhHas/cacheflow/types"
)

type WriteThroughStrategy struct {
    Client redis.Cmdable
    Key    types.KeyFunc
    TTLSeconds int
}

func (w *WriteThroughStrategy) write(row map[string]any) error {
    if w.Key == nil || w.Client == nil {
        return nil
    }
    k := w.Key(row)
    if k == "" {
        return nil
    }
    b, err := json.Marshal(row)
    if err != nil {
        return err
    }
    var ttl = 0
    if w.TTLSeconds > 0 {
        ttl = w.TTLSeconds
    }
    return w.Client.Set(context.Background(), k, string(b), time.Duration(ttl)*time.Second).Err()
}

func (w *WriteThroughStrategy) OnInsert(row map[string]any) error { return w.write(row) }

func (w *WriteThroughStrategy) OnUpdate(before, after map[string]any) error { return w.write(after) }

func (w *WriteThroughStrategy) OnDelete(row map[string]any) error {
    if w.Key == nil || w.Client == nil {
        return nil
    }
    k := w.Key(row)
    if k == "" {
        return nil
    }
    return w.Client.Del(context.Background(), k).Err()
}
