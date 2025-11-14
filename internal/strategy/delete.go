package strategy

import (
    "context"

    "github.com/redis/go-redis/v9"
    "github.com/xzhHas/cacheflow/types"
)

type DeleteStrategy struct {
    Client redis.Cmdable
    Key    types.KeyFunc
}

func (d *DeleteStrategy) OnInsert(row map[string]any) error {
    if d.Key == nil || d.Client == nil {
        return nil
    }
    k := d.Key(row)
    if k == "" {
        return nil
    }
    return d.Client.Del(context.Background(), k).Err()
}

func (d *DeleteStrategy) OnUpdate(before, after map[string]any) error {
    if d.Key == nil || d.Client == nil {
        return nil
    }
    k := d.Key(after)
    if k == "" {
        return nil
    }
    return d.Client.Del(context.Background(), k).Err()
}

func (d *DeleteStrategy) OnDelete(row map[string]any) error {
    if d.Key == nil || d.Client == nil {
        return nil
    }
    k := d.Key(row)
    if k == "" {
        return nil
    }
    return d.Client.Del(context.Background(), k).Err()
}
