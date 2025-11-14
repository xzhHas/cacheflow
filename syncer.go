package cacheflow

import (
    "context"
    "fmt"
    "hash/fnv"
    "os"
    "strconv"
    "strings"
    "time"

    "github.com/redis/go-redis/v9"
    "github.com/xzhHas/cacheflow/internal/binlog"
    "github.com/xzhHas/cacheflow/internal/mq"
    "github.com/xzhHas/cacheflow/internal/strategy"
    "github.com/xzhHas/cacheflow/types"
)

type Config = types.Config
type TableConfig = types.TableConfig
type CacheStrategy = types.CacheStrategy
type Handler = types.Handler
type KeyFunc = types.KeyFunc
type Event = types.Event

// CacheAside：删除缓存；业务读取时重建（推荐，强最终一致性）
const CacheAside CacheStrategy = types.CacheAside
// WriteThrough：写穿；变更后数据写入缓存（读延迟更友好）
const WriteThrough CacheStrategy = types.WriteThrough

type Syncer struct {
    cfg            Config
    rc             redis.Cmdable
    listener       *binlog.Listener
    defaultHandler Handler
    tableHandlers  map[string]Handler
    tableConfs     map[string]TableConfig
    serverID       uint32
    posStore       binlog.PositionStore
    retryEnabled   bool
    maxAttempts    int
    backoffMillis  int
    producer       mq.Producer
    consumer       mq.Consumer
    ctx            context.Context
    cancel         context.CancelFunc
    eventsCh       chan types.Event
    callbacks      []func(types.Event)
}

func New() *Syncer {
    return &Syncer{
        tableHandlers: make(map[string]Handler),
        tableConfs:    make(map[string]TableConfig),
        serverID:      1001,
        eventsCh:      make(chan types.Event, 1024),
    }
}

func (s *Syncer) SetConfig(cfg Config) {
    s.cfg = cfg
    s.tableConfs = make(map[string]TableConfig)
    for _, t := range cfg.Tables {
        k := fmt.Sprintf("%s.%s", t.DB, t.Table)
        if t.Key == nil {
            db := t.DB
            tb := t.Table
            keys := append([]string{}, t.KeyFields...)
            prefix := t.Prefix
            t.Key = func(row map[string]any) string {
                if v, ok := row["id"]; ok {
                    if prefix != "" {
                        return fmt.Sprintf("%s%s:%s:%v", prefix, db, tb, v)
                    }
                    return fmt.Sprintf("%s:%s:%v", db, tb, v)
                }
                if len(keys) > 0 {
                    vals := make([]string, 0, len(keys))
                    for _, f := range keys {
                        if vv, ok := row[f]; ok {
                            vals = append(vals, fmt.Sprintf("%v", vv))
                        }
                    }
                    if len(vals) == len(keys) {
                        if prefix != "" {
                            return fmt.Sprintf("%s%s:%s:%s", prefix, db, tb, strings.Join(vals, ":"))
                        }
                        return fmt.Sprintf("%s:%s:%s", db, tb, strings.Join(vals, ":"))
                    }
                }
                return ""
            }
        }
        s.tableConfs[k] = t
    }
    s.retryEnabled = cfg.Retry.Enable
    if cfg.Retry.MaxAttempts <= 0 {
        s.maxAttempts = 3
    } else {
        s.maxAttempts = cfg.Retry.MaxAttempts
    }
    if cfg.Retry.BackoffMillis <= 0 {
        s.backoffMillis = 500
    } else {
        s.backoffMillis = cfg.Retry.BackoffMillis
    }
    if cfg.MySQL.ServerID > 0 {
        s.serverID = cfg.MySQL.ServerID
    } else {
        s.serverID = autoServerID(cfg.MySQL.Addr)
    }
}

func (s *Syncer) RegisterHandler(h Handler) {
    s.defaultHandler = h
}

func (s *Syncer) RegisterTableHandler(db, table string, h Handler) {
    k := fmt.Sprintf("%s.%s", db, table)
    s.tableHandlers[k] = h
}

func (s *Syncer) SetRedisClient(client redis.Cmdable) { s.rc = client }
func (s *Syncer) SetMQ(producer mq.Producer, consumer mq.Consumer) { s.producer = producer; s.consumer = consumer }
func (s *Syncer) Subscribe(cb func(types.Event)) { s.callbacks = append(s.callbacks, cb) }
func (s *Syncer) Events() <-chan types.Event { return s.eventsCh }

func StartSyncer(cfg Config) (*Syncer, error) {
    s := New()
    s.SetConfig(cfg)
    if err := s.Start(); err != nil {
        return nil, err
    }
    return s, nil
}

func (s *Syncer) Start() error {
    if s.rc == nil {
        s.rc = redis.NewClient(&redis.Options{
            Addr:     s.cfg.Redis.Addr,
            Password: s.cfg.Redis.Password,
            DB:       s.cfg.Redis.DB,
        })
    }
    include := make([]string, 0, len(s.cfg.Tables))
    for _, t := range s.cfg.Tables {
        if t.Table == "*" {
            include = append(include, "^"+escapeRegex(t.DB)+"\\..+$")
        } else {
            include = append(include, "^"+escapeRegex(fmt.Sprintf("%s.%s", t.DB, t.Table))+"$")
        }
    }
    posPath := s.cfg.PositionPath
    if posPath == "" {
        posPath = "cacheflow.pos"
    }
    ps := &binlog.FileStore{Path: posPath}
    l, err := binlog.NewListener(
        s.cfg.MySQL.Addr,
        s.cfg.MySQL.User,
        s.cfg.MySQL.Password,
        s.cfg.MySQL.Flavor,
        include,
        s.serverID,
        s,
        ps,
    )
    if err != nil {
        return err
    }
    s.listener = l
    s.posStore = ps
    s.ctx, s.cancel = context.WithCancel(context.Background())
    if s.retryEnabled && s.cfg.Retry.MQ.URL != "" {
        rmq, err := mq.NewRabbitMQ(s.cfg.Retry.MQ.URL, s.cfg.Retry.MQ.Exchange, s.cfg.Retry.MQ.Queue, s.cfg.Retry.MQ.RoutingKey)
        if err == nil {
            s.producer = rmq
            s.consumer = rmq
            _ = s.consumer.Start(func(m mq.Message) error {
                time.Sleep(time.Duration(s.backoffMillis) * time.Millisecond)
                return s.retryExecute(m)
            })
        }
    }
    return s.listener.Start(s.ctx)
}

func (s *Syncer) Stop() error {
    if s.listener != nil {
        _ = s.listener.Stop()
    }
    if s.consumer != nil {
        _ = s.consumer.Stop()
    }
    if s.cancel != nil {
        s.cancel()
    }
    if c, ok := s.rc.(interface{ Close() error }); ok {
        _ = c.Close()
    }
    return nil
}

func (s *Syncer) OnInsert(schema string, table string, row map[string]any) {
    s.dispatch(schema, table, func(h Handler) { h.OnInsert(row) }, func(tc TableConfig) {
        var err error
        switch tc.Strategy {
        case WriteThrough:
            err = (&strategy.WriteThroughStrategy{Client: s.rc, Key: tc.Key, TTLSeconds: tc.TTLSeconds}).OnInsert(row)
        default:
            err = (&strategy.DeleteStrategy{Client: s.rc, Key: tc.Key}).OnInsert(row)
        }
        if err != nil {
            s.enqueueRetry("insert", schema, table, nil, row, 0)
        }
        s.emit("insert", schema, table, nil, row)
    })
}

func (s *Syncer) OnUpdate(schema string, table string, before map[string]any, after map[string]any) {
    s.dispatch(schema, table, func(h Handler) { h.OnUpdate(before, after) }, func(tc TableConfig) {
        var err error
        switch tc.Strategy {
        case WriteThrough:
            err = (&strategy.WriteThroughStrategy{Client: s.rc, Key: tc.Key, TTLSeconds: tc.TTLSeconds}).OnUpdate(before, after)
        default:
            err = (&strategy.DeleteStrategy{Client: s.rc, Key: tc.Key}).OnUpdate(before, after)
        }
        if err != nil {
            s.enqueueRetry("update", schema, table, before, after, 0)
        }
        s.emit("update", schema, table, before, after)
    })
}

func (s *Syncer) OnDelete(schema string, table string, row map[string]any) {
    s.dispatch(schema, table, func(h Handler) { h.OnDelete(row) }, func(tc TableConfig) {
        var err error
        switch tc.Strategy {
        case WriteThrough:
            err = (&strategy.WriteThroughStrategy{Client: s.rc, Key: tc.Key, TTLSeconds: tc.TTLSeconds}).OnDelete(row)
        default:
            err = (&strategy.DeleteStrategy{Client: s.rc, Key: tc.Key}).OnDelete(row)
        }
        if err != nil {
            s.enqueueRetry("delete", schema, table, row, nil, 0)
        }
        s.emit("delete", schema, table, row, nil)
    })
}

func (s *Syncer) OnPosSynced(file string, pos uint32, gtid string) {}

func (s *Syncer) dispatch(schema, table string, useHandler func(Handler), useDefault func(TableConfig)) {
    k := fmt.Sprintf("%s.%s", schema, table)
    if h, ok := s.tableHandlers[k]; ok && h != nil {
        useHandler(h)
        return
    }
    if s.defaultHandler != nil {
        useHandler(s.defaultHandler)
        return
    }
    if tc, ok := s.tableConfs[k]; ok {
        useDefault(tc)
        return
    }
    if tc, ok := s.tableConfs[fmt.Sprintf("%s.%s", schema, "*")]; ok {
        useDefault(tc)
    }
}

func (s *Syncer) emit(action, schema, table string, before, after map[string]any) {
    e := types.Event{Schema: schema, Table: table, Action: action, Before: before, After: after}
    for _, cb := range s.callbacks {
        cb(e)
    }
    select {
    case s.eventsCh <- e:
    default:
    }
}

func (s *Syncer) enqueueRetry(action, schema, table string, before, after map[string]any, attempts int) {
    if !s.retryEnabled || s.producer == nil {
        return
    }
    _ = s.producer.Publish(mq.Message{Schema: schema, Table: table, Action: action, Before: before, After: after, Attempts: attempts})
}

func (s *Syncer) retryExecute(m mq.Message) error {
    if m.Attempts >= s.maxAttempts {
        return nil
    }
    tc, ok := s.tableConfs[fmt.Sprintf("%s.%s", m.Schema, m.Table)]
    if !ok {
        return nil
    }
    var err error
    switch m.Action {
    case "insert":
        switch tc.Strategy {
        case WriteThrough:
            err = (&strategy.WriteThroughStrategy{Client: s.rc, Key: tc.Key}).OnInsert(m.After)
        default:
            err = (&strategy.DeleteStrategy{Client: s.rc, Key: tc.Key}).OnInsert(m.After)
        }
    case "update":
        switch tc.Strategy {
        case WriteThrough:
            err = (&strategy.WriteThroughStrategy{Client: s.rc, Key: tc.Key}).OnUpdate(m.Before, m.After)
        default:
            err = (&strategy.DeleteStrategy{Client: s.rc, Key: tc.Key}).OnUpdate(m.Before, m.After)
        }
    case "delete":
        switch tc.Strategy {
        case WriteThrough:
            err = (&strategy.WriteThroughStrategy{Client: s.rc, Key: tc.Key}).OnDelete(m.Before)
        default:
            err = (&strategy.DeleteStrategy{Client: s.rc, Key: tc.Key}).OnDelete(m.Before)
        }
    }
    if err != nil {
        m.Attempts++
        s.enqueueRetry(m.Action, m.Schema, m.Table, m.Before, m.After, m.Attempts)
    }
    return nil
}

func escapeRegex(s string) string {
    out := make([]rune, 0, len(s))
    for _, r := range s {
        switch r {
        case '.', '\\', '+', '*', '?', '^', '$', '[', ']', '(', ')', '{', '}', '|':
            out = append(out, '\\', r)
        default:
            out = append(out, r)
        }
    }
    return string(out)
}

func autoServerID(addr string) uint32 {
    if v := os.Getenv("CACHEFLOW_SERVER_ID"); v != "" {
        if n, err := strconv.ParseUint(v, 10, 32); err == nil && n > 0 {
            return uint32(n)
        }
    }
    host, _ := os.Hostname()
    h := fnv.New32a()
    _, _ = h.Write([]byte(host))
    _, _ = h.Write([]byte(addr))
    base := uint32(10000)
    return base + h.Sum32()
}
func Tables(db string, strategy CacheStrategy, names ...string) []TableConfig {
    tcs := make([]TableConfig, 0, len(names))
    for _, n := range names {
        tcs = append(tcs, TableConfig{DB: db, Table: n, Strategy: strategy})
    }
    return tcs
}