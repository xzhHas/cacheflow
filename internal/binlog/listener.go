package binlog

import (
    "context"

    "github.com/go-mysql-org/go-mysql/canal"
    "github.com/go-mysql-org/go-mysql/mysql"
    "github.com/go-mysql-org/go-mysql/replication"
)

type Sink interface {
    OnInsert(schema string, table string, row map[string]any)
    OnUpdate(schema string, table string, before map[string]any, after map[string]any)
    OnDelete(schema string, table string, row map[string]any)
    OnPosSynced(file string, pos uint32, gtid string)
}

type Listener struct {
    c        *canal.Canal
    sink     Sink
    posStore PositionStore
}

type eventHandler struct {
    sink     Sink
    posStore PositionStore
}

func NewListener(addr, user, password, flavor string, include []string, serverID uint32, sink Sink, posStore PositionStore) (*Listener, error) {
    cfg := &canal.Config{
        Addr:              addr,
        User:              user,
        Password:          password,
        Flavor:            flavor,
        ServerID:          serverID,
        IncludeTableRegex: include,
    }
    c, err := canal.NewCanal(cfg)
    if err != nil {
        return nil, err
    }
    h := &eventHandler{sink: sink, posStore: posStore}
    c.SetEventHandler(h)
    l := &Listener{c: c, sink: sink, posStore: posStore}
    return l, nil
}

func (l *Listener) Start(ctx context.Context) error {
    go func() {
        _ = l.c.Run()
    }()
    return nil
}

func (l *Listener) Stop() error {
    l.c.Close()
    return nil
}

func (h *eventHandler) OnRow(e *canal.RowsEvent) error {
    schema := e.Table.Schema
    table := e.Table.Name
    switch e.Action {
    case canal.InsertAction:
        for _, r := range e.Rows {
            h.sink.OnInsert(schema, table, rowMap(e, r))
        }
    case canal.UpdateAction:
        for i := 0; i < len(e.Rows); i += 2 {
            if i+1 >= len(e.Rows) {
                break
            }
            before := rowMap(e, e.Rows[i])
            after := rowMap(e, e.Rows[i+1])
            h.sink.OnUpdate(schema, table, before, after)
        }
    case canal.DeleteAction:
        for _, r := range e.Rows {
            h.sink.OnDelete(schema, table, rowMap(e, r))
        }
    }
    return nil
}

func rowMap(e *canal.RowsEvent, row []interface{}) map[string]any {
    m := make(map[string]any, len(e.Table.Columns))
    for i, c := range e.Table.Columns {
        if i < len(row) {
            m[c.Name] = row[i]
        }
    }
    return m
}

func (h *eventHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
    if h.posStore != nil {
        g := ""
        if set != nil {
            g = set.String()
        }
        _ = h.posStore.Save(Position{File: pos.Name, Pos: pos.Pos, GTID: g})
        h.sink.OnPosSynced(pos.Name, pos.Pos, g)
    }
    return nil
}

func (h *eventHandler) OnRotate(header *replication.EventHeader, r *replication.RotateEvent) error { return nil }
func (h *eventHandler) OnTableChanged(header *replication.EventHeader, schema string, table string) error { return nil }
func (h *eventHandler) OnDDL(header *replication.EventHeader, nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
    return nil
}
func (h *eventHandler) OnXID(header *replication.EventHeader, nextPos mysql.Position) error { return nil }
func (h *eventHandler) OnGTID(header *replication.EventHeader, e mysql.BinlogGTIDEvent) error { return nil }
func (h *eventHandler) OnRowsQueryEvent(e *replication.RowsQueryEvent) error { return nil }
func (h *eventHandler) String() string { return "cacheflow" }
