package binlog

import (
    "encoding/json"
    "errors"
    "os"
)

type Position struct {
    File string
    Pos  uint32
    GTID string
}

type PositionStore interface {
    Save(Position) error
    Load() (Position, error)
}

type FileStore struct {
    Path string
}

func (f *FileStore) Save(p Position) error {
    b, err := json.Marshal(p)
    if err != nil {
        return err
    }
    return os.WriteFile(f.Path, b, 0644)
}

func (f *FileStore) Load() (Position, error) {
    b, err := os.ReadFile(f.Path)
    if err != nil {
        if errors.Is(err, os.ErrNotExist) {
            return Position{}, nil
        }
        return Position{}, err
    }
    var p Position
    err = json.Unmarshal(b, &p)
    return p, err
}
