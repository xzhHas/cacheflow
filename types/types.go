package types

// CacheStrategy 定义缓存维护策略
// CacheAside：删除缓存；业务读取时重建（推荐，强最终一致性）
// WriteThrough：写穿，将变更数据写入缓存；读延迟更友好
type CacheStrategy int

const (
    CacheAside CacheStrategy = iota
    WriteThrough
)

// KeyFunc 用于构造 Redis key，参数为 binlog 行数据
type KeyFunc func(row map[string]any) string

// TableConfig 描述某个表的缓存维护配置
// - DB/Table：库名/表名；Table 支持 "*" 代表该库所有表
// - Key：构造 key 的函数；若为空，默认使用 db:table:id
// - Strategy：缓存策略；默认推荐 CacheAside
// - KeyFields：当没有 id 字段时，使用此字段列表生成 key
// - Prefix：可选 key 前缀，如 "user:"；仅在默认 key 生效
// - TTLSeconds：在 WriteThrough 下写入缓存的过期时间（秒）
type TableConfig struct {
    DB         string
    Table      string
    Key        KeyFunc
    Strategy   CacheStrategy
    KeyFields  []string
    Prefix     string
    TTLSeconds int
}

// Config 为整个同步器的配置
// - MySQL：binlog 监听信息
// - Redis：缓存连接信息
// - Retry：失败重试队列（可选）
// - PositionPath：位点持久化文件路径
// - Tables：需要维护一致性的表清单
type Config struct {
    MySQL struct {
        Addr     string
        User     string
        Password string
        Flavor   string
        GTID     bool
        ServerID uint32
    }
    Redis struct {
        Addr     string
        Password string
        DB       int
    }
    Retry        RetryConfig
    PositionPath string
    Tables       []TableConfig
}

// Handler 为用户自定义处理器，可覆盖默认策略
// 注意：为保证一致性，建议谨慎使用写入缓存的逻辑
type Handler interface {
    OnInsert(row map[string]any)
    OnUpdate(before, after map[string]any)
    OnDelete(row map[string]any)
}

// RetryConfig 定义队列重试相关配置
// - Enable：是否启用重试
// - MaxAttempts：最大重试次数
// - BackoffMillis：退避时间（毫秒）
// - MQ：队列连接与路由信息
type RetryConfig struct {
    Enable        bool
    MaxAttempts   int
    BackoffMillis int
    MQ            struct {
        URL        string
        Exchange   string
        Queue      string
        RoutingKey string
        DLX        string
        DLQ        string
    }
}

type Event struct {
    Schema string
    Table  string
    Action string
    Before map[string]any
    After  map[string]any
}