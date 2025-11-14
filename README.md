# cacheflow

MySQL→Redis 缓存一致性库。在你的服务进程中启动后，监听 binlog 并自动维护缓存（默认删除）。

项目简洁文档：https://blog.csdn.net/m0_73337964/article/details/154839238?spm=1001.2014.3001.5501

## 安装

```
go get github.com/xzhHas/cacheflow
```

## 在项目里使用（放在哪里）

- 单体服务：在 `main()` 启动阶段调用，进程退出前 `Stop()`。
- 微服务：在服务生命周期的初始化处调用，优雅退出时 `Stop()`。

## 最小示例（删缓存）

```go
package main

import "github.com/xzhHas/cacheflow"

func main() {
    s, _ := cacheflow.StartSyncer(cacheflow.Config{
        MySQL: cacheflow.MySQLConfig{Addr: "127.0.0.1:3306", User: "root", Password: "", Flavor: "mysql", ServerID: 1001},
        Redis: cacheflow.RedisConfig{Addr: "127.0.0.1:6379", DB: 0},
        Tables: []cacheflow.TableConfig{{DB: "user", Table: "users", Strategy: cacheflow.CacheAside}},
    })
    defer s.Stop()
}
```

- 不提供 `Key` 时默认使用 `db:table:id`
- 监听整个库：`{DB:"user", Table:"*", Strategy: cacheflow.CacheAside}`

## 写穿（写缓存）

```go
s, _ := cacheflow.StartSyncer(cacheflow.Config{
  MySQL: cacheflow.MySQLConfig{Addr: "127.0.0.1:3306", User: "root", Password: "", Flavor: "mysql", ServerID: 1001},
  Redis: cacheflow.RedisConfig{Addr: "127.0.0.1:6379", DB: 0},
  Tables: []cacheflow.TableConfig{{DB: "user", Table: "users", Strategy: cacheflow.WriteThrough, TTLSeconds: 300}},
})
defer s.Stop()
```

## 事件（类似 canal）

```go
s.Subscribe(func(e cacheflow.Event) { /* e.Action/e.Schema/e.Table/e.Before/e.After */ })
// 或者
for e := range s.Events() { /* ... */ }
```

## 可选：失败重试（RabbitMQ）

```go
s, _ := cacheflow.StartSyncer(cacheflow.Config{
  MySQL: cacheflow.MySQLConfig{Addr: "127.0.0.1:3306", User: "root", Password: "", Flavor: "mysql", ServerID: 1001},
  Redis: cacheflow.RedisConfig{Addr: "127.0.0.1:6379", DB: 0},
  Tables: []cacheflow.TableConfig{{DB: "user", Table: "users", Strategy: cacheflow.CacheAside}},
  Retry: cacheflow.RetryConfig{
    Enable: true,
    MaxAttempts: 3,
    BackoffMillis: 500,
    MQ: cacheflow.MQConfig{
      URL: "amqp://guest:guest@127.0.0.1:5672/", Exchange: "cacheflow", Queue: "cacheflow.retry", RoutingKey: "retry",
    },
  },
})
defer s.Stop()
```

## 关键点

- 策略：`CacheAside`（删缓存，默认） 或 `WriteThrough`（写缓存，支持 `TTLSeconds`）
- Key：默认 `db:table:id`；无 `id` 可用 `KeyFields` 组合；可加 `Prefix`
- server_id：未配置自动生成；可用 `CACHEFLOW_SERVER_ID=12345` 覆盖
