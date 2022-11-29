// 基于rsmq，实现消费-再生产的过程。
// 从单个流单个组读数据，调用handler，向单个流写数据。
// handler是自定义的
package rsmqworker

import (
	"time"

	"github.com/go-redis/redis/v9"
	"github.com/micplus/rsmq"
)

// 消费-再生产的Worker
type Worker struct {
	cli     *redis.Client
	CStream string
	PStream string

	Consumer *rsmq.Consumer
	Producer *rsmq.Producer

	handler func(*Worker) error
}

// Worker的各项配置
type WorkerConfig struct {
	RedisAddr string
	PStream   string
	PMaxLen   int64
	CStream   string
	CGroup    string
	CName     string
	CBlock    time.Duration
	CMaxIdle  time.Duration
}

func NewWorker(cfg *WorkerConfig) *Worker {
	w := &Worker{
		PStream: cfg.PStream,
		CStream: cfg.CStream,
	}
	w.cli = redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
	})
	rsmq.GroupCreateMkStream(w.cli, w.CStream, cfg.CGroup)
	w.Consumer = rsmq.NewConsumer(w.cli, cfg.CStream,
		cfg.CGroup, cfg.CName, cfg.CBlock, cfg.CMaxIdle)
	w.Producer = rsmq.NewProducer(w.cli, cfg.PMaxLen, true)
	return w
}

// handler代表一次消费-再生产过程，仅支持一个handler
func (w *Worker) SetHandler(f func(*Worker) error) {
	w.handler = f
}

// handler返回error时退出循环
func (w *Worker) Work() error {
	for {
		if err := w.WorkOnce(); err != nil {
			return err
		}
	}
}

func (w *Worker) WorkOnce() error {
	return w.handler(w)
}
