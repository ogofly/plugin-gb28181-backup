package gb28181

import (
	"fmt"
	"log"
	"sync"
	"time"

	"net/http"
	_ "net/http/pprof"

	"go.uber.org/zap"
)

/*
通过 key (deviceId + channelId + sn) 唯一区分一次请求和响应
并将其关联起来，以实现异步响应的目的
*/
type recordCache struct {
	expire      time.Duration
	cache       map[string]records
	subscribers map[string]subscriber
	sync.RWMutex
}

type records struct {
	time     time.Time
	sum      int
	finished bool
	list     []*Record
}
type subscriber struct {
	callback  func(records)
	timeout   time.Duration
	startTime time.Time
}

func NewRecordCache() *recordCache {
	c := &recordCache{
		expire:      time.Second * 60,
		cache:       make(map[string]records),
		subscribers: make(map[string]subscriber),
	}
	go c.watchTimeout()
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	return c
}

func (c *recordCache) watchTimeout() {
	tick := time.NewTicker(time.Second)
	for {
		<-tick.C
		for k, s := range c.subscribers {
			if time.Since(s.startTime) > s.timeout {
				if r, ok := c.cache[k]; ok {
					c.notify(k, r)
				}
			}
		}
		for k, r := range c.cache {
			if time.Since(r.time) > c.expire {
				delete(c.cache, k)
			}
		}
	}
}

func (c *recordCache) Put(deviceId, channelId string, sn int, sum int, record []*Record) {
	key, r := c.doPut(deviceId, channelId, sn, sum, record)
	if r.finished {
		c.notify(key, r)
	}
}

func (c *recordCache) doPut(deviceId, channelId string, sn, sum int, record []*Record) (key string, r records) {
	c.Lock()
	defer c.Unlock()
	key = recordKey(deviceId, channelId, sn)
	if v, ok := c.cache[key]; ok {
		r = v
	} else {
		r = records{time: time.Now(), sum: sum, list: make([]*Record, 0)}
	}

	r.list = append(r.list, record...)
	if len(r.list) == sum {
		r.finished = true
	}
	c.cache[key] = r
	GB28181Plugin.Logger.Debug("Put record cache",
		zap.String("key", key),
		zap.Int("sum", sum),
		zap.Int("count", len(r.list)))
	return
}

func (c *recordCache) SubscribeOnce(deviceId, channelId string, sn int, sub subscriber) {
	key := recordKey(deviceId, channelId, sn)
	c.Lock()
	defer c.Unlock()
	c.subscribers[key] = sub
}

func (c *recordCache) notify(key string, r records) {
	if s, ok := c.subscribers[key]; ok {
		s.callback(r)
	}
	c.Lock()
	defer c.Unlock()
	delete(c.subscribers, key)
	delete(c.cache, key)
	GB28181Plugin.Logger.Debug("record cache notify", zap.String("key", key))
}

func recordKey(deviceId, channelId string, sn int) string {
	return fmt.Sprintf("%s-%s-%d", deviceId, channelId, sn)
}

var RecordCache = NewRecordCache()
