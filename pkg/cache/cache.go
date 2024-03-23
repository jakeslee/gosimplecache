package cache

import (
	"context"
	"github.com/VictoriaMetrics/fastcache"
	"github.com/go-redis/redis/v8"
	jsoniter "github.com/json-iterator/go"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"k8s.io/utils/lru"
	"reflect"
	"sync"
	"time"
)

type CacheSupplier func(ctx context.Context, key string) (interface{}, error)
type RedisSupplier func(ctx context.Context, key string) (string, error)
type CacheFilter func(key string, content interface{}) bool
type ResultConvertFunc func(content string) string

type CacheQueryBuilder struct {
	key             string
	optionalKeys    []string
	ttl             time.Duration
	withMem         bool // 默认不存内存
	onlyMem         bool
	withoutRedis    bool // 不查询 Redis
	supplier        CacheSupplier
	rSupplier       RedisSupplier
	filter          CacheFilter
	resultConverter ResultConvertFunc
}

const (
	DefaultTTL                 = time.Hour * 2
	EmptyResultCached          = "-Empty-"
	DefaultCacheBroadcastTopic = "kube:cache:bc"
)

var rdb *redis.Client

var (
	disabledCache = false
	// 1024MB max
	cache                        = fastcache.LoadFromFileOrNew("/tmp/kubegw-cache", 1024*1024*1024)
	Json                         = jsoniter.ConfigCompatibleWithStandardLibrary
	FilterNilOrEmpty CacheFilter = func(key string, content interface{}) bool {
		switch v := content.(type) {
		case nil:
			return false
		case string:
			if v == "" {
				return false
			}
		default:
			//if reflect.TypeOf(content).Kind() == reflect.Slice {
			//	return reflect.ValueOf(content).Len() > 0
			//}

			return true
		}

		return true
	}
	DoNothingSupplier CacheSupplier = func(ctx context.Context, key string) (interface{}, error) {
		return nil, nil
	}
	DefaultRedisSupplierFunc RedisSupplier = func(ctx context.Context, key string) (string, error) {
		val, err := rdb.Get(ctx, key).Result()

		if err != nil {
			logrus.WithError(err).WithField("key", key).
				Warn("redis get key with err")
		}

		return val, err
	}
	DefaultResultConverter ResultConvertFunc = func(content string) string {
		if content == EmptyResultCached {
			return ""
		}

		return content
	}
	Broadcast = NewBroadcast(DefaultCacheBroadcastTopic)
)

func Init(client *redis.Client) {
	rdb = client
}

func DisableCacheGlobal() {
	disabledCache = true
}

func (b *CacheQueryBuilder) Key(key string) *CacheQueryBuilder {
	if b.key == "" {
		b.key = key
	} else {
		b.optionalKeys = append(b.optionalKeys, key)
	}

	return b
}

func (b *CacheQueryBuilder) ExpiredAfterWrite(ttl time.Duration) *CacheQueryBuilder {
	b.ttl = ttl
	return b
}

func (b *CacheQueryBuilder) WithMem() *CacheQueryBuilder {
	b.withMem = true
	return b
}

func (b *CacheQueryBuilder) WithoutRedis() *CacheQueryBuilder {
	b.withoutRedis = true
	return b
}

func (b *CacheQueryBuilder) OnlyMem() *CacheQueryBuilder {
	b.onlyMem = true
	b.withMem = true
	return b
}

func (b *CacheQueryBuilder) FinalQueryFunc(supplier CacheSupplier) *CacheQueryBuilder {
	b.supplier = supplier
	return b
}

func (b *CacheQueryBuilder) RedisQueryFunc(supplier RedisSupplier) *CacheQueryBuilder {
	b.rSupplier = supplier
	return b
}

func (b *CacheQueryBuilder) Filter(filter CacheFilter) *CacheQueryBuilder {
	b.filter = filter
	return b
}

func (b *CacheQueryBuilder) WithResultConvertFunc(converter ResultConvertFunc) *CacheQueryBuilder {
	b.resultConverter = converter
	return b
}

func (b *CacheQueryBuilder) getIfValidTTL(jsonStr []byte) string {
	if len(jsonStr) <= 0 {
		return ""
	}

	results := gjson.GetManyBytes(jsonStr, "start", "value", "ttl")
	start, v := results[0].Time(), results[1].String()

	ttl := time.Duration(results[2].Int()) * time.Second

	if ttl == 0 {
		ttl = b.ttl / 3
	}

	// 内存缓存时间是 Redis 的 1/3
	if start.Add(ttl).Before(time.Now()) {
		cache.Del([]byte(b.key))
		return ""
	}

	return v
}

func (b *CacheQueryBuilder) WatchBroadcast(broadcast *CacheBroadcast) *CacheQueryBuilder {
	broadcast.AddWatcher(b.key, DefaultMemEvictFunc)
	return b
}

func (b *CacheQueryBuilder) WatchDefaultBroadcast() *CacheQueryBuilder {
	Broadcast.AddWatcher(b.key, DefaultMemEvictFunc)
	return b
}

func (b *CacheQueryBuilder) MemCache() *fastcache.Cache {
	return cache
}

func (b *CacheQueryBuilder) Query(ctx context.Context) (string, error) {
	if disabledCache {
		nContent, err := b.supplier(ctx, b.key)
		if err != nil {
			logrus.WithError(err).WithField("key", b.key).
				Warn("cache supplier err")
		}

		jsonStr, err := Json.Marshal(nContent)
		return string(jsonStr), err
	}

	// 1. 检查内存缓存
	if b.withMem {
		v := cache.Get(nil, []byte(b.key))

		if val := b.getIfValidTTL(v); val != "" {
			return b.resultConverter(val), nil
		}
	}

	// 2. 内存缓存没有，调用 Redis
	if !b.withoutRedis && !b.onlyMem {
		content, err := b.rSupplier(ctx, b.key)

		if err != nil {
			logrus.WithError(err).WithField("key", b.key).
				Warn("load from redis err")
		}

		if content != "" {
			result := b.resultConverter(content)
			if b.withMem {
				err = saveMemCache(b.key, content, b.ttl)
				if err != nil {
					return result, err
				}
			}

			return result, nil
		}
	}

	// 3. Redis 不存在，调用获取方法
	nContent, err := b.supplier(ctx, b.key)

	if err != nil {
		return "", err
	}

	// 只缓存过滤过的
	if !b.filter(b.key, nContent) {
		return "", nil
	}

	var result string

	if c, ok := nContent.(string); ok {
		result = c
	} else {
		jsonStr, err := Json.Marshal(nContent)
		result = string(jsonStr)

		if err != nil {
			return result, err
		}
	}

	// 缓存数据
	if !b.withoutRedis && !b.onlyMem {
		statusCmd := rdb.Set(ctx, b.key, result, b.ttl)

		if statusCmd.Err() != nil {
			return "", statusCmd.Err()
		}
	}

	if b.withMem {
		err = saveMemCache(b.key, result, b.ttl)
		if err != nil {
			return "", err
		}
	}

	return b.resultConverter(result), nil
}

func (b *CacheQueryBuilder) Evict(ctx context.Context) {
	cache.Del([]byte(b.key))
	rdb.Del(ctx, b.key)

	for _, key := range b.optionalKeys {
		cache.Del([]byte(key))
		rdb.Del(ctx, key)
	}
}

func (b *CacheQueryBuilder) EvictGlobal(ctx context.Context) {
	keys := []string{b.key}

	rdb.Del(ctx, b.key)
	for _, key := range b.optionalKeys {
		rdb.Del(ctx, key)
		keys = append(keys, key)
	}

	err := OmitEvent(ctx, &CacheEvent{
		Topic: DefaultCacheBroadcastTopic,
		Event: CacheEvict,
		Keys:  keys,
	})

	logrus.WithError(err).
		WithField("keys", keys).
		Info("evict keys")
}

func saveMemCache(key, content string, ttl time.Duration) error {
	m := map[string]interface{}{
		"start": time.Now(),
		"ttl":   ttl / time.Second,
		"value": content,
	}

	jsonStr, err := Json.Marshal(m)

	if err != nil {
		return err
	}

	cache.Set([]byte(key), jsonStr)
	return nil
}

func CacheBuilder() *CacheQueryBuilder {
	return &CacheQueryBuilder{
		ttl:             DefaultTTL,
		filter:          FilterNilOrEmpty,
		supplier:        DoNothingSupplier,
		rSupplier:       DefaultRedisSupplierFunc,
		resultConverter: DefaultResultConverter,
	}
}

func WithErrCompressed(fn func() (string, error)) string {
	r, err := fn()
	if err != nil {
		logrus.WithError(err).Warn("query db error")
	}

	return r
}

func WithErrorFunc(fn func(ctx context.Context, key string) (interface{}, error)) CacheSupplier {
	return func(ctx context.Context, key string) (interface{}, error) {
		i, err := fn(ctx, key)
		if err != nil {
			logrus.WithError(err).
				WithField("key", key).
				Warn("query error")
			return nil, err
		}
		return i, nil
	}
}

type CacheBroadcast struct {
	Topic     string
	channel   *redis.PubSub
	running   bool
	runningMu sync.Mutex
	entries   *lru.Cache
}

type CacheEventType int
type CacheEvent struct {
	Topic string         `json:"topic"`
	Event CacheEventType `json:"event"`
	Keys  []string       `json:"key"`
	Value string         `json:"value"`
}

type CacheEventHandleFunc func(key string, event *CacheEvent)

const (
	CacheCreate CacheEventType = iota
	CacheEvict
)

var (
	DefaultCacheEvictFunc CacheEventHandleFunc = func(key string, event *CacheEvent) {
		rdb.Del(context.Background(), event.Keys...)
	}
	DefaultMemEvictFunc CacheEventHandleFunc = func(key string, event *CacheEvent) {
		cache.Del([]byte(key))
	}
)

func NewCacheEvent(payload string) *CacheEvent {
	var s CacheEvent
	_ = Json.Unmarshal([]byte(payload), &s)

	return &s
}

func NewBroadcast(topic string) *CacheBroadcast {
	return &CacheBroadcast{
		Topic:   topic,
		entries: lru.New(5000),
	}
}

func (c *CacheBroadcast) Start() {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()

	if c.running {
		return
	}

	c.running = true
	pubSub := rdb.Subscribe(context.Background(), c.Topic)
	c.channel = pubSub

	go c.run()
}

func (c *CacheBroadcast) Stop() {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()

	c.running = false
	c.channel.Close()
}

func (c *CacheBroadcast) run() {
	for message := range c.channel.Channel() {
		go func(payload string) {
			event := NewCacheEvent(payload)
			for _, key := range event.Keys {
				if v, ok := c.entries.Get(key); ok {
					fns, ok := v.([]CacheEventHandleFunc)

					if !ok {
						return
					}

					for _, fn := range fns {
						fn(key, event)
					}
				}
			}
			logrus.Warnf("cache event processed: %+v", event)
		}(message.Payload)
	}
}

func (c *CacheBroadcast) AddWatcher(key string, handleFunc CacheEventHandleFunc) {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()

	if value, ok := c.entries.Get(key); ok {
		fns := value.([]CacheEventHandleFunc)
		hFn := reflect.ValueOf(handleFunc)

		for _, watchedFunc := range fns {
			wFn := reflect.ValueOf(watchedFunc)
			if hFn.Pointer() == wFn.Pointer() {
				return
			}
		}

		c.entries.Add(key, append(fns, handleFunc))
	} else {
		c.entries.Add(key, []CacheEventHandleFunc{handleFunc})
	}
}

func OmitEvent(ctx context.Context, event *CacheEvent) error {
	eventStr, err := Json.Marshal(event)
	if err != nil {
		return err
	}

	return rdb.Publish(ctx, event.Topic, eventStr).Err()
}
