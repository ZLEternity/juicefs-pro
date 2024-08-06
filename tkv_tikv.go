//go:build !notikv
// +build !notikv

/*
 * JuiceFS, Copyright 2021 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package meta

import (
	"context"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/time/rate"
	"net/url"
	"os"
	"strings"

	plog "github.com/pingcap/log"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/tikv/client-go/v2/config"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/txnutil"
)

var (
	opCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "operation_count",
		Help: "The number of times a method execute.",
		//ConstLabels: prometheus.Labels{"constname": "constvalue"},
	},
		[]string{"method_type"},
	)
	getTokenFromBucketErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "meta_token_limit_errors",
			Help: "Meta requests get token from rateLimit bucket error.",
		},
		[]string{"method"},
	)
)

func opMetrics(methodType string) {
	opCount.WithLabelValues(methodType).Add(1)
}

func metaRequestGetTokenErrorMetrics(methodType string) {
	getTokenFromBucketErrors.WithLabelValues(methodType).Add(1)
}

func InitTikvMetrics(reg prometheus.Registerer) {
	if reg != nil {
		reg.MustRegister(opCount)
		reg.MustRegister(getTokenFromBucketErrors)
	}
}

func init() {
	Register("tikv", newKVMeta)
	drivers["tikv"] = newTikvClient

}

type RateLimitConf struct {
	GetsLimitBurst       int
	ScanKeysLimitBurst   int
	ScanValuesLimitBurst int
}

type metaRateLimitManager struct {
	getsLimit       *rate.Limiter
	scanKeysLimit   *rate.Limiter
	scanValuesLimit *rate.Limiter
}

var limitManager = &metaRateLimitManager{}

func InitMetaLimitManager(conf *RateLimitConf) {
	if conf != nil && *conf != (RateLimitConf{}) {
		if conf.GetsLimitBurst > 0 {
			limitManager.getsLimit = rate.NewLimiter(rate.Limit(float64(conf.GetsLimitBurst)*0.85), conf.GetsLimitBurst)
		}
		if conf.ScanKeysLimitBurst > 0 {
			limitManager.scanKeysLimit = rate.NewLimiter(rate.Limit(float64(conf.ScanKeysLimitBurst)*0.85), conf.ScanKeysLimitBurst)
		}
		if conf.ScanValuesLimitBurst > 0 {
			limitManager.scanValuesLimit = rate.NewLimiter(rate.Limit(float64(conf.ScanValuesLimitBurst)*0.85), conf.ScanValuesLimitBurst)
		}
	}
}

func UpdateMetaLimitManager(interfaceName string, burstSize int, lastBurstSize int) {
	if burstSize == lastBurstSize {
		return
	}
	switch interfaceName {
	case "gets":
		if limitManager.getsLimit == nil && burstSize > 0 {
			limitManager.getsLimit = rate.NewLimiter(rate.Limit(float64(burstSize)*0.85), burstSize)
		} else if limitManager.getsLimit != nil {
			if burstSize > 0 {
				limitManager.getsLimit.SetLimit(rate.Limit(float64(burstSize) * 0.85))
				limitManager.getsLimit.SetBurst(burstSize)
			} else if lastBurstSize > 0 && burstSize == 0 {
				limitManager.getsLimit.SetLimit(rate.Inf)
			}
		}
	case "scanKeys":
		if limitManager.scanKeysLimit == nil && burstSize > 0 {
			limitManager.scanKeysLimit = rate.NewLimiter(rate.Limit(float64(burstSize)*0.85), burstSize)
		} else if limitManager.scanKeysLimit != nil {
			if burstSize > 0 {
				limitManager.scanKeysLimit.SetLimit(rate.Limit(float64(burstSize) * 0.85))
				limitManager.scanKeysLimit.SetBurst(burstSize)
			} else if lastBurstSize > 0 && burstSize == 0 {
				limitManager.scanKeysLimit.SetLimit(rate.Inf)
			}
		}
	case "scanValues":
		if limitManager.scanValuesLimit == nil && burstSize > 0 {
			limitManager.scanValuesLimit = rate.NewLimiter(rate.Limit(float64(burstSize)*0.85), burstSize)
		} else if limitManager.scanValuesLimit != nil {
			if burstSize > 0 {
				limitManager.scanValuesLimit.SetLimit(rate.Limit(float64(burstSize) * 0.85))
				limitManager.scanValuesLimit.SetBurst(burstSize)
			} else if lastBurstSize > 0 && burstSize == 0 {
				limitManager.scanValuesLimit.SetLimit(rate.Inf)
			}
		}
	default:
		logger.Errorf("method: UpdateMetaLimitManager, interfaceName error")
	}
}

func newTikvClient(addr string) (tkvClient, error) {
	var plvl string // TiKV (PingCap) uses uber-zap logging, make it less verbose
	switch logger.Level {
	case logrus.TraceLevel:
		plvl = "debug"
	case logrus.DebugLevel:
		plvl = "info"
	case logrus.InfoLevel, logrus.WarnLevel:
		plvl = "warn"
	case logrus.ErrorLevel:
		plvl = "error"
	default:
		plvl = "dpanic"
	}
	// "log level: debug, info, warn, error, fatal (default 'info')" ----> default 'info' 即 plvl = 'warn'
	// tikv go-client中 打印日志的级别设置 fatal TODO 优化
	plvl = "fatal"

	l, prop, _ := plog.InitLogger(&plog.Config{Level: plvl})
	plog.ReplaceGlobals(l, prop)

	tUrl, err := url.Parse("tikv://" + addr)
	if err != nil {
		return nil, err
	}
	config.UpdateGlobal(func(conf *config.Config) {
		q := tUrl.Query()
		conf.Security = config.NewSecurity(
			q.Get("ca"),
			q.Get("cert"),
			q.Get("key"),
			strings.Split(q.Get("verify-cn"), ","))
	})

	if tUrl.Query() != nil && tUrl.Query().Has("ca") && tUrl.Query().Has("cert") && tUrl.Query().Has("key") {
		// continue
	} else {
		cfg := config.DefaultConfig()
		cacertFilePath, certFilePath, keyFilePath, err1 := createCertFile()
		cfg.Security = config.NewSecurity(cacertFilePath, certFilePath, keyFilePath, []string{})
		if err1 != nil {
			return nil, err1
		}
		config.StoreGlobalConfig(&cfg)
	}
	pds := strings.Split(tUrl.Host, ",")
	for i, pd := range pds {
		if !strings.HasPrefix(pd, "http") {
			pds[i] = "https://" + pd
		}
	}
	client, err := txnkv.NewClient(pds)

	if err != nil {
		return nil, err
	}
	prefix := strings.TrimLeft(tUrl.Path, "/")
	return withPrefix(&tikvClient{client.KVStore}, append([]byte(prefix), 0xFD)), nil
}

func createCertFile() (string, string, string, error) {
	dir, err := utils.GetHomeDir()
	if err != nil {
		return "", "", "", err
	}
	tlsPathDir := dir + "/.xxx"
	cacertFile := tlsPathDir + "/xxx.xxx"
	certFile := tlsPathDir + "/xxx.xxx"
	keyFile := tlsPathDir + "/xxx.xxx"
	if utils.IsExist(cacertFile) && utils.IsExist(certFile) && utils.IsExist(keyFile) {
		return cacertFile, certFile, keyFile, nil
	}
	if !utils.IsExist(tlsPathDir) {
		err := os.MkdirAll(tlsPathDir, 0666)
		if err != nil {
			return "", "", "", err
		}
	}
	perm := os.FileMode(0666)
	err = os.WriteFile(cacertFile, []byte(cacert), perm)
	if err != nil {
		return "", "", "", err
	}
	err = os.WriteFile(certFile, []byte(cert), perm)
	if err != nil {
		return "", "", "", err
	}
	err = os.WriteFile(keyFile, []byte(key), perm)
	if err != nil {
		return "", "", "", err
	}
	return cacertFile, certFile, keyFile, nil
}

type tikvTxn struct {
	*tikv.KVTxn
}

func (tx *tikvTxn) get(key []byte) []byte {
	defer opMetrics("get")
	value, err := tx.Get(context.TODO(), key)
	if tikverr.IsErrNotFound(err) {
		return nil
	}
	if err != nil {
		panic(err)
	}
	return value
}

func (tx *tikvTxn) gets(keys ...[]byte) [][]byte {
	defer opMetrics("gets")
	if limitManager != nil && limitManager.getsLimit != nil {
		err1 := limitManager.getsLimit.Wait(context.TODO())
		if err1 != nil {
			metaRequestGetTokenErrorMetrics("gets")
			// 暂时仅记录异常metric，不使用panic，规避进程中断风险
			//panic(err1)
		}
	}
	ret, err := tx.BatchGet(context.TODO(), keys)
	if err != nil {
		panic(err)
	}
	values := make([][]byte, len(keys))
	for i, key := range keys {
		values[i] = ret[string(key)]
	}
	return values
}

func (tx *tikvTxn) scanRange0(begin, end []byte, limit int, filter func(k, v []byte) bool) map[string][]byte {
	if limit == 0 {
		return nil
	}

	it, err := tx.Iter(begin, end)
	if err != nil {
		panic(err)
	}
	defer it.Close()
	var ret = make(map[string][]byte)
	for it.Valid() {
		key := it.Key()
		value := it.Value()
		if filter == nil || filter(key, value) {
			ret[string(key)] = value
			if limit > 0 {
				if limit--; limit == 0 {
					break
				}
			}
		}
		if err = it.Next(); err != nil {
			panic(err)
		}
	}
	return ret
}

func (tx *tikvTxn) scanRange(begin, end []byte) map[string][]byte {
	defer opMetrics("scanRange")
	return tx.scanRange0(begin, end, -1, nil)
}

func (tx *tikvTxn) scanKeys(prefix []byte) [][]byte {
	defer opMetrics("scanKeys")
	if limitManager != nil && limitManager.scanKeysLimit != nil {
		err1 := limitManager.scanKeysLimit.Wait(context.TODO())
		if err1 != nil {
			metaRequestGetTokenErrorMetrics("scanKeys")
			//panic(err1)
		}
	}
	it, err := tx.Iter(prefix, nextKey(prefix))
	if err != nil {
		panic(err)
	}
	defer it.Close()
	var ret [][]byte
	for it.Valid() {
		ret = append(ret, it.Key())
		if err = it.Next(); err != nil {
			panic(err)
		}
	}
	return ret
}

// scanValues 传入的参数 prefix = "volumeNameý" + 其他标记，例如："volumeNameýD"
func (tx *tikvTxn) scanValues(prefix []byte, limit int, filter func(k, v []byte) bool) map[string][]byte {
	defer opMetrics("scanValues")
	if limitManager != nil && limitManager.scanValuesLimit != nil {
		err1 := limitManager.scanValuesLimit.Wait(context.TODO())
		if err1 != nil {
			metaRequestGetTokenErrorMetrics("scanValues")
			//panic(err1)
		}
	}
	return tx.scanRange0(prefix, nextKey(prefix), limit, filter)
}

func (tx *tikvTxn) exist(prefix []byte) bool {
	defer opMetrics("exist")
	it, err := tx.Iter(prefix, nextKey(prefix))
	if err != nil {
		panic(err)
	}
	defer it.Close()
	return it.Valid()
}

func (tx *tikvTxn) set(key, value []byte) {
	defer opMetrics("set")
	if err := tx.Set(key, value); err != nil {
		panic(err)
	}
}

func (tx *tikvTxn) ctripSet(key, value []byte) {
	ctripKey := append([]byte("$"), key...)
	tx.set(ctripKey, value)
}

func (tx *tikvTxn) ctripDels(keys ...[]byte) {
	// need not implement
}

func (tx *tikvTxn) append(key []byte, value []byte) []byte {
	new := append(tx.get(key), value...)
	tx.set(key, new)
	return new
}

func (tx *tikvTxn) incrBy(key []byte, value int64) int64 {
	buf := tx.get(key)
	new := parseCounter(buf)
	if value != 0 {
		new += value
		tx.set(key, packCounter(new))
	}
	return new
}

func (tx *tikvTxn) dels(keys ...[]byte) {
	defer opMetrics("dels")
	for _, key := range keys {
		if err := tx.Delete(key); err != nil {
			panic(err)
		}
	}
}

type tikvClient struct {
	client *tikv.KVStore
}

func (c *tikvClient) name() string {
	return "tikv"
}

func (c *tikvClient) shouldRetry(err error) bool {
	return strings.Contains(err.Error(), "write conflict") || strings.Contains(err.Error(), "TxnLockNotFound")
}

func (c *tikvClient) txn(f func(kvTxn) error) (err error) {
	tx, err := c.client.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if r := recover(); r != nil {
			fe, ok := r.(error)
			if ok {
				err = fe
			} else {
				err = errors.Errorf("tikv client txn func error: %v", r)
			}
		}
	}()
	if err = f(&tikvTxn{tx}); err != nil {
		return err
	}
	if !tx.IsReadOnly() {
		tx.SetEnable1PC(true)
		tx.SetEnableAsyncCommit(true)
		err = tx.Commit(context.Background())
	}
	return err
}

func (c *tikvClient) scan(prefix []byte, handler func(key, value []byte)) error {
	defer opMetrics("scan")
	ts, err := c.client.CurrentTimestamp("global")
	if err != nil {
		return err
	}
	snap := c.client.GetSnapshot(ts)
	snap.SetScanBatchSize(10240)
	snap.SetNotFillCache(true)
	snap.SetPriority(txnutil.PriorityLow)
	it, err := snap.Iter(prefix, nextKey(prefix))
	if err != nil {
		return err
	}
	defer it.Close()
	for it.Valid() {
		handler(it.Key(), it.Value())
		if err = it.Next(); err != nil {
			return err
		}
	}
	return nil
}

func (c *tikvClient) reset(prefix []byte) error {
	_, err := c.client.DeleteRange(context.Background(), prefix, nextKey(prefix), 1)
	return err
}

func (c *tikvClient) close() error {
	return c.client.Close()
}

var cacert = `-----BEGIN CERTIFICATE-----
...
-----END CERTIFICATE-----`

var cert = `-----BEGIN CERTIFICATE-----
...
-----END CERTIFICATE-----`

var key = `-----BEGIN RSA PRIVATE KEY-----
...
-----END RSA PRIVATE KEY-----`
