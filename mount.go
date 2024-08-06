/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
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

package cmd

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/object"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/urfave/cli/v2"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/metric"
	"github.com/juicedata/juicefs/pkg/usage"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/juicedata/juicefs/pkg/version"
	"github.com/juicedata/juicefs/pkg/vfs"
)

func cmdMount() *cli.Command {
	compoundFlags := [][]cli.Flag{
		mount_flags(),
		clientFlags(),
		shareInfoFlags(),
	}
	return &cli.Command{
		Name:      "mount",
		Action:    mount,
		Category:  "SERVICE",
		Usage:     "Mount a volume",
		ArgsUsage: "META-URL MOUNTPOINT",
		Description: `
Mount the target volume at the mount point.

Examples:
# Mount in foreground
$ juicefs mount redis://localhost /mnt/jfs

# Mount in background with password protected Redis
$ juicefs mount redis://:mypassword@localhost /mnt/jfs -d
# A safer alternative
$ META_PASSWORD=mypassword juicefs mount redis://localhost /mnt/jfs -d

# Mount with a sub-directory as root
$ juicefs mount redis://localhost /mnt/jfs --subdir /dir/in/jfs

# Enable "writeback" mode, which improves performance at the risk of losing objects
$ juicefs mount redis://localhost /mnt/jfs -d --writeback

# Enable "read-only" mode
$ juicefs mount redis://localhost /mnt/jfs -d --read-only

# Disable metadata backup
$ juicefs mount redis://localhost /mnt/jfs --backup-meta 0`,
		Flags: expandFlags(compoundFlags),
	}
}

func installHandler(mp string) {
	// Go will catch all the signals
	signal.Ignore(syscall.SIGPIPE)
	signalChan := make(chan os.Signal, 10)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	go func() {
		for {
			sig := <-signalChan
			logger.Infof("Received signal %s, exiting...", sig.String())
			go func() { _ = doUmount(mp, true) }()
			go func() {
				time.Sleep(time.Second * 3)
				logger.Warnf("Umount not finished after 3 seconds, force exit")
				os.Exit(1)
			}()
		}
	}()
}

func exposeMetrics(c *cli.Context, m meta.Meta, registerer prometheus.Registerer, registry *prometheus.Registry) string {
	var ip, port string
	//default set
	ip, port, err := net.SplitHostPort(c.String("metrics"))
	if err != nil {
		logger.Fatalf("metrics format error: %v", err)
	}

	m.InitMetrics(registerer)
	vfs.InitMetrics(registerer)
	go metric.UpdateMetrics(m, registerer)
	http.Handle("/metrics", promhttp.HandlerFor(
		registry,
		promhttp.HandlerOpts{
			// Opt into OpenMetrics to support exemplars.
			EnableOpenMetrics: false,
		},
	))
	registerer.MustRegister(collectors.NewBuildInfoCollector())
	meta.InitTikvMetrics(registerer)

	// If not set metrics addr,the port will be auto set
	if !c.IsSet("metrics") {
		// If only set consul, ip will auto set
		if c.IsSet("consul") {
			ip, err = utils.GetLocalIp(c.String("consul"))
			if err != nil {
				logger.Errorf("Get local ip failed: %v", err)
				return ""
			}
		}
	}

	ln, err := net.Listen("tcp", net.JoinHostPort(ip, port))
	if err != nil {
		// Don't try other ports on metrics set but listen failed
		if c.IsSet("metrics") {
			logger.Errorf("listen on %s:%s failed: %v", ip, port, err)
			return ""
		}
		// Listen port on 0 will auto listen on a free port
		ln, err = net.Listen("tcp", net.JoinHostPort(ip, "0"))
		if err != nil {
			logger.Errorf("Listen failed: %v", err)
			return ""
		}
	}

	go func() {
		if err := http.Serve(ln, nil); err != nil {
			logger.Errorf("Serve for metrics: %s", err)
		}
	}()

	metricsAddr := ln.Addr().String()
	logger.Infof("Prometheus metrics listening on %s", metricsAddr)
	return metricsAddr
}

func wrapRegister(mp, name string) (prometheus.Registerer, *prometheus.Registry) {
	registry := prometheus.NewRegistry() // replace default so only JuiceFS metrics are exposed
	registerer := prometheus.WrapRegistererWithPrefix("juicefs_",
		prometheus.WrapRegistererWith(prometheus.Labels{"mp": mp, "vol_name": name}, registry))
	registerer.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	registerer.MustRegister(collectors.NewGoCollector())
	return registerer, registry
}

func updateFormat(c *cli.Context) func(*meta.Format) {
	return func(format *meta.Format) {
		if c.IsSet("bucket") {
			format.Bucket = c.String("bucket")
		}
		if c.IsSet("storage") {
			format.Storage = c.String("storage")
		}
	}
}

func daemonRun(c *cli.Context, addr string, vfsConf *vfs.Config, m meta.Meta) {
	if runtime.GOOS != "windows" {
		if cd := c.String("cache-dir"); cd != "memory" {
			ds := utils.SplitDir(cd)
			for i, d := range ds {
				if strings.HasPrefix(d, "/") {
					continue
				} else if strings.HasPrefix(d, "~/") {
					if h, err := os.UserHomeDir(); err == nil {
						ds[i] = filepath.Join(h, d[1:])
					} else {
						logger.Fatalf("Expand user home dir of %s: %s", d, err)
					}
				} else {
					if ad, err := filepath.Abs(d); err == nil {
						ds[i] = ad
					} else {
						logger.Fatalf("Find absolute path of %s: %s", d, err)
					}
				}
			}
			for i, a := range os.Args {
				if a == cd || a == "--cache-dir="+cd {
					os.Args[i] = a[:len(a)-len(cd)] + strings.Join(ds, string(os.PathListSeparator))
				}
			}
		}
	}
	embeddedSchemes := []string{"sqlite3://", "badger://"}
	for _, es := range embeddedSchemes {
		if strings.HasPrefix(addr, es) {
			path := addr[len(es):]
			path2, err := filepath.Abs(path)
			if err == nil && path2 != path {
				for i, a := range os.Args {
					if a == addr {
						os.Args[i] = es + path2
					}
				}
			}
		}
	}
	// The default log to syslog is only in daemon mode.
	utils.InitLoggers(!c.Bool("no-syslog"))
	err := makeDaemon(c, vfsConf.Format.Name, vfsConf.Meta.MountPoint, m)
	if err != nil {
		logger.Fatalf("Failed to make daemon: %s", err)
	}
}

func getVfsConf(c *cli.Context, metaConf *meta.Config, format *meta.Format, chunkConf *chunk.Config) *vfs.Config {
	cfg := &vfs.Config{
		Meta:       metaConf,
		Format:     format,
		Version:    version.Version(),
		Chunk:      chunkConf,
		BackupMeta: duration(c.String("backup-meta")),
	}
	if cfg.BackupMeta > 0 && cfg.BackupMeta < time.Minute*5 {
		logger.Fatalf("backup-meta should not be less than 5 minutes: %s", cfg.BackupMeta)
	}
	return cfg
}

func registerMetaMsg(m meta.Meta, store chunk.ChunkStore, chunkConf *chunk.Config) {
	m.OnMsg(meta.DeleteSlice, func(args ...interface{}) error {
		return store.Remove(args[0].(uint64), int(args[1].(uint32)))
	})
	m.OnMsg(meta.CompactChunk, func(args ...interface{}) error {
		return vfs.Compact(*chunkConf, store, args[0].([]meta.Slice), args[1].(uint64))
	})
}

func prepareMp(mp string) {
	fi, err := os.Stat(mp)
	if !strings.Contains(mp, ":") && err != nil {
		if err := os.MkdirAll(mp, 0777); err != nil {
			if os.IsExist(err) {
				// a broken mount point, umount it
				_ = doUmount(mp, true)
			} else {
				logger.Fatalf("create %s: %s", mp, err)
			}
		}
	} else if err == nil {
		ino, _ := utils.GetFileInode(mp)
		if ino <= uint64(meta.RootInode) && fi.Size() == 0 {
			// a broken mount point, umount it
			_ = doUmount(mp, true)
		} else if ino == uint64(meta.RootInode) {
			logger.Warnf("%s is already mounted by juicefs, maybe you should umount it first.", mp)
		}
	}
}

func getMetaConf(c *cli.Context, mp string, readOnly bool) *meta.Config {
	cfg := &meta.Config{
		Retries:           c.Int("io-retries"),
		Strict:            true,
		ReadOnly:          readOnly,
		NoBGJob:           c.Bool("no-bgjob"),
		OpenCache:         time.Duration(c.Float64("open-cache") * 1e9),
		Heartbeat:         duration(c.String("heartbeat")),
		MountPoint:        mp,
		Subdir:            c.String("subdir"),
		CleanObjFileLever: c.Int("clean-object-data-lever"),
	}
	if cfg.Heartbeat < time.Second {
		logger.Warnf("heartbeat should not be less than 1 second")
		cfg.Heartbeat = time.Second
	}
	if cfg.Heartbeat > time.Minute*10 {
		logger.Warnf("heartbeat shouldd not be greater than 10 minutes")
		cfg.Heartbeat = time.Minute * 10
	}
	return cfg
}

func getChunkConf(c *cli.Context, format *meta.Format) *chunk.Config {
	chunkConf := &chunk.Config{
		BlockSize:  format.BlockSize * 1024,
		Compress:   format.Compression,
		HashPrefix: format.HashPrefix,

		GetTimeout:    time.Second * time.Duration(c.Int("get-timeout")),
		PutTimeout:    time.Second * time.Duration(c.Int("put-timeout")),
		MaxUpload:     c.Int("max-uploads"),
		MaxDeletes:    c.Int("max-deletes"),
		MaxRetries:    c.Int("io-retries"),
		Writeback:     c.Bool("writeback"),
		Prefetch:      c.Int("prefetch"),
		BufferSize:    c.Int("buffer-size") << 20,
		UploadLimit:   c.Int64("upload-limit") * 1e6 / 8,
		DownloadLimit: c.Int64("download-limit") * 1e6 / 8,
		UploadDelay:   duration(c.String("upload-delay")),

		CacheDir:       c.String("cache-dir"),
		CacheSize:      int64(c.Int("cache-size")),
		FreeSpace:      float32(c.Float64("free-space-ratio")),
		CacheMode:      os.FileMode(0600),
		CacheFullBlock: !c.Bool("cache-partial-only"),
		AutoCreate:     true,
	}
	if chunkConf.MaxUpload <= 0 {
		logger.Warnf("max-uploads should be greater than 0, set it to 1")
		chunkConf.MaxUpload = 1
	}
	if chunkConf.BufferSize <= 32<<20 {
		logger.Warnf("buffer-size should be more than 32 MiB")
		chunkConf.BufferSize = 32 << 20
	}

	if chunkConf.CacheDir != "memory" {
		ds := utils.SplitDir(chunkConf.CacheDir)
		for i := range ds {
			ds[i] = filepath.Join(ds[i], format.UUID)
		}
		chunkConf.CacheDir = strings.Join(ds, string(os.PathListSeparator))
	}

	chunkConf.SetRateLimitPriority()

	return chunkConf
}

func initBackgroundTasks(c *cli.Context, vfsConf *vfs.Config, metaConf *meta.Config, m meta.Meta, blob object.ObjectStorage, registerer prometheus.Registerer, registry *prometheus.Registry) {
	metricsAddr := exposeMetrics(c, m, registerer, registry)
	if c.IsSet("consul") {
		metric.RegisterToConsul(c.String("consul"), metricsAddr, vfsConf.Meta.MountPoint)
	}
	if !metaConf.ReadOnly && !metaConf.NoBGJob && vfsConf.BackupMeta > 0 {
		go vfs.Backup(m, blob, vfsConf.BackupMeta)
	}
	if !c.Bool("no-usage-report") {
		go usage.ReportUsage(m, version.Version())
	}
}

type storageHolder struct {
	object.ObjectStorage
	fmt meta.Format
}

func NewReloadableStorage(format *meta.Format, cli meta.Meta, patch func(*meta.Format)) (object.ObjectStorage, error) {
	if patch != nil {
		patch(format)
	}
	blob, err := createStorage(*format)
	if err != nil {
		return nil, err
	}
	holder := &storageHolder{
		ObjectStorage: blob,
		fmt:           *format, // keep a copy to find the change
	}
	cli.OnReload(func(new *meta.Format) {
		if patch != nil {
			patch(new)
		}
		old := &holder.fmt
		if new.Storage != old.Storage || new.Bucket != old.Bucket || new.AccessKey != old.AccessKey || new.SecretKey != old.SecretKey || new.SessionToken != old.SessionToken {
			logger.Infof("found new configuration: storage=%s bucket=%s ak=%s", new.Storage, new.Bucket, new.AccessKey)

			newBlob, err := createStorage(*new)
			if err != nil {
				logger.Warnf("object storage: %s", err)
				return
			}
			holder.ObjectStorage = newBlob
			holder.fmt = *new
		}
	})
	return holder, nil
}

func NewReloadRateLimit(format *meta.Format, cli meta.Meta, store chunk.ChunkStore, chunkConf *chunk.Config) {
	// backup last value, if not change, need not update
	lastUpLimit := format.VolumeUpLimit
	lastDownLimit := format.VolumeDownLimit
	lastMetaLimitConfStr := format.MetaInterfaceRateLimit

	// meta rate limit
	// init 'gets:10000,scanKeys:10000,scanValues:2000'
	conf := &meta.RateLimitConf{}

	// config not update && volume-upload-limit/volume-download-limit > 0 (init)
	if !chunkConf.UseMountUploadLimitConf && lastUpLimit > 0 {
		store.UpdateCachedStoreRateLimit(format.VolumeUpLimit, lastUpLimit, "upLimit")
	}
	if !chunkConf.UseMountDownloadLimitConf && lastDownLimit > 0 {
		store.UpdateCachedStoreRateLimit(format.VolumeDownLimit, lastDownLimit, "downLimit")
	}
	if lastMetaLimitConfStr != "" {
		err := checkMetaInterfaceRateLimit(lastMetaLimitConfStr, conf)
		if err != nil {
			logger.Fatalf(err.Error())
		}
		meta.InitMetaLimitManager(conf)
	}

	cli.OnReload(func(new *meta.Format) {
		if !chunkConf.UseMountUploadLimitConf {
			store.UpdateCachedStoreRateLimit(new.VolumeUpLimit, lastUpLimit, "upLimit")
			lastUpLimit = new.VolumeUpLimit
		}
		if !chunkConf.UseMountDownloadLimitConf {
			store.UpdateCachedStoreRateLimit(new.VolumeDownLimit, lastDownLimit, "downLimit")
			lastDownLimit = new.VolumeDownLimit
		}
		// meta rate limit config update
		if lastMetaLimitConfStr != new.MetaInterfaceRateLimit {
			handleMetaRateLimitUpdate(lastMetaLimitConfStr, new.MetaInterfaceRateLimit)
			lastMetaLimitConfStr = new.MetaInterfaceRateLimit
		}
	})
}

func mount(c *cli.Context) error {
	setup(c, 2)
	addr := c.Args().Get(0)
	mp := c.Args().Get(1)

	prepareMp(mp)
	metaConf := getMetaConf(c, mp, c.Bool("read-only") || utils.StringContains(strings.Split(c.String("o"), ","), "ro"))
	metaConf.CaseInsensi = strings.HasSuffix(mp, ":") && runtime.GOOS == "windows"
	metaCli := meta.NewClient(addr, metaConf)
	format, err := metaCli.Load(true)
	if err != nil {
		return err
	}

	token := handleTokenInput(format.Name)

	// check token
	if token == "" {
		err1 := clearTokenFile(format.Name)
		if err1 != nil {
			logger.Warnf("tokenFile or Dir clear error: %s", err1)
		}
		logger.Fatalf("token is required")
	}
	if token != format.TokenInfo.Token {
		err2 := clearTokenFile(format.Name)
		if err2 != nil {
			logger.Warnf("tokenFile or Dir clear error: %s", err2)
		}
		logger.Fatalf("token is error")
	}

	inode1CacheGctime := c.Float64("inode1-cache-gctime")
	inode1Cache := c.Float64("inode1-cache")
	if (inode1CacheGctime > 0 && inode1Cache > 0) || (inode1CacheGctime == 0 && inode1Cache == 0) {
		// continue
	} else {
		logger.Fatalf("inode1-cache-gctime and inode1-cache param error, must set together")
	}

	// Wrap the default registry, all prometheus.MustRegister() calls should be afterwards
	registerer, registry := wrapRegister(mp, format.Name)

	if !c.Bool("writeback") && c.IsSet("upload-delay") {
		logger.Warnf("delayed upload only work in writeback mode")
	}

	blob, err := NewReloadableStorage(format, metaCli, updateFormat(c))
	if err != nil {
		return fmt.Errorf("object storage: %s", err)
	}
	logger.Infof("Data use %s", blob)

	chunkConf := getChunkConf(c, format)
	store := chunk.NewCachedStore(blob, *chunkConf, registerer)
	registerMetaMsg(metaCli, store, chunkConf)

	vfsConf := getVfsConf(c, metaConf, format, chunkConf)

	if c.Bool("background") && os.Getenv("JFS_FOREGROUND") == "" {
		daemonRun(c, addr, vfsConf, metaCli)
	} else {
		go checkMountpoint(vfsConf.Format.Name, mp, c.String("log"), false)
	}

	removePassword(addr)
	err = metaCli.NewSession()
	if err != nil {
		logger.Fatalf("new session: %s", err)
	}

	installHandler(mp)
	v := vfs.NewVFS(vfsConf, metaCli, store, registerer, registry)
	initBackgroundTasks(c, vfsConf, metaConf, metaCli, blob, registerer, registry)

	if inode1CacheGctime > 0 {
		go vfs.CacheGC(time.Duration(inode1CacheGctime * 1e9))
	}
	if inode1Cache > 0 {
		vfs.Inode1CacheConfLock.Lock()
		vfs.Inode1CacheTimeOut = time.Duration(inode1Cache * 1e9)
		vfs.Inode1CacheConfLock.Unlock()
	}

	NewReloadRateLimit(format, metaCli, store, chunkConf)

	mount_main(v, c)
	return metaCli.CloseSession()
}

func handleMetaRateLimitUpdate(lastMetaLimitConfStr, newMetaLimitConfStr string) {
	// 将last做拆解
	lastConfBean := &meta.RateLimitConf{}
	if lastMetaLimitConfStr != "" {
		err := checkMetaInterfaceRateLimit(lastMetaLimitConfStr, lastConfBean)
		if err != nil {
			logger.Errorf(err.Error())
			return
		}
	}
	// 将new做拆解
	newConfBean := &meta.RateLimitConf{}
	if newMetaLimitConfStr != "" {
		err := checkMetaInterfaceRateLimit(newMetaLimitConfStr, newConfBean)
		if err != nil {
			logger.Errorf(err.Error())
			return
		}
	}
	meta.UpdateMetaLimitManager("gets", newConfBean.GetsLimitBurst, lastConfBean.GetsLimitBurst)
	meta.UpdateMetaLimitManager("scanKeys", newConfBean.ScanKeysLimitBurst, lastConfBean.ScanKeysLimitBurst)
	meta.UpdateMetaLimitManager("scanValues", newConfBean.ScanValuesLimitBurst, lastConfBean.ScanValuesLimitBurst)
}

func checkMetaInterfaceRateLimit(metaInterfaceRateLimit string, conf *meta.RateLimitConf) error {
	if metaInterfaceRateLimit == "" {
		return nil
	}
	splits := strings.Split(metaInterfaceRateLimit, ",")
	for _, s := range splits {
		kv := strings.Split(s, ":")
		if len(kv) != 2 {
			return fmt.Errorf("metaInterfaceRateLimit pattern is error")
		} else if kv[1] == "" || !utils.IsNum(kv[1]) {
			return fmt.Errorf("metaInterfaceRateLimit type is error, the value part is null or not number")
		}
		switch kv[0] {
		case "gets":
			v, err := strconv.Atoi(kv[1])
			if err != nil {
				return fmt.Errorf("metaInterfaceRateLimit:gets type is error, the value part cannot convert to int")
			}
			if conf != nil {
				conf.GetsLimitBurst = v
			}
		case "scanKeys":
			v, err := strconv.Atoi(kv[1])
			if err != nil {
				return fmt.Errorf("metaInterfaceRateLimit:scanKeys type is error, the value part cannot convert to int")
			}
			if conf != nil {
				conf.ScanKeysLimitBurst = v
			}
		case "scanValues":
			v, err := strconv.Atoi(kv[1])
			if err != nil {
				return fmt.Errorf("metaInterfaceRateLimit:scanValues type is error, the value part cannot convert to int")
			}
			if conf != nil {
				conf.ScanValuesLimitBurst = v
			}
		default:
			return fmt.Errorf("metaInterfaceRateLimit not support param: %s", kv[0])
		}
	}
	return nil
}

/**
 *	1.用户输入token
 *	2.写到隐藏文件，base64编码（文件存在则跳过）
 *	3.读取隐藏文件，解码
 *
 *  backup: 守护进程执行无法获取 os.Stdin 中内容，由于隐藏文件存在，会直接从文件中读取
 */
func handleTokenInput(volumeName string) string {
	dir, err := utils.GetHomeDir()
	if err != nil {
		logger.Fatalf("Get home dir error: %s", err)
	}
	tokenFilePathDir := dir + "/.../" + volumeName
	tokenFile := tokenFilePathDir + "/volume.to"
	if !utils.IsExist(tokenFile) {
		fmt.Print("请输入token > ")
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		tokenInput := scanner.Text()
		if err = scanner.Err(); err != nil {
			logger.Fatalf("token input error")
		}
		err = createTokenFile(tokenFilePathDir, tokenFile, tokenInput)
		if err != nil {
			logger.Fatalf("token handle error")
		}
	}
	encodeTokenBytes, err := ioutil.ReadFile(tokenFile)
	if err != nil {
		logger.Fatalf("token read error")
	}
	tokenBytes, err := base64.StdEncoding.DecodeString(string(encodeTokenBytes))
	if err != nil {
		logger.Fatalf("token decode error")
	}
	return string(tokenBytes)
}

func createTokenFile(tokenFilePathDir, tokenFile, tokenStr string) error {
	if !utils.IsExist(tokenFilePathDir) {
		err := os.MkdirAll(tokenFilePathDir, 0666)
		if err != nil {
			return err
		}
	}
	perm := os.FileMode(0666)

	// base64 encode
	encodeTokenStr := base64.StdEncoding.EncodeToString([]byte(tokenStr))
	err := os.WriteFile(tokenFile, []byte(encodeTokenStr), perm)
	if err != nil {
		return err
	}
	return nil
}