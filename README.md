# IPTV Auto Selector

一个稳定、轻量化的 IPTV 自动优选项目。它会从多个订阅源收集频道链接，做频道别名归一、黑白名单过滤、HTTP 小流量测速、m3u8 分片验证、历史成功率评分，最后输出适合播放器使用的订阅文件。

## 输出文件

运行完成后会生成：

- `output/iptv.m3u`：标准 M3U 订阅文件
- `output/tv.txt`：DIYP / TvBox 常用文本格式
- `output/report.json`：本次检测报告
- `config/history.json`：历史成功率和测速记录

## 快速使用

1. 把你的 IPTV 源写入 `config/sources.txt`，每行一个地址。
2. 根据需要修改 `config/alias.txt`、`config/allow_list.txt`、`config/blacklist.txt`、`config/template_output.txt`。
3. 本地运行：

```bash
pip install -r requirements.txt
python main.py
```

4. 推送到 GitHub 后，进入 `Actions` 页面手动运行一次，或等待定时任务自动运行。

## 配置说明

### `config/sources.txt`

每行一个订阅源地址，支持常见 `m3u`、`txt` 格式：

```text
https://example.com/iptv.m3u
https://example.com/live.txt
```

### `config/alias.txt`

频道别名配置，格式：

```text
标准名,别名1,别名2,别名3
```

示例：

```text
CCTV-1,CCTV1,CCTV-1综合,央视一套
```

### `config/allow_list.txt`

频道白名单。留空表示输出所有频道。如果你只想输出指定频道，就每行写一个标准频道名：

```text
CCTV-1
CCTV-5
```

### `config/blacklist.txt`

支持三种规则：

```text
频道名          # 精确屏蔽某个频道
*关键词         # 屏蔽包含关键词的频道
url*关键词      # 屏蔽 URL 包含关键词的线路
```

示例：

```text
*购物
*广告
url*/ad/
url*advert
```

### `config/template_output.txt`

控制输出顺序、显示名称和分组。格式：

```text
标准频道名|显示名称|分组名称
```

示例：

```text
CCTV-1|CCTV-1 综合|央视频道
CCTV-5|CCTV-5 体育|央视频道
```

如果这个文件为空，程序会按解析到的频道自然输出。

## 筛选逻辑

项目默认不依赖 `ffmpeg` 或 `ffprobe`，适合 GitHub Actions 轻量运行。

核心判断包括：

- 链接是否命中 URL 黑名单
- 普通直链是否能读取到有效字节
- `.m3u8` 是否能解析 playlist
- `.m3u8` 是否至少有一个分片可读取
- 本次读取速度
- 本次请求耗时
- 历史成功率
- 每个频道尽量保留不同域名的线路，避免同源全部失效

综合评分大致为：

```text
可用性 40%
读取速度 25%
响应耗时 15%
历史成功率 20%
```

## 环境变量

可以在 GitHub Actions 或本地运行时调整：

```text
SOURCE_DOWNLOAD_WORKERS       下载订阅源并发，默认 6
STREAM_TEST_WORKERS           测速并发，默认 10
KEEP_PER_CHANNEL              每个频道保留线路数，默认 3
PRETEST_MAX_PER_CHANNEL       每频道最多进入测速的候选线路数，默认 80
CONNECT_TIMEOUT               连接超时，默认 3 秒
READ_TIMEOUT                  读取超时，默认 6 秒
DIRECT_READ_BYTES             直链最多读取字节数，默认 512KB
SEGMENT_READ_BYTES            m3u8 分片最多读取字节数，默认 256KB
MIN_VALID_BYTES               判断有效的最小字节数，默认 16KB
```

示例：

```bash
STREAM_TEST_WORKERS=16 KEEP_PER_CHANNEL=5 python main.py
```

## GitHub Actions

项目已包含 `.github/workflows/iptv.yml`。

默认触发方式：

- 每天自动运行一次
- 支持 `workflow_dispatch` 手动运行

运行成功后会自动提交这些文件：

```text
output/iptv.m3u
output/tv.txt
output/report.json
config/history.json
```

如果你 fork 或新建仓库后第一次运行，请确认仓库的 `Actions` 权限允许写入：

```text
Settings → Actions → General → Workflow permissions → Read and write permissions
```

## 设计取舍

这个项目本着“稳定、轻量、适合自动运行”的原则，没有默认引入画质探测工具。它能筛出更可能稳定播放的优质线路，但不能百分百判断真实分辨率和码率。

如果你后续想进一步追求“真实画质最好”，可以在当前结果基础上追加可选 `ffprobe` 精测阶段，只对每个频道前几条候选线路检测分辨率、帧率和码率。

## 建议

- `config/sources.txt` 不要放太多质量很差的源，否则测速时间会变长。
- `KEEP_PER_CHANNEL` 建议保留 `3-5`，不要只保留 1 条。
- 如果 GitHub Actions 测出来的结果和你本地观看体验差异很大，可以在本地设备、NAS 或软路由上运行同一项目。
- 不建议一次失败就永久拉黑链接，本项目默认只用历史成功率降权，不自动写入黑名单。
