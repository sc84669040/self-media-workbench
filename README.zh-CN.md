# self-media workbench 中文说明

这是一个本地优先的自媒体工作台，用来做内容发现、正文抓取、主题整理、素材检索和写作辅助。

如果你是第一次使用，先按“最短启动流程”跑起来。API Key、Cookie、外部工具、正式数据库路径都可以晚一点再配置。

## 适合谁用

- 想把 RSS、YouTube、B 站、X、公众号等来源统一整理到本地的人。
- 想用 NightHawk 风格的事件库观察热点、主题和素材的人。
- 想把素材检索、选题包、写作草稿放在一个本地页面里完成的人。

## 最短启动流程

先准备：

- Windows 电脑。
- Python 3.11 或更新版本。
- Git。
- 浏览器，Edge 或 Chrome 都可以。

打开 PowerShell，执行：

```powershell
cd <你的目录>\self-media-workbench
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
copy config\examples\local.example.yaml config\local\local.yaml
python scripts\check_env.py
python scripts\init_runtime.py --profile sample
python scripts\start_local.py
```

然后打开：

```text
http://127.0.0.1:8791
```

第一次启动建议使用 sample 模式。它不需要 API Key、Cookie、RSSHub、X 登录信息或私人知识库，能先确认页面和基础流程都能跑起来。

如果 PowerShell 提示不能执行 `Activate.ps1`，可以不激活虚拟环境，直接这样安装和启动：

```powershell
.\.venv\Scripts\python -m pip install -e ".[dev]"
.\.venv\Scripts\python scripts\check_env.py
.\.venv\Scripts\python scripts\init_runtime.py --profile sample
.\.venv\Scripts\python scripts\start_local.py
```

## 页面入口

- `/`：入口页面，适合从这里进入各个模块。
- `/config`：配置中心，集中管理路径、模型、密钥、来源、定时任务和外部工具。
- `/create`：创作工作台，用于选题、素材和写作。
- `/search`：检索页面，用于查找本地事件、主题和素材。

每个主要页面都应该能返回入口页或切换到配置页。如果你只记一个地址，记住 `http://127.0.0.1:8791/`。

## 配置文件在哪里

你只需要改这两个本地文件：

```text
config/local/local.yaml
.env
```

其中：

- `config/local/local.yaml` 放路径、端口、来源、定时任务等本机配置。
- `.env` 放 API Key、Cookie 环境变量、外部工具路径等敏感内容。
- `config/default.yaml` 是公开默认值，通常不要直接改。
- `config/examples/local.example.yaml` 是示例模板，可以重新复制一份。

这些本地私有文件已经被 Git 忽略，不应该上传到 GitHub。

## 新手先填什么

第一次使用时，建议只确认这几项：

```yaml
paths:
  runtime_dir: ${repo_root}/runtime
  event_radar_db_path: ${runtime_dir}/event_radar.db
  create_studio_db_path: ${runtime_dir}/create_studio.db

services:
  creative_studio:
    host: 127.0.0.1
    port: 8791
```

默认会把运行数据放到 `runtime/`，这对新手最安全。如果你已经有正式数据库，再把路径改成你的正式库位置。

注意：正式数据库路径、私人知识库路径、Cookie 路径不要写进公开文件，只写在 `config/local/local.yaml` 或 `.env`。

## 配置页怎么理解

配置页看起来项目多，是因为它把所有高级能力都收在一个地方。新手可以按下面理解。

### 数据库与路径

这里决定程序读写哪些本地文件。

- NightHawk 事件库：热点、事件、来源数据所在的 SQLite 数据库。
- 创作索引库：创作工作台用来检索素材和选题的数据。
- 镜像库：用于某些同步或只读场景，可先保持默认。
- 知识库路径：你的本地 Markdown 笔记或素材库路径。

如果不确定，先用默认 `runtime/`。确认流程跑通后，再切到正式库。

### 内容来源

内容来源不是必须马上配置。它控制程序从哪里收集内容，例如 RSS、YouTube、公众号、X 账号等。

新手建议：

- 先保留默认 RSS。
- 暂时关闭需要 Cookie 或账号的来源。
- 等本地页面跑通后，再逐个开启渠道。

### 定时任务

定时任务里的数字不是“数据库编号”，而是任务运行规则。

- `enabled`：是否启用这个任务。
- `interval_minutes`：每隔多少分钟运行一次。例如 `20` 表示每 20 分钟跑一次。
- `lookback_hours`：每次回看过去多少小时的内容。例如 `48` 表示检查最近 48 小时。
- `concurrency`：并发数量。新手保持 `1`。
- `retry_count`：失败后重试次数。新手保持 `0` 或 `1`。

常见任务：

- `collector`：抓取来源列表里的新内容。
- `body_worker`：补抓正文。
- `topic_pipeline`：整理事件和主题。
- `index_sync`：同步创作索引。
- `notifier`：通知任务。

新手建议只保留 `index_sync` 开启，其它需要账号、Cookie 或网络环境的任务先关闭。

### 外部工具

外部工具不是必须的。只有当你需要更完整的视频、音频或平台抓取能力时才配置。

- `yt-dlp`：用于 YouTube、B 站等视频信息或字幕抓取。
- `ffmpeg`：用于音视频处理。
- `FunASR`：用于语音转文字。
- X/Twitter 工具：用于某些 X 数据抓取流程。

如果只是体验创作工作台和 sample 数据，可以全部留空。

### 模型与密钥

模型和密钥可以简化理解成两类：

- 自动填充：帮助把素材整理成候选选题或结构。
- 写作模型：根据素材生成草稿。

不填 Key 也能运行，只是写作和自动生成能力会处于 mock 或 disabled 模式。

如果要启用模型，建议把 Key 放进 `.env`：

```text
OPENAI_API_KEY=
CREATE_STUDIO_WRITING_API_KEY=
CREATE_STUDIO_AUTOFILL_API_KEY=
```

不要把真实 Key 写进 README、公开配置或提交到 GitHub。

## 启动和停止

启动默认服务：

```powershell
python scripts\start_local.py
```

只启动创作工作台：

```powershell
python scripts\start_local.py --creative-only
```

同时启动可选的抓取服务：

```powershell
python scripts\start_local.py --with-fetch-hub
```

停止服务：回到 PowerShell 窗口按 `Ctrl+C`。

## 运行定时任务

只跑一次：

```powershell
python scripts\scheduler.py --once
```

持续运行：

```powershell
python scripts\scheduler.py
```

如果你还没有配置账号、Cookie、来源和正式数据库，不建议一开始就打开所有定时任务。

## 上传 GitHub 前检查

每次公开发布前建议执行：

```powershell
python scripts\scan_secrets.py --verbose
git status --short --ignored
```

确认下面这些文件没有被提交：

```text
.env
config/local/local.yaml
runtime/
*.db
cookies
tokens
```

如果密钥曾经出现在已提交文件里，不要只删除文件，要去对应平台轮换密钥。

## 常见问题

### 页面打不开

确认 PowerShell 里 `python scripts\start_local.py` 还在运行，并检查端口是不是 `8791`。

### 配置页保存后没有变化

确认当前编辑的是 `config/local/local.yaml`，或者检查是否设置了 `SELF_MEDIA_CONFIG_PATH`。如果设置了这个环境变量，配置页会编辑它指定的文件。

### 抓取功能不可用

大多数平台抓取需要 Cookie、账号、外部工具或代理。先用 sample 模式确认基础流程，再逐项配置。

### 写作功能不可用

检查 `creative_studio.writing.provider` 和 `.env` 里的 `CREATE_STUDIO_WRITING_API_KEY`。如果 provider 是 `disabled`，写作生成会关闭。

### 不知道该不该用代理

代理只影响需要访问外网的平台和 GitHub 上传。能直连就不用；直连失败时，再在当前终端或当前仓库设置代理，避免影响系统全局配置。

## 推荐学习顺序

1. 用 sample 模式启动页面。
2. 打开 `/config` 熟悉路径、来源、定时任务。
3. 打开 `/search` 看本地检索是否正常。
4. 打开 `/create` 体验选题和写作流程。
5. 再接入正式数据库、知识库、API Key 和平台来源。

这样做比较稳：先确认工作台可运行，再逐步打开高级能力。
