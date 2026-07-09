# ⚡ 生图注册机

ChatGPT 账号自动注册工具 — 从 Outlook 邮箱池接验证码，完成 OpenAI OAuth 注册，输出 access_token。

## 特性

- **全自动注册** — Outlook 邮箱池 → 接码 → 注册 → 输出 access_token
- **TLS 指纹伪装** — 基于 curl-cffi，绕过 Cloudflare 检测
- **Web UI** — Flask 管理界面，实时进度 + 一键下载 token
- **CLI + Web** — 支持命令行和浏览器两种方式

## 安装

```bash
git clone https://github.com/akihitohyh/shengtu-register.git
cd 生图注册机
python3 -m venv .venv
source .venv/bin/activate
pip install curl-cffi pyyaml
# Web UI 额外依赖
pip install flask
```

## 快速开始

### 1. 配置

```bash
cp config.example.yaml config.yaml
# 编辑 config.yaml，填入：
#   - 代理地址
#   - Outlook 邮箱池 (email----password----client_id----refresh_token)
```

邮箱池格式（一行一个）：
```
user@outlook.com----password----client_id----refresh_token
```

### 2. 运行

**CLI 模式**
```bash
python main.py -n 10          # 注册 10 个账号
python main.py -n 10 -v       # 详细日志
python main.py -n 5 -o at.txt # 自定义输出文件
```

**Web UI 模式**
```bash
python webui.py
# 浏览器打开 http://127.0.0.1:5800
```

## 输出

`access_tokens.txt` — 每行一个 access_token

## 配置文件

| 配置项 | 说明 |
|--------|------|
| `proxy.url` | HTTP/SOCKS5 代理地址 |
| `proxy.flaresolverr_url` | FlareSolverr 地址（可选，用于过 Cloudflare） |
| `registration.threads` | 并发线程数 |
| `registration.total` | 注册数量 |
| `mail.providers[].mailboxes` | Outlook 邮箱池 |

## 项目结构

```
├── main.py              # CLI 入口
├── webui.py             # Web UI (Flask)
├── config.example.yaml  # 配置示例
├── register/
│   ├── registrar.py     # 核心注册流程 (10 步 OAuth)
│   ├── mail_provider.py # 邮箱池管理
│   ├── session.py       # curl-cffi 会话 + Cloudflare 处理
│   └── headers.py       # 浏览器指纹请求头
└── utils/
    ├── pkce.py          # PKCE 生成
    ├── sentinel.py      # Sentinel PoW Token
    └── proxy.py         # 代理工具
```

## 注册流程

```
[1] 获取邮箱 → [2] Authorize (PKCE) → [3] 注册用户
→ [4] 发送验证码 → [5] 等待接码 → [6] 验证 OTP
→ [7] 创建账号 → [8] 交换 Token → 输出 access_token
```


