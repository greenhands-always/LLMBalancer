# ULRDS — Unified LLM Resource Dispatcher & Scheduler

异构大模型统一调度与负载均衡系统。通过异步任务队列、智能分级调度和自动化错误归类，最大化利用零散/受限的 LLM API 资源。

## 架构

```
Client (Producer)
    │
    ▼  POST /api/v1/task/submit
┌─────────────────────────┐
│   FastAPI Server        │
│   ├─ Auth (Bearer)      │
│   ├─ Priority Queue     │  ◄── SQLite 持久化
│   ├─ Scheduler Loop     │
│   │   └─ Provider Match │
│   ├─ Error Classifier   │  ◄── 规则引擎 + LLM 兜底
│   └─ Callback Service   │  ◄── 指数退避重试
└─────────────────────────┘
    │
    ▼  HTTP (OpenAI-compatible)
┌─────────────────────────┐
│  Provider Pool          │
│  ├─ GPT-4o API          │  Level 5
│  ├─ Claude API          │  Level 4
│  ├─ Local Qwen-32B      │  Level 3
│  └─ Free API            │  Level 1-2
└─────────────────────────┘
```

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置 Provider

编辑 `providers.yaml`，添加你的 LLM API 信息：

```yaml
providers:
  - provider_id: "my-gpt4"
    base_url: "https://api.openai.com/v1"
    api_key: "sk-xxx"
    model_name: "gpt-4o"
    level: 5              # 能力等级 (任务 level<=5 均可分配到此 provider)
    rpm_limit: 10
    max_concurrent: 3
    timeout_seconds: 120
```

### 3. 设置环境变量（可选）

```bash
export ULRDS_AUTH_TOKEN="your-secret-token"   # 默认: change-me-in-production
export ULRDS_DB_PATH="ulrds.db"               # 默认: ulrds.db
export ULRDS_PORT="8000"                      # 默认: 8000
export ULRDS_LOG_LEVEL="INFO"                 # 默认: INFO
```

### 4. 启动服务

```bash
python main.py
```

或使用 uvicorn：

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

## API 接口

所有接口（除 `/health`）需要 Bearer Token 认证：

```
Authorization: Bearer your-secret-token
```

### 提交任务

```bash
curl -X POST http://localhost:8000/api/v1/task/submit \
  -H "Authorization: Bearer change-me-in-production" \
  -H "Content-Type: application/json" \
  -d '{
    "model_config": {"level": 3, "type": "text"},
    "priority": 10,
    "payload": {"messages": [{"role": "user", "content": "你好"}]},
    "callback_url": "http://my-service/webhook",
    "allow_downgrade": false,
    "max_wait_seconds": 600
  }'
```

### 查询任务状态

```bash
curl http://localhost:8000/api/v1/task/{task_id} \
  -H "Authorization: Bearer change-me-in-production"
```

### 查看 Provider 状态

```bash
curl http://localhost:8000/api/v1/providers \
  -H "Authorization: Bearer change-me-in-production"
```

### 重置被禁用的 Provider

```bash
curl -X POST http://localhost:8000/api/v1/providers/{provider_id}/reset \
  -H "Authorization: Bearer change-me-in-production"
```

### 系统统计

```bash
curl http://localhost:8000/api/v1/stats \
  -H "Authorization: Bearer change-me-in-production"
```

## 核心概念

### 能力等级 (Level)

Level 是正整数，无硬编码上限，数字越大代表能力越强。常见分级参考：

| Level | 定义 | 典型 Provider |
|-------|------|--------------|
| 1 | 简单文本处理 | 免费 API、小模型 |
| 2 | 基础对话 | GPT-3.5 级别 |
| 3 | 中等复杂度 | 本地 32B 模型 |
| 4 | 复杂推理 | Claude/GPT-4 |
| 5 | 高级 Coding | GPT-4o/Claude-3.5 |

任务提交的 `level` 表示**最低能力要求**：level=3 会优先分配给 level=3 的 provider，如果 3 没有额度则自动升级到 4、5。

### Provider 状态机

- **ACTIVE**: 正常可用
- **COOLDOWN**: 暂时不可用，到达 `next_available_time` 后自动恢复（懒检查）
- **DISABLED**: 永久停用（如账号被封），需手动调用 reset 接口恢复

### 调度策略

- **最低匹配**: 任务 level 表示最低要求，`provider.level >= task.level` 即可匹配
- **优先节省**: 优先选择满足条件的最低等级 provider（节省高级资源）
- **自动升级**: level=3 的请求若没有 3 级 provider 可用，自动尝试 4、5 级
- **可选降级**: 设置 `allow_downgrade: true` 时，允许使用低于请求 level 的 Provider
- **负载均衡**: 同等候选中选择当前并发最低的 Provider
- **智能退避**: Provider 全忙时 scheduler 自动退避（0.5s → 1s → 2s → ... → 10s），provider 释放后立即唤醒

### 错误归因

| 类别 | 触发条件 | Provider 动作 | 任务动作 |
|------|---------|-------------|---------|
| QUOTA_EXCEEDED | 429 + quota 关键词 | COOLDOWN 10min | 重试 |
| SERVER_BUSY | 5xx | COOLDOWN 1min | 重试 |
| AUTH_FAILED | 401/403 | DISABLED | 重试(换 Provider) |
| CONTENT_FILTER | 400 + filter 关键词 | 不变 | 标记失败 |
| UNKNOWN | 其他 | COOLDOWN 5min | 重试 |

## 技术栈

- **Python** + **FastAPI** + **asyncio**
- **SQLite** (WAL 模式) 任务持久化
- **aiohttp** 调用 LLM API 和回调
- 内存优先级堆 + SQLite 双重保障
