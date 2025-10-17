# A 股信息

该目录提供了抓取 A 股行情数据的基础设施，核心模块如下：

- `Ashare_data/providers/`: 面向具体行情源的适配层，继承 `BaseProvider` 并实现抓取逻辑。
- `Ashare_data/fetchers/`: 聚合各类 Provider，提供统一的抓取入口。
- `Ashare_data/storage/`: 封装持久化能力，当前实现了 SQLite 存储。
- `Ashare_data/scheduler/`: 负责初始化全量抓取以及每日增量更新流程。
- `Ashare_data/utils/`: 日志、限流/重试以及配置等通用工具。

## 使用方式

```python
import asyncio
from datetime import date

from Ashare_data.fetchers import DailyFetcher
from Ashare_data.providers import EastMoneyProvider, QQProvider
from Ashare_data.scheduler.initializer import run_initial_load
from Ashare_data.storage import SQLiteStorage

async def main():
    storage = SQLiteStorage()
    await storage.initialize()

    fetcher = DailyFetcher([
        QQProvider(priority=10),
        EastMoneyProvider(priority=5),
    ])

    await run_initial_load(
        storage=storage,
        fetcher=fetcher,
        symbols=["1.000001"],
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
    )

    await fetcher.close()

asyncio.run(main())
```

## 扩展说明

- 新增数据源时继承 `BaseProvider` 并实现 `fetch_daily`，必要时在 `utils/` 添加新的通用能力。
- 若需要其它存储后端，可参考 `storage/sqlite.py` 的结构实现 `insert`/`upsert` 行为。
- 自定义交易日历可通过设置环境变量 `ASHARE_CALENDAR` 为 CSV 或纯文本文件路径。
