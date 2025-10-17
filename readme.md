# A 股信息

## 问题啊

- 复权因子，因此产生的三种价格，脑子疼

- 如何快速，而且不被封的拿到所有股票的日线级别的数据？不过第一次慢一点也可以，后来可以判断为交易日再 update，不知道是否有api可以拿到市场上所有的股票的信息

## 获取全部股票列表

`Ashare_data` 提供了 `list_all_stocks` 帮助函数，可以一次性抓取东财接口提供的全部股票列表：

```python
import asyncio

from Ashare_data import list_all_stocks


async def main() -> None:
    stocks = await list_all_stocks()
    print(f"共获取 {len(stocks)} 只股票")


if __name__ == "__main__":
    asyncio.run(main())
```

- 该接口底层调用东财的 `https://push2.eastmoney.com/api/qt/clist/get`，按页（`pn`）和每页数量（`pz`）逐页抓取。
- 若需要自定义超时、限流或代理，可以传入自建的 `httpx.AsyncClient`。
- 远端接口可能存在限流或数据延迟，若遇到失败请等待并重试，避免频繁请求导致被封禁。
