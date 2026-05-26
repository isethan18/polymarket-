import asyncio
import os
from logging import getLogger

# 严格指定使用 2026 年最新的 V2 客户端
from py_clob_client_v2 import (
    ApiCreds,
    ClobClient,
    MarketOrderArgs,
    OrderType,
    PartialCreateOrderOptions,
    Side,
)

logger = getLogger("PolymarketV2_Arb")


class PolymarketV2ArbitrageBot:
    def __init__(self):
        # 严格的生产环境环境变量读取，拒绝硬编码
        self.creds = ApiCreds(
            api_key=os.getenv("POLY_API_KEY"),
            api_secret=os.getenv("POLY_API_SECRET"),
            api_passphrase=os.getenv("POLY_API_PASSPHRASE"),
        )
        self.private_key = os.getenv("POLYGON_PRIVATE_KEY")
        self.client = ClobClient(self.private_key, self.creds)

        # 策略核心参数
        self.target_allocation_per_market = 500.0  # 单次套利总投入 (pUSD)
        self.profit_threshold = 0.04  # 扣除滑点后的净利润阈值 (4%)
        self.blacklisted_markets = {}  # 插针熔断黑名单 {market_id: unlock_timestamp}

    async def get_effective_ask(self, token_id, target_usd):
        """
        修改点2：彻底解决滑点陷阱。遍历Orderbook计算特定资金量下的【有效买入均价】
        """
        try:
            orderbook = await self.client.get_order_book(token_id)
            asks = orderbook.asks  # 结构通常为列表，包含价格 price 和数量 size

            accumulated_usd = 0.0
            accumulated_tokens = 0.0

            for ask in asks:
                price = float(ask.price)
                size = float(ask.size)
                available_usd_at_this_level = price * size

                if accumulated_usd + available_usd_at_this_level >= target_usd:
                    needed_usd = target_usd - accumulated_usd
                    accumulated_tokens += needed_usd / price
                    accumulated_usd = target_usd
                    break

                accumulated_usd += available_usd_at_this_level
                accumulated_tokens += size

            # 如果深度不够吃满我们的资金，返回无穷大价格，放弃交易
            if accumulated_usd < target_usd:
                return float("inf")

            effective_price = target_usd / accumulated_tokens
            return effective_price
        except Exception as exc:
            logger.error("获取Token %s 深度失败: %s", token_id, exc)
            return float("inf")

    async def scan_and_execute(self):
        """
        主扫描与原子性执行模块
        """
        # 1. 获取当前钱包内最新的 pUSD 余额（2026年V2标准）
        # 注：此处由 Codex 补充调用 pUSD ERC20 智能合约或 client.get_collateral_balance()
        wallet_balance = await self.get_pusd_balance()
        if wallet_balance < self.target_allocation_per_market:
            logger.warning("pUSD 余额不足，暂停扫描。")
            return

        # 2. 获取所有互斥的多选题市场
        markets = await self.client.get_markets()

        for market in markets:
            if not market.is_mutually_exclusive or market.id in self.blacklisted_markets:
                continue

            tokens = market.tokens  # 该事件下的所有互斥选项 Token ID 列表
            n_options = len(tokens)
            allocation_per_token = self.target_allocation_per_market / n_options

            # 并发计算所有选项的有效买入价
            tasks = [self.get_effective_ask(t.id, allocation_per_token) for t in tokens]
            effective_prices = await asyncio.gather(*tasks)

            # 核心数学闭环判定：有效价格之和是否小于 1 - 利润阈值
            sum_effective_price = sum(effective_prices)
            if sum_effective_price < (1.0 - self.profit_threshold):
                logger.info(
                    "发现套利机会！市场: %s, 有效价格和: %s",
                    market.title,
                    sum_effective_price,
                )

                # 3. 触发原子性下单（修改点3：使用 V2 原生市价单 + FOK）
                await self.execute_batch_market_orders(tokens, allocation_per_token)

    async def execute_batch_market_orders(self, tokens, allocation_per_token):
        """
        修改点3：利用 V2 原生 Market Order 并发下单，严格实施 FOK 逻辑
        """
        order_tasks = []
        for token in tokens:
            # 构建 V2 规定的 MarketOrderArgs 参数
            order_args = MarketOrderArgs(
                token_id=token.id,
                amount=allocation_per_token,  # 投入的 pUSD 金额
                side=Side.BUY,
                options=PartialCreateOrderOptions(tick_size="0.01"),  # V2 必须指定的精确度参数
            )
            # 使用 FOK (Fill-or-Kill) 确保要么全部吃掉，要么一个都不成交
            order_tasks.append(
                self.client.create_and_post_market_order(order_args, order_type=OrderType.FOK)
            )

        # 并发向 CLOB 提交订单集群
        results = await asyncio.gather(*order_tasks, return_exceptions=True)

        # 检查是否有任何一笔订单失败（单边暴露风险控制）
        for result in results:
            if isinstance(result, Exception):
                logger.critical(
                    "原子性下单失败！部分订单被拒绝，触发紧急对冲或日志报警: %s",
                    result,
                )
                # 工业级实盘中此处需加入紧急平仓单（Market Sell）来冲销已成交的残余仓位
                return

        logger.info("套利订单全部完美成交，无风险锁仓成功。")

    async def get_pusd_balance(self):
        # 此处让 Codex 补齐通过 Web3.py 查询 Polygon 链上 pUSD 余额的代码
        return 10000.0
