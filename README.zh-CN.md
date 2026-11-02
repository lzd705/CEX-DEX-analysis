# CEX–DEX 市场结构与执行研究

[English](readme.md) | 简体中文

**[打开市场研究看板](http://43.156.102.166:8765)** · [实现导航](#实现导航) · [复现研究](#复现研究)

**基于来源证据的市场数据、协议级精确 AMM 报价，以及考虑执行成本的跨市场研究。**

## 项目概览

项目研究跨市场价差如何随着交易规模、流动性分布、手续费、Token 精度和市场状态时间而变化。
它把 CEX/DEX 对比平台、订单簿执行模型、固定区块池状态、整数交换模拟和可复现现金流实验连接起来。

| 模块 | 规模 | 观测依据 |
| --- | --- | --- |
| 市场数据平台 | 30 个 Token、12 家 CEX、493 条市场序列 | 2026-07-29 发布快照：345 条 CEX 序列，148 条 DEX 序列，对应 147 个物理池 |
| 历史每日观测 | 94,778 行 | 7 月快照：66,015 行 CEX 与 28,763 行 DEX 日度观测 |
| 流动性区间 | 10 / 25 / 50 / 100 bp | 双向订单簿深度和支持的池状态深度 |
| 交易规模网格 | 1千 / 5千 / 1万 / 5万 / 10万美元 | 使用相同目标 Token 数量的买卖报价 |
| V3 整数引擎 | 两个 Ethereum UNI 池 | UNI/USDT、UNI/WETH；exact-input、exact-output 与同区块 QuoterV2 校验 |
| SHIB V2 池间研究 | 10 个方向与规模场景 | 固定区块下的 Uniswap V2、ShibaSwap V1 对比 |
| 11 月 UNI CEX/V3 实验 | 51 个不同区块、510 个 DEX 报价 | 21,420 个费率/Gas 现金流场景，附公开输入与离线复现 |

[7 月发布报告](docs/senior-report.md)和[11 月实验](experiments/uni_cex_v3_20261102/README.md)
提供上述数字对应的日期、数据和计算背景。

## 系统架构

```text
CEX 现货 API               GeckoTerminal             EVM JSON-RPC
      └────────────────────── 采集器 ─────────────────────┘
                                  │
                       市场身份与单位标准化
                                  │
                     结构、覆盖率与时间校验
                                  │
          ┌───────────────────────┼─────────────────────────┐
       每日 OHLCV          TVL、深度与交易报价          研究采集批次
         SQLite               最新快照                固定状态输入
          └───────────────────────┼─────────────────────────┘
                          Python API 与看板

研究模块：CEX 逐档吃单、V2 整数储备模型、V3 整数 SwapMath
          Quoter 对照、同数量现金流、费用/Gas/延迟分析
```

**技术栈：**Python、SQLite、Decimal/整数运算、JSON-RPC、JavaScript、HTML/CSS、
Docker、Nginx、systemd，以及独立的 Foundry/Anvil 回放工具链。

## 数据质量

- **身份：**CEX 保留交易所与交易对；DEX 保留链、协议、池和 Token 地址。
- **单位：**保留基础/计价资产、Token decimals、原始整数数量和明确的美元换算方法。
- **完整性：**校验市场与场景清单、盘口边界；截断深度按已观测下界记录。
- **状态：**区分 `observed`、`partial`、`unsupported` 和 `failed`；缺失数值保留为 `null`。
- **时间：**保留来源提供的事件时间、请求与接收时间、采集跨度、区块时间和换算价格观测时间。
- **证据：**保存原始响应、采集清单、来源哈希和计算关联；发布门槛检查有效覆盖率及原有可用市场的保留比例。

日度 OHLCV、时点流动性和路由采集批次分别保留自己的存储与发布流程。
详见[数据管理](data/README.md)与[采集运维](docs/collection-operations.md)。

## 定量方法

### CEX：逐档订单簿执行

给定目标数量 `q`，买入按卖价从低到高消耗 asks，卖出按买价从高到低消耗 bids。
最后一档仅取完成目标所需的数量。总计价资产支出或收入除以成交数量，得到 VWAP。

```math
C_{buy}(q)=\sum_i p_i\Delta q_i,\qquad
R_{sell}(q)=\sum_j b_j\Delta q_j
```

参考价取同一份盘口的中间价。买入报价成本为“买入支出 − q × 参考价”，
卖出报价成本为“q × 参考价 − 卖出收入”；账户费率场景作为独立成本层加入。

### V2：带手续费的整数储备模型

设输入储备为 `x`、输出储备为 `y`、总输入为 `a`、手续费比例为 `f`，exact-input 输出为：

```math
amountOut=\left\lfloor\frac{y(1-f)a}{x+(1-f)a}\right\rfloor
```

Exact-output 反求储备方程，并采用协议的整数除法与 `+1` 规则。
计算使用 Token 原始单位、池实际费率和采集到的储备状态；池深度沿曲线计算至目标价格边界。

### V3：集中流动性与精确整数交换

[V3 模块](research/v3_exact/README.md)读取 `slot0`、活跃 `liquidity`、`fee`、
`tickSpacing`、`tickBitmap`、已初始化 tick 和 `liquidityNet`。
`sqrtPriceX96` 以 Q64.96 表示原始单位 token1/token0 价格的平方根；
转换成人类单位价格时，进一步应用 Token decimals 调整。

在一个活跃流动性固定的区间内，卖出 token0 使平方根价格从 `s0` 降到 `s1`。
令 `s = sqrtPriceX96 / 2^96`，区间关系为：

```math
amount0_{effective}=L(1/s_1-1/s_0),\qquad
amount1_{out}=L(s_0-s_1)
```

引擎使用整数定点运算和与方向相关的取整规则实现这些关系：

1. `compute_swap_step` 处理单个区间的输入、输出、手续费与下一价格。
2. `simulate_swap` 寻找下一个已初始化 tick 或价格限制。
3. 跨越 tick 时按 `liquidityNet` 更新活跃流动性；`zeroForOne` 方向反转其符号。
4. 持续计算，直到目标数量完成或达到限定价格边界。

卖出使用 exact-input，买入使用 exact-output，保持目标 Token 数量一致。
两个注册 UNI 池使用同一整数引擎计算深度边界和固定数量报价；
其他 V3 类型深度适配器保留各自的协议计算路径。

### 同区块 QuoterV2 校验

两池 canary 将状态绑定在同一个 finalized 区块，并校验池、factory、Token 顺序、
decimals 和费率。它以原始整数金额、`sqrtPriceX96After` 和符合 Quoter 语义的
`initializedTicksCrossed` 对照本地计算，receipt 记录共享区块与来源关联。

11 月实验采用绑定区块哈希的 latest 区块报价，并在采集结束时复查规范区块哈希。
各样本保留区块年龄与 CEX 观测年龄。

### 跨市场现金流

路由模型从相同的**净 Token 数量**出发。CEX 现金流按选定的手续费资产、数量
gross-up、步长和费用取整计算；DEX 交换报价包含池费，路由层补充其余适用成本。

```math
\Pi_{CEX\to DEX}=DEX_{sell}(q)-CEX_{buy}(q)-G
```

```math
\Pi_{DEX\to CEX}=CEX_{sell}(q)-DEX_{buy}(q)-G
```

`G` 为选定的 Gas 情景。结果同时记录费率、库存和时间假设。
详见[执行成本契约](docs/execution-cost-data-contract.md)。

## 已公开研究结果

### UNI：Binance ↔ Uniswap V3，2026-11-02

约十分钟采集包含 60 次轮询、51 个不同的有效区块和 510 个 QuoterV2 报价。
网格组合两个方向、五档规模、六档 CEX 费率和七档 Gas。

| 指标 | 结果 |
| --- | --- |
| 池 | Ethereum UNI/USDT，30 bp 池费 |
| 费率/Gas 现金流行数 | 21,420 |
| 采样网格中的正收益行数 | 0 |
| 零 CEX 手续费、零 Gas 诊断情景 | 两个方向、全部规模均为负 |
| 1,000 美元初始规模，CEX fee=10 bp、Gas=1 USDT 的最佳场景 | Binance 买 → V3 卖：−2.578299 USDT |
| 延迟实验 | 精确 +1/+2/+3 区块重报价，包含条件式 CEX-first 现金流 |

[采集与复现指南](experiments/uni_cex_v3_20261102/README.md) ·
[原始样本索引](experiments/uni_cex_v3_20261102/capture/manifest.json) ·
[结果](experiments/uni_cex_v3_20261102/results/summary.json) ·
[详细报告](docs/uni-cex-v3-experiment-2026-11-02.md)

离线复现校验原始响应、数量与状态绑定、费率证据和现金流，
再逐字节重建全部四份参考分析文件。

### SHIB：Uniswap V2 ↔ ShibaSwap V1

两个 RPC 供应商在同一 finalized 区块交叉核对 35 项逻辑读取，形成 70 条供应商观测。
研究以同一原始 SHIB 数量计算两个方向、五档规模。
留存的十个场景，其池费调整后价差均非正。
仓库包含[公共快照](research/shib_v2v2/data/public/research/shib-v2v2/latest.json)
和[计算实现](research/shib_v2v2/scripts/shib_v2_research.py)。

### Foundry：历史双市场执行工具

[UNI/WETH 执行器](foundry/src/TwoVenueV2Executor.sol)通过 Uniswap 和 SushiSwap
完成 WETH → UNI → WETH，校验 router 身份、调用者、库存和残留余额。
当前交换参数使用 `amountOutMin=0`，截止时间为 `block.timestamp + 60`。
[单元/fork 测试及工具链入口](foundry/README.md)介绍了回放流程。

## 实现导航

| 模块 | 默认分支源码 |
| --- | --- |
| 市场数据应用 | [dashboard/](dashboard/)、[采集脚本](scripts/run_collection_cycle.py) |
| V3 整数引擎 | [uniswap_v3_math.py](research/v3_exact/scripts/uniswap_v3_math.py) |
| V3 状态、深度与 Quoter 对照 | [fetch_dex_depth.py](research/v3_exact/scripts/fetch_dex_depth.py) |
| V3 两池注册表 | [uniswap_v3_execution_markets.json](research/v3_exact/config/uniswap_v3_execution_markets.json) |
| SHIB V2 研究 | [shib_v2_research.py](research/shib_v2v2/scripts/shib_v2_research.py) |
| Foundry 回放 | [TwoVenueV2Executor.sol](foundry/src/TwoVenueV2Executor.sol) |
| UNI 采集与校验 | [采集器](scripts/uni_cex_v3_experiment.py)、[分析器](scripts/analyze_uni_cex_v3_experiment.py)、[离线复现](scripts/replay_uni_cex_v3.py) |
| 来源版本 | [固定提交与文件哈希](research/sources.json) |

根目录应用和 Foundry 来源为 `83e66079`；独立 V3、SHIB 模块分别保留 `401eaad1`、
`b16ee24d`。[研究索引](docs/research-index.md)串联命令、测试、证据和来源版本。

## 复现研究

克隆默认分支，使用 Python 3.10+ 运行离线工作流：

```bash
git clone https://github.com/lzd705/CEX-DEX-analysis.git
cd CEX-DEX-analysis
python3 scripts/run_research.py list
python3 scripts/run_research.py v3-math
python3 scripts/run_research.py shib-replay
python3 scripts/run_research.py uni-v3-replay
```

V3 命令运行 23 项数学测试；SHIB 回放检查与留存快照的字节一致性；UNI 回放输出
观测数量、结果哈希和新建的本地输出路径。这三项使用仓库附带证据与 Python 标准库。

检查源码版本和双语导航：

```bash
python3 scripts/verify_research_sources.py
python3 scripts/check_research_docs.py
```

应用采集与看板运行见[看板操作文档](dashboard/README.md)；
Foundry 回放使用[固定工具链](config/historical_foundry_replay_toolchain.json)。

## 后续研究

1. 加入事件时间、接收时间模型，以及实测报价到提交的延迟分布。
2. 维护带序列校验的 WebSocket 盘口，扩展连续观测窗口。
3. 接入账户费率、费用结算精度、库存约束、部分成交和对冲恢复。
4. 使用 fork 执行和交易回执校准 Gas 与交易排序。
5. 按统一的身份、证据和复现契约扩展路由及协议覆盖。
