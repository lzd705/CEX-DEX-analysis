# CEX–DEX 跨市场研究平台

[English](readme.md) | 简体中文

**网站：**[打开市场研究看板](http://43.156.102.166:8765)

一个面向中心化交易所与链上流动性池的跨市场研究平台，用于比较价格、测量流动性、估算执行成本和验证套利机会。

项目将市场数据看板、基于交易数量的报价引擎和可复现的机会研究流程连接起来，围绕一个核心问题展开：**观察到的价格差异，在考虑交易规模、市场深度、费用和时间因素后，还剩下多少？**

[浏览应用代码](https://github.com/lzd705/CEX-DEX-analysis/tree/codex/historical-foundry-opportunity) · [数据契约](#文档导航) · [运行演示](#快速开始) · [研究工作线](#仓库导航)

## 平台功能

| 方向 | 功能 |
| --- | --- |
| 市场对比 | 选择一个 Token 和两个明确的市场，对比 CEX/CEX、CEX/DEX 或 DEX/DEX 的每日价格、收益率、波动率、成交额和价差。 |
| 流动性分析 | 基于 CEX 订单簿和支持的 DEX 池状态，计算 10、25、50、100 bp 的双向深度，并展示池子 TVL。 |
| 执行成本研究 | 在五档交易规模下逐档消耗盘口或模拟池内交换，记录数量、成交均价、费用范围和来源状态。 |
| 机会评估 | 用相同净 Token 数量匹配买卖两腿，附加成本证据、执行时间检查，并发布路由级研究结果。 |
| 历史回放 | 保留区块身份和采集证据，通过独立的回放与校验流程重建固定状态下的交易场景。 |
| 数据运维 | 支持增量采集、质量审计、快照发布、健康检查，以及经过身份验证的 Token 审核流程。 |

应用工作线配置了 **30 个 Token** 和 **12 个公共 CEX 订单簿适配器**。每份采集快照分别报告市场可用情况与有效覆盖率。

## 整体架构

```text
公共 CEX API                       DEX API + EVM JSON-RPC
      │                                      │
      └───────────────── 采集器 ──────────────┘
                           │
                   原始响应 + 采集清单
                           │
                 市场身份、结构与时间校验
                           │
          ┌────────────────┼──────────────────┐
          │                │                  │
       每日指标        深度与交易报价        路由评估
          │                │                  │
       SQLite           最新快照          不可变数据包
          └────────────────┼──────────────────┘
                           │
                   Python API + 看板
```

**技术栈：**Python、SQLite、Decimal/整数运算、JSON-RPC、JavaScript、HTML/CSS、Docker、Nginx 和 systemd；历史回放工作线还集成了 Foundry/Anvil。

每日时间序列、时点流动性和路由采集批次具有独立的存储与发布流程。看板通过明确的市场身份和来源关联将它们组合起来。

## 数据质量

质量检查贯穿采集、计算、发布和 API 交付各个阶段。

### 市场身份与单位

- CEX 观测保留交易所、现货交易对、基础资产和计价资产。
- DEX 观测保留链、协议、池地址、Token 地址、Token 顺序和 decimals。
- 计价资产转换为美元时，记录换算来源和方法；以 USDT 为基础的流程明确标注美元平价代理假设。

### 有效性与完整性

- 校验数据结构、盘口正值、买卖价格顺序、完整场景清单和来源哈希。
- 缺失数值保存为 `null`，区分实测零值、部分覆盖、未支持模型和采集失败。
- 检查订单簿截断情况，为各深度区间保留完整性标记；覆盖不足的实测深度作为已观测下界展示。
- 以明确的市场清单计算覆盖率；发布检查同时评估本轮有效覆盖率和此前可用市场的保留比例。

### 时间与可复现性

- 保留来源时间、观测时间、采集跨度，以及适用场景中的区块身份。
- 在固定区块读取一个 DEX 池所需的状态；精确执行和回放工作线进一步绑定区块哈希并检查规范链状态。
- 保留原始响应、采集清单、标准化数据和 SHA-256 来源关联，便于检查与回放。
- 每日图表保留缺口，日收益率仅在相邻且有效的日期之间计算。

详见[采集操作手册](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/historical-foundry-opportunity/docs/collection-operations.md)和[数据生命周期](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/historical-foundry-opportunity/data/README.md)。

## Depth 计算

深度衡量参考价格指定范围内可交易的计价资产金额，分别保留买入和卖出方向。

### CEX 订单簿

设最优买价为 `b`、最优卖价为 `a`，价格区间为 `k` 个基点：

```text
中间价 = (b + a) / 2
买盘深度 = Σ(价格 × 数量 × 计价资产美元价格)，价格 ≥ 中间价 × (1 − k / 10,000)
卖盘深度 = Σ(价格 × 数量 × 计价资产美元价格)，价格 ≤ 中间价 × (1 + k / 10,000)
```

采集器检查返回档位是否覆盖目标价格边界，从而区分完整区间深度和受 API 档位数量限制的深度。

### DEX 池状态

- **V2 类型池：**读取储备量、Token 元数据和实际池费率，使用恒定乘积模型计算价格移动到各边界所需的交易量。
- **V3 类型池：**读取 `slot0`、活跃流动性、费率、tick spacing、tick bitmap 和已初始化 tick；逐段累积交易量，并在跨越 tick 时更新活跃流动性。
- **V3 精确交换工作线：**独立整数引擎支持 UNI/USDT 和 UNI/WETH 的 exact-input、exact-output、协议式取整和限定范围的 tick 遍历，并提供 QuoterV2 对照校验工具。

TVL 描述池子的总资产价值，depth 描述沿特定价格方向可使用的流动性。在集中流动性池中，两者的关系取决于流动性在各 tick 间的分布。

## Execution Cost 计算

标准报价清单采用 **1,000、5,000、10,000、50,000 和 100,000 美元**五档规模，每档分别计算买入和卖出场景。

设参考金额为 `N`，参考价格为 `P`：

```text
目标数量 q = N / P，并按适用数量精度调整
参考金额 = q × P

买入执行成本 = 实际报价支出 − 参考金额
卖出执行成本 = 参考金额 − 实际报价收入
报价成本 bp = 执行成本 / 参考金额 × 10,000
```

CEX 报价按顺序消耗原始盘口档位；V2 执行使用整数交换运算；V3 精确执行工作线遍历已初始化 tick。各方法均直接针对目标交易数量计算。

### 成本分层

| 成本层 | 处理方式 |
| --- | --- |
| 买卖价差和价格冲击 | 体现在逐档盘口计算或池内交换报价中。 |
| 池内交换手续费 | 已包含在池内交换计算中。 |
| CEX 交易手续费 | 由机会成本层加入，并保留费率来源、具体费率和证据有效期。 |
| Gas、路由费、转账税和再平衡 | 作为独立路由成本项，记录数值、适用性与证据状态。 |
| 时间因素 | 记录报价年龄、来源时间跨度、路由截止时间和采集批次新鲜度。 |

常规市场报价页面计算 CEX 可见盘口的执行成本，以及支持的 V2 池中包含池费的交换成本。独立机会层补充具体路由的成本和假设；V3 精确执行由专门工作线提供。

## 如何验证套利机会

1. **选择明确市场。**确定 Token、买入市场、卖出市场、计价资产、方向和目标规模。
2. **采集同一研究批次。**保存两腿的盘口或池状态、交易规则、时间戳和原始证据。
3. **确定相同净 Token 数量。**结合数量步长和手续费扣除资产，使买卖两腿对应相同的库存变动。
4. **分别计算两腿报价。**根据采集到的流动性，计算总买入支出和卖出收入。
5. **附加成本证据。**记录各项费用，并标明是否已包含在报价中。
6. **评估路由。**计算场景净价差，检查时间、完整性和必需证据。
7. **发布与检查。**保存完整路由数据包，通过 API 展示结果，并保留场景不可用的原因。

```text
场景净收益 = 扣除卖出手续费后的收入
           − 包含买入手续费的支出
           − 额外适用的路由成本
```

每项成本只计入一次。当前公共 CEX 流程采用明确的双边预置库存场景，并返回 `research_estimate` 或 `unavailable` 分类及相应成本记录。

### 参考工作流

| 工作流 | 市场与场景清单 | 实现内容 |
| --- | --- | --- |
| 公共 CEX 机会研究 | Binance 与 Bybit；UNI/USDT、CAKE/USDT；四个有向路由 × 五档规模 | 实时采集、数量匹配、公共费率证据、完整数据包发布、API 和看板。 |
| V3 精确执行 | Ethereum UNI/USDT 与 UNI/WETH；两个池 × 两个方向 × 五档规模 | 共享整数交换引擎、固定状态采集、QuoterV2 校验和两池 canary 工具。 |
| SHIB 池间对比 | Ethereum Uniswap V2 与 ShibaSwap V1 的 SHIB/WETH；两个路由 × 五档规模 | 最终确认区块采集、两个供应商交叉核对 35 项逻辑读取，以及确定性的池费调整后价差计算。 |
| 历史机会研究 | 固定状态下的路由场景 | Foundry/RPC 回放、扫描、证据校验和历史结果展示工具。 |

### 已记录的 CEX 观测

[2026-09-05 公共采集记录](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/historical-foundry-opportunity/docs/live-cex-multi-token-acceptance-2026-09-05.md)覆盖了全部四个配置的 CEX 市场，并发布 20 行场景。在 1,000 美元规模下，UNI 的研究净收益为：Bybit → Binance **−3.5735828 美元**，Binance → Bybit **−4.35663 美元**。CAKE 场景记录了缺少精确交易对费率证据的原因。验收记录包含采集时间、来源哈希、费用处理和新鲜度检查。

## 快速开始

### 1. 获取应用与研究演示

默认 `main` 分支提供项目介绍和导航。以下分支包含看板、市场采集器、实时 CEX 工作流和历史演示：

```bash
git clone --branch codex/historical-foundry-opportunity \
  https://github.com/lzd705/CEX-DEX-analysis.git
cd CEX-DEX-analysis
```

演示程序使用 Python 标准库。应用和演示使用 Python 3.8 或更新版本；Foundry 回放工具链使用独立的固定运行环境配置。

### 2. 打开离线工作流演示

```bash
python3 scripts/run_historical_opportunity_demo.py --port 0
```

打开程序输出的 URL。该演示生成十个确定性合成场景，展示校验、API 交付、筛选和历史结果呈现流程。

运行封装的 Current Opportunity 工作流演示：

```bash
python3 scripts/run_current_opportunity_demo.py --port 0
```

该演示使用两个合成 AAA/WETH V2 池和五档交易规模。打开程序输出的完整 URL，进入 fixture 视图。两个程序均管理临时本地数据，按 `Ctrl-C` 结束。

### 3. 采集当前公共 CEX 报价

核对[公共费率配置](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/historical-foundry-opportunity/config/cex_public_fee_schedules.csv)中的官方来源、精确交易对、审核时间与到期时间，然后选择绝对路径作为输出目录：

```bash
python3 scripts/run_live_cex_opportunity.py \
  --data-dir /absolute/path/to/market-data \
  --public-fee-schedule config/cex_public_fee_schedules.csv \
  --serve --port 8765
```

程序采集公共盘口与交易规则、发布新数据包、验证冷加载，并输出看板 URL。每个批次的报价有效期为 120 秒；重新采集生成新的批次。

### 4. 使用采集数据运行市场看板

```bash
python3 scripts/run_collection_cycle.py --profile full --publish-local
npm --prefix dashboard install
./scripts/run_dashboard.sh
```

打开 `http://127.0.0.1:8765`。`daily` 和 `depth` 采集配置分别支持不同更新周期；已有审核 CSV 可通过 `scripts/import_local_snapshot.py` 导入。

## 仓库导航

| 分支 | 定位 | 入口 |
| --- | --- | --- |
| [`main`](https://github.com/lzd705/CEX-DEX-analysis/tree/main) | 项目介绍与导航 | 本 README |
| [`codex/historical-foundry-opportunity`](https://github.com/lzd705/CEX-DEX-analysis/tree/codex/historical-foundry-opportunity) | 应用、当前 CEX 研究、历史回放与演示 | 推荐快速启动分支 |
| [`codex/executable-opportunity-pipeline`](https://github.com/lzd705/CEX-DEX-analysis/tree/codex/executable-opportunity-pipeline) | 路由采集批次、数量匹配、成本项和发布流程 | [路由契约](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/executable-opportunity-pipeline/docs/route-cohort-data-contract.md) |
| [`codex/v3-exact-swap-mvp`](https://github.com/lzd705/CEX-DEX-analysis/tree/codex/v3-exact-swap-mvp) | V3 整数数学与两池精确执行 | [交换引擎](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/v3-exact-swap-mvp/scripts/uniswap_v3_math.py) |
| [`codex/v3-production-resilience`](https://github.com/lzd705/CEX-DEX-analysis/tree/codex/v3-production-resilience) | V3 采集与运行改进 | [采集运维](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/v3-production-resilience/docs/collection-operations.md) |
| [`codex/shib-v2v2-research-loop`](https://github.com/lzd705/CEX-DEX-analysis/tree/codex/shib-v2v2-research-loop) | 固定区块下的 SHIB 池间对比与回放 | [研究契约](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/shib-v2v2-research-loop/docs/shib-v2v2-research-contract.md) |
| [`website-v1`](https://github.com/lzd705/CEX-DEX-analysis/tree/website-v1) | 原始网站框架 | [看板代码](https://github.com/lzd705/CEX-DEX-analysis/tree/website-v1/dashboard) |

早期功能分支保留了市场指标、采集质量、流动性和页面导航的迭代历史，见[完整分支目录](https://github.com/lzd705/CEX-DEX-analysis/branches)。

快速启动分支的应用目录：

```text
config/                 Token 配置、路由注册表、费率证据与回放策略
dashboard/              Python API、SQLite 查询和浏览器界面
scripts/                采集、数学计算、质量检查、发布和演示程序
data/schema/            版本化数据库结构
data/curated/           审核后的事件和市场生命周期证据
docs/                   数据契约、设计说明、验收记录与操作手册
tests/                  计算、采集、发布、API 和前端测试
deploy/                 服务和反向代理模板
foundry/                Foundry 回放工具
```

采集响应和生成的数据库存储在 Git 忽略的运行目录中。[数据生命周期文档](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/historical-foundry-opportunity/data/README.md)介绍了目录结构和发布顺序。

## 文档导航

- [市场指标与对比](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/historical-foundry-opportunity/docs/market-facts-contract.md)
- [CEX 深度](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/historical-foundry-opportunity/docs/cex-depth-data-contract.md)
- [DEX 深度](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/historical-foundry-opportunity/docs/dex-depth-data-contract.md)
- [执行成本与路由成本项](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/historical-foundry-opportunity/docs/execution-cost-data-contract.md)
- [路由采集批次与机会发布](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/historical-foundry-opportunity/docs/route-cohort-data-contract.md)
- [采集周期与质量门槛](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/historical-foundry-opportunity/docs/collection-operations.md)
- [事件指标](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/historical-foundry-opportunity/docs/event-facts-contract.md)
- [管理与 Token 审核](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/historical-foundry-opportunity/docs/admin-operations.md)
- [部署运维](https://github.com/lzd705/CEX-DEX-analysis/blob/codex/historical-foundry-opportunity/docs/production-hardening.md)

## 验证

在应用分支目录下运行：

```bash
python3 -m unittest discover -s tests -v
git diff --check
```

测试涵盖盘口标准化、池计算、数量规则、缺失值处理、来源关联、发布行为、API 契约和前端状态。验收记录保存带日期的真实来源观测及对应代码与证据。

## 后续方向

- 维护带序列号校验的 WebSocket 订单簿，开展更细粒度的机会观测。
- 接入账户认证费率和交易所特定的费用结算精度。
- 将交易级 Gas、区块包含延迟和 MEV 场景纳入路由研究。
- 加入库存约束、再平衡成本和执行反馈测量。
- 扩展协议适配器，以及连续、多时间窗口的机会实验。
