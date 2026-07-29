# CEX / DEX Market Monitor 学长汇报稿

> 使用范围：面向懂 Web3、关心市场微观结构和数据工程的学长。
>
> 产品边界：这是一个 source-backed、fact-only 的市场事实看板，不是交易信号、因子模型或事件研究系统。

Event API/UI 已进入部署应用 commit；运行时 Event data 由独立的不可变 bundle
标识。代码和事件数据可以分别回滚，不能用一个模糊的“版本号”替代 application
SHA 与 Event bundle ID。

## 已验证发布证据

以下证据来自 2026-07-29 的最终服务器和真实浏览器检查，不是开发环境推断。

### Release identity

- Remote：`https://github.com/lzd705/CEX-DEX-analysis.git`
- Branch：`codex/event-source-coverage`
- Deployed application SHA：
  `3524528b5160f441d0312c66504706377230ff1f`
- Deployed application commit：
  `feat(events): cover all configured tokens with official facts`
- Server checkout SHA：
  `3524528b5160f441d0312c66504706377230ff1f`
- Report evidence commit message：
  `docs(report): record 30-token Event coverage deployment`
- Report evidence SHA：不在文件内部自指；以本分支的 `git log -1` 和最终交接记录为准。
- PR：none；当前交付是远端新分支，不是已合并主分支。

本次应用 release 的 commit 与 push comment：

| Commit | Message | Push comment |
| --- | --- | --- |
| `bccdbd1af00c86d6ce4fe537d4b37d0fe265fa15` | `feat(dashboard): add Compare charts and source-backed Event Facts` | Publish the verified Compare charts, five-page Events workflow, Event bundle/API gates, and CEX fee fail-closed rules for production validation. |
| `5317ecb5fda3c8487bea58e5a5f0380fd6227cda` | `test(runtime): skip JavaScript checks when Node is unavailable` | Make deployment-host test results truthful: run all Python 3.8 checks and explicitly skip only Node-dependent frontend tests; browser smoke remains mandatory. |
| `c102a78ce50370707c6996aaa164390a4ac39a6c` | `test(events): honor missing Node in route regression` | Close the last deployment-host Node availability gap so Python 3.8 release tests finish with explicit frontend skips, followed by mandatory live-browser QA. |
| `de175084c4c763182730a2222025f2e498f18759` | `fix(mobile): keep primary navigation and workspace count readable` | Publish the 320px navigation/readability fix found during live browser QA and correct the five-page workspace copy. |
| `38cedaa034fa7bfbed02ef962e8d4d8332ab98d8` | `test(mobile): scope responsive header assertions` | Publish selector-safe mobile navigation behavior and rule-scoped regression checks before production deployment. |
| `3524528b5160f441d0312c66504706377230ff1f` | `feat(events): cover all configured tokens with official facts` | Publish verified 30-token Event coverage, lifecycle safeguards, and catalog-wide release checks. |

### Deployment environment

- Public demo URL：`http://43.156.102.166:8765`
- 部署完成：2026-07-29 10:10:02 UTC / 18:10:02 HKT
- 最终服务器证据检查：2026-07-29 10:20:51 UTC / 18:20:51 HKT
- Runtime：Python 3.8.10
- Supervisor：systemd user service `cex-dex-dashboard.service`
- ExecStart：
  `/usr/bin/python3 dashboard/server.py --host 10.3.0.6 --port 8765`
- 环境定位：demo/staging；当前是裸 HTTP/IP，没有 reverse proxy 或 HTTPS。
- Public admin：`/admin.html -> 404`；`/api/admin/status -> 404`。

### Tests and live checks

- Local complete suite，Python 3.13.5：
  `python3 -m unittest discover -s tests -p 'test_*.py'`
  → 运行 340 项，0 skipped，0 failed，0 errors。
- Server suite，Python 3.8.10：同一命令运行 340 项；
  29 个 Node-dependent frontend tests 明确 skipped，
  0 failed，0 errors。服务器没有 Node，因此真实浏览器 smoke 是必须的补充，
  不能把 skipped 写成“全部通过”。
- `/health`：`status=ok`、`data_ready=true`、`data_status=current`。
- Release checker：passed；30 Token、493 catalog markets、
  `data_generation=f0408687aa056557e3711f9383755c82c85f92799081c35d514ef6a6a2c26345`、
  44 latest Event Facts、30/30 Token presence coverage、Event bundle
  `236accecf312dc79ca6f8630`；release checker 逐个请求 30 个 Token，均返回至少一条记录。
- Desktop browser：Screener、Markets、Compare、Liquidity & Execution、
  Events、Data Quality 六条公开路由均可加载；没有可见 error state，
  捕获到的 page/console/unhandled-rejection errors 为 0，页面横向溢出为 0。
- Interaction smoke：warning 原因悬停、Price/Spread/Volume 切换、
  Event tooltip、SPA workspace 跳转和 lifecycle URL 持久化均通过；本次 release
  另行检查 LINK、MORPHO revision 2、OP、JTO、UNI、STRK 的 Events 页面和 Token 切换。
- 320 px browser：document/body 横向溢出为 0；三个顶部导航完整位于 viewport；
  freshness 完整换行；五个 workspace tabs 可用；Event 五张覆盖卡单列显示，
  `TOKEN COVERAGE 30 / 30` 可读。STRK 默认 30 日 Compare 窗口实测 1 个 Event
  overlay；overlay 数量取决于 Token 和窗口，不能写成全局固定值。
- 官方页面 browser spot-check：UNI/OKX、LINK/Binance、OP/Optimism、
  RAY/OKX 四页均公开加载，页面标题与正文包含对应事实。
- 本次 Event coverage release 未关闭 P0：none。
- 本次 Event coverage release 未关闭 P1：none。

### 当前运行数据快照

| Fact family | 运行计数 / 状态 | 时间范围或快照时间 |
| --- | --- | --- |
| Catalog | 30 Token；493 market series = 345 CEX + 148 DEX；12 CEX exchanges；147 physical DEX pools | release check 2026-07-29 |
| CEX daily | 66,015 rows；30 Token；12 exchanges | 2026-01-16 至 2026-07-28 |
| DEX daily | 28,763 rows；30 Token；147 physical pools | 2025-05-14 至 2026-07-28 |
| TVL latest | 148 rows：148 observed；147 physical pools | 2026-07-29 01:06:20–01:09:09 UTC |
| CEX depth latest | 344 rows：339 observed、3 partial、2 failed | 2026-07-29 10:05:43–10:07:36 UTC |
| DEX depth latest | 148 rows：84 observed、64 unsupported；147 physical pools | 2026-07-29 10:09:24–10:10:54 UTC |
| CEX execution latest | 3,440 rows：2,887 observed、533 partial、20 failed | 2026-07-29 10:05:43–10:07:36 UTC |
| DEX execution latest | 1,480 rows：159 observed、31 partial、1,290 unsupported | 2026-07-29 10:09:24–10:10:54 UTC |
| Event latest | 44 facts / 45 revisions；30/30 Token presence；15 occurred、29 scheduled；44 primary_confirmed | bundle `236accecf312dc79ca6f8630` |

148 条 DEX market series 对应 147 个 physical pools，因为 catalog/API 使用
Token-perspective market identity；同一个物理 pool 可能承载两个目标 Token 视角。
345 个 CEX catalog markets 与 344 条当前 depth snapshots 也不是同一个概念；
前者是目录，后者是这次 snapshot publication 的结果。

3,440 条 CEX execution rows 全部是
`fee_status=excluded_unknown_account_tier`，numeric CEX fee fields 保持 null。
DEX execution 中 190 条 supported rows 包含 pool swap fee mechanics；1,290 条
unsupported rows 不发布 fee 数值。CSV 的历史 machine enum 名称仍是
`included_protocol_fee`，但网站展示和实际语义均是 pool swap fee，详见“明确边界”。

## 一句话介绍

这个项目把同一个 Token 在 CEX 现货交易对和 DEX pool 上的价格、成交量、回报、波动率、TVL、深度、固定金额执行成本、来源时间和数据质量放进一个可审计的工作流；新加入的 Events 页面展示来源可追溯的事件事实，Compare 只叠加时间标记，不把事件与价格变化解释成因果关系。

## 3–5 分钟口头稿

下面可以直接作为口头汇报使用。

### 0:00–0:35：解决什么问题

我做的是一个 CEX / DEX 市场结构事实看板。它不是告诉用户应该买什么，而是回答三个更基础的问题：同一个 Token 在哪些市场交易；这些市场的价格、成交量和流动性有什么事实差异；这些数字来自哪里、是什么时间采集、是否完整以及能不能比较。传统聚合页面经常把 Token 级汇总、某一个交易对、DEX TVL 和可执行深度混在一起，这个项目把这些口径拆开。

### 0:35–1:20：用户怎么操作

用户先在 Screener 里按时间窗口、综合成交量、CEX 成交量或 DEX 成交量筛 Token。进入一个 Token 后，工作台分五个视角。Markets 是完整的 CEX pair 和 DEX pool 目录，并用于设置 Market A 和 Market B；Compare 比较同一 Token 的两个市场在相同 UTC 日期上的价格、成交量、回报、波动率和价差；Liquidity & Execution 比较 10、25、50、100 bps 深度以及 1 千到 10 万美元固定名义金额的 quoted execution cost；Events 展示官方来源、时间精度、lifecycle 和 revision 均可追溯的事件事实；Data Quality 展示来源、覆盖率、快照时间、缺失、unsupported 和 warning 原因。A/B、Token、daily window 和 Events lifecycle 都进入 URL，所以页面可以刷新、返回和分享。

### 1:20–2:10：最重要的计算口径

Selected-window return 是窗口第一条有效 close 到最后一条有效 close，不是固定 24 小时收益。Daily volatility 只使用相邻 UTC 日的 log return；中间缺一天，就不会把跨两天变化伪装成一天波动。CEX depth 来自真实公开订单簿，在中间价上下 10、25、50、100 bps 内累计可见 quote notional；DEX depth 不是用 TVL 估算，而是在一个固定 block 上读取 pool state，按 V2 constant-product 或 V3 tick liquidity 计算边际价格走到同样 bps 所需的 quote notional。

Execution cost 也不是从四个深度点插值。CEX 会逐档走原始 order book；支持的 DEX V2 会用整数 constant-product swap mechanics。它表示相对于交易前参考价的 quoted shortfall，不是已经实现的成交结果，也不是 all-in 成本。CEX 用户费率、gas、router fee、MEV、延迟和隐藏流动性都明确排除；支持的 DEX pool swap fee 才会进入相应计算。

### 2:10–3:00：数据与系统怎样建成

CEX daily collector 最多查询 12 个公开 spot adapter，实际发布了多少 exchange/market 必须以运行 API 为准；DEX OHLCV 和 source-reported TVL 来自 GeckoTerminal；CEX depth 来自交易所公开 order book；DEX depth 来自链上 JSON-RPC fixed-block state。TVL、CEX depth 和 DEX depth 保留 raw response 或 RPC transcript、来源时间与 SHA-256；daily CEX/DEX OHLCV 当前保留规范化来源字段和时间，但不声称逐请求 raw payload 已归档。Daily facts 进入 SQLite，TVL、depth 和 execution 保持独立 latest snapshot。后端是 Python 的只读 API，前端是原生 HTML、CSS 和 JavaScript。当前演示环境由 systemd user service 运行并直接暴露 HTTP 端口；它适合验收和演示，但没有 HTTPS reverse proxy，不能称为 hardened production。

### 3:00–3:45：Event Fact 新能力

Event Fact 的目的不是解释价格，而是给市场时间序列补一个可核查的事实坐标。目前 taxonomy 只接受 unlock、airdrop claim start 和 CEX spot trading start。每条事件保存 effective time、时间精度、lifecycle、官方来源、source check time、evidence status 和 revision lineage。Compare 图上只放事件标记，tooltip 说明是什么事件、什么时间口径和哪一个来源；不会自动计算事件前后收益，也不会显示“导致上涨”或“导致下跌”。

相对于当前 `latest.json` 指向的 prior bundle，事件修订不能覆盖旧记录。延期、取消、时间或规模修正都必须增加同一个 `event_id` 的下一版 revision，之前已经发布的 revision 留在 bundle 中；网站/API 默认只读每个 `event_id` 的 latest revision，完整历史用于审计。它不是外部永久存证，因此删除整个 publication root 后，程序本身无法证明已经丢失的历史。

### 3:45–4:35：为什么这些数字可以审计

系统对真实 `0`、`null/missing`、展示层 `N/A`、`partial`、`unsupported`、
`failed` 和 publication 级 `unavailable` 分开处理，缺失值不会补零。Event feed
`available + 0 rows` 只表示当前筛选没有匹配项，也不等于 unavailable。Daily、
TVL、CEX depth、DEX depth 和 execution 有独立 freshness，不用一个“最后更新”
掩盖落后的来源。CEX 截断 order book 会把深度标为 lower bound；没有协议特定、
项目内实现并通过测试验证的 DEX adapter 时保持 null，不会拿 TVL 或通用公式猜一个
结果。每次发布有 manifest、文件 hash、覆盖率和 freshness gate，运行数据库和
latest snapshot 都采用验证后替换。

### 4:35–结束：结论

因此，这个看板目前的价值是建立一层可解释、可复现、可继续扩展的 market fact infrastructure。它已经适合做跨 venue 的事实核查、流动性比较和数据质量审计；但我不会把它包装成实盘执行器、回测结论或事件因果模型。下一步会先继续关闭时间口径和数据覆盖问题，再按独立 contract 增加 funding rate、明确 CEX fee scope 或正式 event-study，而不是把不同概念直接塞进现有指标。

## 10 分钟演示路线

演示前准备：

- 选择一个同时有 CEX、DEX、有效深度和 Compare 日线的 Token。
- Event Fact 演示可先用 STRK：它在默认 daily window 有时间标记，并同时包含
  occurred 与 scheduled；再切到 LINK 或 MORPHO，演示单记录、lifecycle 和 revision。
- 预先确认 A/B 都属于该 Token，并确认 event marker、warning tooltip 和 Data Quality 链接可用。
- 不使用 stale、failed 或 unsupported 结果作为“最佳市场”的结论。

### 0:00–1:15：Screener

1. 展示全市场入口和 UTC daily window。
2. 选择排名 Fact、Aggregate / CEX / DEX scope 及升降序，说明缺失值固定排在最后，
   每行同时展示真正参与排序的数值。
3. 搜索 Token，指出 Token 行是汇总，primary CEX/DEX 是明确市场，而不是把全部市场平均成一条虚构记录。
4. 指出 Warning/Critical 只是 fact quality，不是 Token 风险评级。

### 1:15–2:30：Markets

1. 打开 Token workspace。
2. 展示 CEX exchange/pair 与 DEX chain/protocol/pool identity。
3. 选择 Market A 和 Market B。
4. 悬停或聚焦 warning 图标，展示具体原因、observed value 和 threshold。
5. 强调 A/B 可以是 CEX/CEX、CEX/DEX 或 DEX/DEX，但必须是同一个 Token 的两个不同 market ID。

### 2:30–4:10：Compare 与 Event 时间标记

1. 展示 A/B 的相同 UTC 日 price、volume、return、volatility 和 spread。
2. 切换 Price、Spread、Volume 图；指出折线只连接连续有效观察，缺失或非连续日期会断线，不 forward-fill。
3. 指向 Event Fact marker：
   - 打开 tooltip；
   - 展示 event type、effective time/precision、lifecycle、source 和 revision；
   - 说明 marker 只是上下文，不表示该事件造成旁边的价格变化。
4. 下方原始 daily 表继续保留，图不是唯一证据。

### 4:10–5:45：Liquidity & Execution

1. 展示 10/25/50/100 bps depth。
2. 切换 Total/Directional 和 Linear/Log。
3. 说明 CEX 是 order-book band，DEX 是 fixed-block pool-state mechanics，TVL 不参与 depth 换算。
4. 切换 Buy/Sell 和 `$1k/$5k/$10k/$50k/$100k`。
5. 展示 quoted cost、fill ratio、status、fee scope 和 snapshot skew。
6. 若某个 market unsupported，主动解释它为什么是 `N/A`，而不是绕开。

### 5:45–6:45：Events

1. 打开当前 Token 的 Events 页，展示 feed availability、已发布 bundle 和记录数。
2. 切换 Occurred / Scheduled lifecycle，说明筛选条件会写入 URL。
3. 展开一条记录的 effective time/precision、event type/subtype、size relation、
   evidence status、官方来源、source checked time 和 revision。
4. 强调“当前 release 没有匹配记录”和“Event feed 未发布”是两种不同状态；
   二者都不能被解释为该 Token 没有事件。

### 6:45–7:40：Data Quality

1. 切换 All markets 与 Selected A/B。
2. 展示 daily coverage、source timestamp、TVL/depth/execution lineage。
3. 对比 `observed`、`partial`、`unsupported`、`failed`、`N/A` 和真实 `0`。
4. 说明 selected-window quality 与 full-history quality 必须分开解释。

### 7:40–8:35：受保护的数据运维

1. 在仅管理员可访问的页面展示“近期缺口重拉”“历史缺口重拉”和“必须人工复核”
   三类队列；公开看板仍然只读。
2. 对可重拉项，只允许提交当前质量报告列出的 Token、market、日期窗口，不能手填
   任意目标绕过审计。
3. 展示采集原因：网络/限流/来源无数据/来源不支持/解析或校验失败，与结构性
   `unsupported`、测量限制和市场状态分开。
4. 输入 chain 和智能合约地址预览 Token 身份，再确认 DEX-first onboarding；
   不从链上地址猜 CEX pair。

### 8:35–9:25：架构与更新

用下面的系统图说明 raw evidence、processed facts、SQLite/latest CSV、API、前端和 deployment 的关系。强调代码发布和数据发布可以分别回滚。

### 9:25–10:00：证据和收尾

展示已经核验的：

- deployed application SHA 与 server checkout SHA；
- local complete suite 与 server Python 3.8 suite；
- `/health`；
- release checker；
- 当前 data generation 与 Event bundle ID；
- 一次完整浏览器 smoke。

最后用一句话收尾：

> 我现在交付的是一套不会把缺失值、不同时间尺度和不同流动性定义混在一起的事实基础设施；预测和因果分析必须建立在这层事实通过之后。

## 看板使用说明

### Screener：发现 Token

- Daily window：控制 price、volume、return、volatility 和 daily price gap。
- Aggregate：按所有 cataloged CEX/DEX markets 的窗口成交量排序。
- CEX / DEX：只改变 Token 级排序口径，不改变已经选中的具体 A/B。
- Search：按 Token symbol 过滤。
- Open workspace：进入该 Token 的完整 market catalog。
- CSV：导出当前可见的 Screener 事实，不等于导出原始数据仓库。

### Markets：确认身份与选择 A/B

- CEX 行必须有 exchange 和 pair。
- DEX 行必须有 chain、protocol、pool address 和 pool name。
- `Set as A/B` 明确改变共享比较上下文。
- Market quality tooltip 给出原因和阈值。
- 当前窗口 coverage 与全历史 catalog quality 是不同 scope，汇报时不能混说成一个覆盖率。

### Compare：比较 daily facts

- A/B 必须属于当前 Token，并且不能是同一 market ID。
- 只对同一 UTC 日期都有有效价格的记录计算 spread。
- 图表提供 Price、Spread、Volume 三种事实视图。
- 缺失日期让线段断开，不补值、不插值。
- Event Fact 只画在它自己的有效时间或时间区间上，并保留原始时间精度。
- 图上 marker 不改变价格数据，也不触发事件影响结论。

### Liquidity & Execution：比较容量与 quoted cost

- Depth：
  - Total：买卖两侧相加；
  - Directional：按买入/卖出 Token 的方向统一语义；
  - 10/25/50/100 bps 是四个实际计算点，不画成连续可插值曲线。
- Execution：
  - 方向是 `buy_token` 或 `sell_token`；
  - notional 是 `$1k/$5k/$10k/$50k/$100k`；
  - 完整成交才显示完整 VWAP 和 quoted cost；
  - partial 可以显示 fill ratio，但不发布完整请求成本；
  - unsupported/failed 的数值保持 null。

### Events：查看来源可追溯的事件事实

- 页面展示每个 `event_id` 在当前 release 中的 latest revision；完整 revision
  history 保存在 bundle，不在网页中展开。
- 页面按当前 Token 和 lifecycle 过滤，不受 daily market window 影响。
- Compare overlay 才会使用 daily window 请求同一 Token 的事件。
- `available + 0 rows` 只表示当前 release 没有匹配记录；`unavailable` 表示
  Event publication 本身不可用，二者都不是“该 Token 没有事件”的证明。

### Data Quality：判断事实能不能用

- 查看每一 fact family 的来源和时间。
- 查看 daily coverage 和 skipped gap。
- 查看 point-in-time snapshot status。
- 查看 raw hash、fixed block、endpoint、fee scope 和 excluded cost。
- Warning 是可审计 heuristic；它不等于 exchange insolvency、smart-contract safety 或 Token fundamental risk。

指标定义、来源限制和 non-claims 已合并进相关工作页与 Data Quality；独立
Methodology 页面已删除。旧 `/methodology` URL 只做兼容跳转，不再形成第七套页面。

### Admin：处理缺口和加入 Token

- Admin 默认关闭；只有在 HTTPS、访问控制和管理员凭据都已配置时才启用。
- “Retryable missing facts” 只显示当前已发布质量报告批准的精确窗口。近期 D-1
  缺口与历史内部缺口分别标识，刷新完成后还要验证新的 publication identity 和
  精确 market/date 行，不能只凭采集进程退出码报成功。
- “Manual review required” 展示硬异常和 market lifecycle 不明等不可自动处理项，
  并给出 primary-source URL hints；它们没有重拉按钮。
- “Add Token” 先输入 chain 与 smart-contract address，解析并确认 Token 身份和
  pool，再启动 DEX daily、TVL 和支持协议的 DEX depth 采集。CEX mapping 保持
  `requires_manual_review`，不会自动猜测。

## 系统架构

```text
公开 CEX spot API       GeckoTerminal API       EVM JSON-RPC
       │                       │                     │
       └───────────── market collectors ────────────┘
                              │
       normalized daily rows / point-in-time raw evidence + hash
                              │
          Daily OHLCV SQLite + TVL/depth/execution latest CSV
                              │
                              ├──────────────┐
官方 Event 页面 → evidence JSON + curated revision CSV          │
                              │                                 │
                  validated immutable Event bundle/SQLite       │
                              └──────────────┬──────────────────┘
                                             │
                                  Python read-only API
                                             │
                    summary / catalog / compare / quality /
                              execution / events
                                             │
                              Vanilla HTML/CSS/JavaScript
                                             │
                         systemd user service → HTTP demo endpoint
```

### 前端

- 原生 HTML/CSS/JavaScript SPA。
- Lucide 图标 vendored，不依赖公共 CDN。
- Token workspace 路由保留 Token、Market A/B 和 daily window。
- Screener 只加载轻量 summary；进入 Token 后才加载该 Token catalog。
- Compare 图、depth 图和 tooltip 保留键盘可访问语义。

### 后端

- Python 标准库 `ThreadingHTTPServer`。
- Daily market facts 优先从 SQLite 只读查询。
- TVL、CEX depth、DEX depth、CEX/DEX execution 是独立 latest overlay。
- Market payload 使用 source signature 和 `data_generation` 防止浏览器混用不同发布代际；Event payload 使用 `bundle_id`，Event 文件同时进入服务端 source signature 和 cache invalidation。
- 大 payload 和 gzip response 采用有界进程内 cache。

### Event Fact 子系统

- `data/curated/event_facts.csv`：人工复核、版本化的 event revisions。
- `data/evidence/events/*.json`：项目实际检查过的来源记录及限制。
- `scripts/event_facts.py`：校验、规范化并构建 immutable bundle。
- `event_fact_revisions.csv`：全部 revision。
- `event_facts_latest.csv`：每个稳定 `event_id` 的最高 revision。
- `event_facts.sqlite3`：索引存储和 latest view。
- `manifest.json` 与 `latest.json`：hash inventory 和原子发布指针。
- `dashboard/event_facts.py`：校验 pointer、manifest、文件 hash 后，输出 null-preserving、可按 Token/window/lifecycle 过滤的 API payload。

### 部署

- 当前代码 release 由 Git commit 固定，runtime Event data 由不可变 bundle
  和 `latest.json` 指针发布，两者可以分别回滚。
- 当前演示服务器由 systemd user service 管理 Python 进程，并直接暴露
  `http://IP:8765`。
- Public process 保持 `ADMIN_ENABLED=false`，外部 `/admin.html` 与
  `/api/admin/*` 必须返回 404。
- 由于当前入口没有 Nginx、HTTPS、HSTS、限流和边缘访问日志，它只能称为
  demo/staging；这些是 hardened production 的下一步，不是已经完成的能力。

## Fact 来源和公式

| Fact | 来源 | 计算/口径 | 关键限制 |
| --- | --- | --- | --- |
| CEX daily OHLCV | 最多 12 个公开 spot adapter；运行覆盖以 API 为准 | 各 venue 原生字段规范为 USD daily rows | venue 字段定义和 quote conversion 不完全相同 |
| DEX daily OHLCV | GeckoTerminal API v2 | 指定目标 Token side，`currency=usd` | 当前领先池选择会带来 survivorship bias |
| Price | daily source close | 窗口最后一条有效 close | 不是实时 executable quote |
| Volume | daily USD volume | 窗口内同一 market 求和 | Token 汇总和具体 market 必须区分 |
| Window return | daily close | `last / first - 1` | 第一/最后 observation 不保证恰好在窗口边界 |
| Daily volatility | daily close | 相邻 UTC 日 log return 的 sample standard deviation | 跨 missing day 的 interval 被排除 |
| Absolute spread | 同日 A/B close | `abs(A - B)` | 只有同一 UTC 日都有效才计算 |
| Spread bps | 同日 A/B close | `abs(A-B) / ((A+B)/2) × 10,000` | 不是 bid/ask spread 或 slippage |
| TVL | GeckoTerminal `reserve_in_usd` | 来源报告的 point-in-time pool reserve | 不是 V3 active liquidity，也不是 depth |
| CEX depth | public order book | 中间价上下 10/25/50/100 bps 内累计 quote notional | REST level 截断时只是 lower bound |
| DEX V2 depth | fixed-block reserves | constant-product、Token decimals 和 pool swap fee | 不包含 block 后状态变化 |
| DEX V3 depth | fixed-block tick state | 逐 initialized tick 积分 active liquidity | 仅项目内实现并通过验证的协议特定 adapter |
| Execution cost | order book 或 fixed-block pool state，加独立 USD conversion observation | 固定 Token quantity相对 pre-trade reference price 的 quoted shortfall | DEX USD-price/state skew 超过 2 小时则 fail closed；不是 realized 或 all-in cost |
| Event Fact | 官方 project/governance/exchange 或直接 onchain evidence | 保存 effective time/precision、lifecycle、source、revision | 只提供事实标记，不计算影响或因果 |

### CEX depth

```text
midpoint = (best_bid + best_ask) / 2

bid_boundary(k) = midpoint × (1 - k / 10,000)
ask_boundary(k) = midpoint × (1 + k / 10,000)

bid_depth_usd = Σ(price × base_quantity × quote_to_usd)
ask_depth_usd = Σ(price × base_quantity × quote_to_usd)
```

`k ∈ {10, 25, 50, 100}`。返回档位没有覆盖 band boundary 时，结果只能标成 observed lower bound。

### DEX V2 depth

设 reserves 为 `x, y`，`k = x × y`，fee fraction 为 `f`，下行边际价格因子为 `m`：

```text
net_input_0   = x × (1 / sqrt(m) - 1)
gross_input_0 = net_input_0 / (1 - f)
output_1      = y - k / (x + net_input_0)
```

反方向使用相应的上行价格因子。实际 execution 使用 Token base-unit 的整数 arithmetic，而不是只用连续小数近似。

### Execution cost

```text
target_token_quantity = requested_notional_usd / reference_price_usd
```

Sell：

```text
cost_usd = reference_notional_usd - quote_received_usd
cost_bps = cost_usd / reference_notional_usd × 10,000
```

Buy：

```text
cost_usd = quote_paid_usd - reference_notional_usd
cost_bps = cost_usd / reference_notional_usd × 10,000
```

CEX reference price 是同一订单簿的 midpoint。DEX 的 pre-trade、pre-fee
marginal pool ratio 来自同一 fixed block，但 USD notional 还要乘以独立
GeckoTerminal quote-token USD observation；页面显示 price/state skew，超过
2 小时 hard gate 时 execution cost 不发布。它因此不是纯粹的 same-block USD fact。

## Event Fact 新能力

### 第一版 taxonomy

| 类型 | subtype | effective time 的含义 |
| --- | --- | --- |
| `unlock` | `scheduled_release` | 官方 lock-up schedule 中的释放日期 |
| `airdrop` | `claim_start` | 用户第一次可以 claim 的时间 |
| `cex_listing` | `spot_trading_start` | 现货交易开放时间，不是充值、call auction 或提币时间 |

### 当前 curated release

当前运行 bundle `236accecf312dc79ca6f8630` 包含 44 条 latest facts、
45 条 immutable revisions、29 个不同的官方 source URL，并覆盖 catalog 配置的
30/30 Token：

- 15 条 STRK monthly scheduled-release dates；
- 其余 29 个 Token 各至少 1 条官方来源事实；
- taxonomy 分布为 27 条 `cex_listing`、15 条 `unlock`、2 条 `airdrop`；
- lifecycle 分布为 15 条 `occurred`、29 条 `scheduled`；
- 44 条 latest facts 全部为 `primary_confirmed`。

这里的 30/30 只表示“每个配置 Token 至少有一条已验证时间轴事实”，不表示已完整
收录每个 Token 的所有历史 unlock、airdrop 和 listing。27 条 listing 中，只有
6 条官方页面明确说明交易已经开始或当前可用，因而标为 `occurred`；其余 21 条是
开盘前公告，保持 `scheduled`。即使公告时间已经过去，也不会只根据时钟自动改成
`occurred`。

STRK 单 Token 页面仍是 7 条 occurred + 8 条 scheduled。其中 7 条 STRK
`occurred` 由官方页面“have been and will be unlocked”的措辞支持，
不是因为日期已经过去；这些仍是 schedule facts，并不等同于 onchain transfer
confirmation。MORPHO 的 revision 1
保留在 bundle 中；revision 2 将此前过度推断的 `occurred` 修正为
`scheduled`，并写明 revision reason。未知金额、USD value 或 supply percentage
保持空值，不从二级日历、搜索摘要或市场首个观测反推。

官方来源由交易所和项目方页面组成，包括 Binance、OKX、Optimism、Eigen
Foundation 和 Starknet。新增来源记录主要在 2026-07-29 09:37:30 UTC 检查，
MORPHO 修订证据在 09:54:30 UTC 检查；既有 STRK/EIGEN 记录保留
07:33:00 UTC 的检查时间。浏览器另外现场打开并核对：

- UNI：[OKX listing page](https://www.okx.com/en-us/help/uniswap-uni-now-available)
- LINK：[Binance listing page](https://www.binance.com/en/support/announcement/detail/360021982271)
- OP：[Optimism claim page](https://optimism.io/blog/let-the-claims-begin)
- RAY：[OKX listing page](https://www.okx.com/en-gb/help/okx-will-list-raydium-ray-token-for-spot-trading)

### 时间、来源和修订

- `announced_at`、`effective_at` 和 `source_published_at` 都带 precision。
- `day`、`minute`、`second` 和 `month` 不会被强行补成一个虚构午夜时间。
- lifecycle 与 evidence 分开：
  - lifecycle：scheduled、occurred、postponed、cancelled、superseded；
  - evidence：primary_confirmed、cross_checked、onchain_observed。
- 每条事实要求 HTTPS source、source kind、source check time、source record、record locator 和 record SHA-256。
- `record_sha256` 证明项目检查记录没有被静默改变，不证明第三方网页永远不变。
- 修订必须从 1 连续增加；Token 和 taxonomy identity 不能换；必须存在 material change。
- 相对于当前 publication root 的 prior latest bundle，已发布 revision 不能删除或原地改写；网站/API只返回 latest revision，bundle 保留完整历史。

### Compare 图上的表达规则

Event Fact 在 Compare 图上只承担四项功能：

1. 标出来源支持的 effective date 或 date interval。
2. 展示 event name、type、lifecycle 和时间精度。
3. 链接或展示 source/evidence/revision lineage。
4. 帮助用户定位“这一时间附近发生了什么已核查事实”。

明确不做：

- 不计算 event-window return；
- 不计算 abnormal return；
- 不判断价格、spread、volume 或 liquidity impact；
- 不按涨跌给事件贴正面/负面标签；
- 不用相关的时间位置宣称因果；
- 不把 secondary calendar 或 market first-observation 自动当成官方事件。

## 数据更新与网站发布流程

### 市场事实

```text
daily profile，每日 00:30 UTC：
incremental CEX/DEX OHLCV → TVL

depth profile，每小时 :05 UTC：
CEX depth/execution → 临时 GeckoTerminal DEX USD-price refresh
                    → DEX depth/execution
```

截至 2026-07-29 10:20:51 UTC，两个 systemd user timers 均为 active：

- `cex-dex-depth.timer`：上次 2026-07-29 10:05:43 UTC；
  下次 2026-07-29 11:05:00 UTC。
- `cex-dex-daily.timer`：上次 2026-07-29 00:30:43 UTC；
  下次 2026-07-30 00:30:00 UTC。

timer 是可变运行状态；今后每次正式汇报仍应现场重新执行
`systemctl --user list-timers`，不能只引用这次快照。

完整 profile 顺序：

```text
daily OHLCV → CEX depth/execution → TVL（同时刷新 DEX USD-price 输入）
           → DEX depth/execution
```

更新机制：

1. 采集器读取当前 catalog inventory。
2. TVL/CEX depth/DEX depth 保存 raw response、RPC transcript 或 structured
   failure；daily OHLCV 当前保存规范化来源字段，不宣称逐请求 raw 归档。
3. 规范化并验证 exact market/pool inventory。
4. Daily 采用 overlap incremental upsert，并 staged-build SQLite。
5. TVL/depth append history，再替换 latest。
6. Execution 验证每 market 五个 notional × 两个 direction 后替换 latest。
7. `collection.lock` 防止 daily/hourly 并发写。
8. manifest 保存命令、exit code、duration、log hash、file hash、coverage 和 freshness。
9. 后端在下一次 API request 时检测 source signature，清理旧 generation cache。
10. 浏览器看到新的 `data_generation` 后丢弃旧 Token catalog。

当前没有 WebSocket 推送。已经打开且没有再次发请求的页面，需要刷新、切页或重新触发 API 才能看到新 snapshot。

### Event Fact

```text
官方来源人工复核
    ↓
更新 evidence JSON 和 curated revision CSV
    ↓
python3 scripts/event_facts.py
    ↓
人工检查 manifest、CSV、SQLite 和原始来源
    ↓
python3 scripts/event_facts.py --publish-local
    ↓
原子切换 latest.json
```

Event Fact 没有伪装成 hourly feed。它的 freshness 是 source-check freshness；演示或生产发布前，应重新打开所有可见事件来源并更新检查证据。

### 应用部署

当前部署使用可追溯但较轻量的 release 流程：

- 代码以已 push 的精确 Git commit 部署，回滚时切回上一已验证 commit；
- Event data 先构建并校验不可变 bundle，再原子切换 `latest.json`；
- 切换前使用服务器实际 Python 解释器做 compile、import 和测试；
- 切换后重启 systemd user service，并运行 health、release checker 和浏览器
  smoke；
- 任一检查失败，就恢复上一代码 commit 或上一健康 data pointer。

## 质量和测试证据

### 已实现的 fail-closed 规则

- Missing 保持 null，不 forward-fill，不替换为零。
- Daily spread 只在相同 UTC 日两边价格有效时计算。
- CEX depth level 截断产生 partial/lower-bound，不冒充 complete。
- DEX 无协议特定、项目内实现并通过验证的 adapter 时保持 unsupported。
- Execution partial 不发布完整请求 VWAP/cost。
- Event source、时间精度、taxonomy、market identity 和 revision history 不合规时拒绝发布。
- Event bundle 的 pointer、manifest、CSV 和 SQLite hash 全部通过后才读取。
- 发布期间源 generation 改变时，API 放弃旧响应并重建。

### 本次发布证据摘要

```text
Deployed app SHA:   3524528b5160f441d0312c66504706377230ff1f
Server checkout:    3524528b5160f441d0312c66504706377230ff1f
Test command:       python3 -m unittest discover -s tests -p 'test_*.py'
Local tests:        340 run / 0 skipped / 0 failed / 0 errors
Server tests:       340 run / 29 Node-dependent skipped / 0 failed / 0 errors
Health:             ok / data_ready=true / current
Release checker:    passed / 30 Token / 493 markets / 44 Event facts / 30 covered
Browser smoke:      representative Event routes + lifecycle + Token switch + 320 px passed
Public demo URL:    http://43.156.102.166:8765
Data generation:    f0408687aa056557e3711f9383755c82c85f92799081c35d514ef6a6a2c26345
Event bundle ID:    236accecf312dc79ca6f8630
Public admin:       /admin.html 404 / /api/admin/status 404
```

测试结果对应 deployed application SHA；汇报材料本身随后以 docs-only commit
进入同一分支，因此 branch HEAD 可以晚于服务器 application SHA。

### 单次 release-checker 延迟样本

| Endpoint | 单次 elapsed |
| --- | ---: |
| `/health` | 306.20 ms |
| summary | 1,248.93 ms |
| token catalog | 1,913.67 ms |
| full catalog | 1,942.64 ms |
| all events | 312.79 ms |
| scoped Event endpoints | 多数约 280–320 ms；本轮最慢 1,303.11 ms |
| compare | 482.25 ms |
| quality | 303.10 ms |
| execution | 327.58 ms |

这只是一次公开入口 release-checker latency sample，不是 load test，也不能证明并发
吞吐。部署后第一次完整 catalog 请求曾观察到约 12.65 秒；后续本轮样本为
1.94 秒，payload 仍约 1.45 MB raw / 170 KB gzip。它说明单进程 cold-build、
catalog 体积和公网路径仍是性能瓶颈；缓存后的单次结果不能被包装成低延迟能力。

## 明确边界

1. 看板不输出买卖信号、目标价、未来回报或投资建议。
2. Event Fact 是事实时间轴，不是 event study，也不宣称事件造成价格变化。
3. Daily OHLCV 与 TVL/depth/execution 属于不同时间尺度。
4. CEX/DEX 快照依次采集，不是原子同步市场状态。
5. TVL 是 source-reported reserve，不等于 executable depth。
6. CEX depth 只看可见公开订单簿，不含 hidden liquidity。
7. DEX depth 不包含 gas、MEV、router 行为、Token tax 或 block 后状态变化。
8. Quoted execution cost 不是 realized cost 或 all-in cost。
9. CEX account-specific fee tier 未采集，也没有假定为零。
10. 支持的 DEX pool swap fee 可以进入 pool mechanics，但不代表所有外部交易成本。
11. DEX V3 depth 可在项目内实现并通过验证的协议特定 adapter 上计算；V3 fixed-notional execution 当前仍 unsupported。
12. Curve、Balancer、V4 hooks、部分 Algebra/zk/Solana 等协议没有适配时保持 unsupported。
13. Funding rate 当前未采集。
14. TVL/depth/execution 在看板上主要是 latest snapshot，不应声称已有完整历史回测面板。
15. TVL/depth/execution 跟随已发布 catalog；full DEX OHLCV rebuild 可以重新 discovery/选择 leading pools，但仍不等于全链 pool inventory。
16. Event 已完成 30/30 Token presence coverage，但每个 Token 的历史深度仍有限；
    只能说“每个配置 Token 至少一条已验证事实”，不能说覆盖所有 unlock、
    airdrop 和 listing。
17. `record_sha256` 证明本项目的 source-check record 完整，不是网页永久存证。
18. 当前后端是单进程和进程内 cache，不是低延迟撮合或实时流系统。
19. 裸 IP/8765 不等于生产安全；正式公开需要 HTTPS reverse proxy。
20. Public admin 默认关闭，普通用户不能从看板修改数据。
21. 当前 DEX depth 只有 84/148 rows 为 observed；DEX execution 只有
    190/1,480 rows 为 observed 或 partial，其余 1,290 rows 因协议/链未适配而
    unsupported。这是 fail-closed 的真实覆盖，不应包装成全面 DEX 支持。
22. DEX execution CSV 的历史 machine enum `included_protocol_fee` 实际语义是
    pool swap fee；网站已经只显示 “pool swap fee”。这个 legacy schema label
    应在独立兼容迁移中清理，不能解释成协议金库收取的 protocol fee。
23. 完整 catalog 首次公开请求曾约 12.65 秒，后续样本约 1.94 秒；当前没有并发
    load-test 证据。

## 下一阶段

建议顺序：

1. 给当前 demo 加 HTTPS reverse proxy、访问日志、限流和部署监控，形成 hardened production 入口。
2. 为 funding rate 建立 derivative market identity、contract type、interval、annualization、settlement 和 missing-value contract，再开始采集。
3. 30/30 Token presence coverage 已完成；下一步建立 29 个官方页面的定期
   source re-check，并逐 Token 补深历史，而不是把 presence 误写成完整覆盖。
4. 继续保持 Event Fact 和 event study 分离；只有在单独定义 estimation window、event window、benchmark、overlap、multiple testing 和 revision policy 后，才进入因果或异常收益研究。
5. 为 DEX V3 fixed-notional execution 实现协议精确整数 SwapMath 或经过项目验证的 same-block Quoter，并进一步缩短独立 USD quote 与 pool block 的时间差。
6. 如要比较 CEX fee，先建立公开费率、用户 tier、maker/taker、VIP、返佣和时间版本规则；不能使用一个统一假设费率。
7. 将 cold summary/catalog 的底层 SQLite 查询进一步按 Token/window 分区，减少单进程 cold-build 压力。
8. 在事实层稳定后再增加历史 TVL/depth/execution partitioned store，而不是让单个 CSV 无限增长。

## 高概率 Q&A

### Q1：这个看板和 CoinMarketCap、DefiLlama 或交易所页面有什么不同？

它不是另一个只报价格或 TVL 的聚合页。核心差异是同一个 Token 下保留具体 CEX pair 和 DEX pool identity，把 daily facts、point-in-time liquidity、execution mechanics、quality status 和 source lineage 放进同一个可分享工作流，并明确哪些指标不能互相替代。

### Q2：为什么不直接告诉我哪个市场最好？

“最好”需要用户目标、交易方向、金额、账户费率、延迟、gas、MEV 和风险偏好。当前系统只报告可核查事实和 quoted mechanics。用户可以比较，但系统不会把不完整的成本范围包装成综合排名。

### Q3：TVL 高是不是代表滑点一定低？

不是。TVL 是池总储备的 source-reported snapshot；可执行深度取决于资产比例、V3 active tick liquidity、fee、当前价格位置和交易方向。看板不会把 TVL 转成 depth。

### Q4：Execution cost 就是滑点吗？

更精确的说法是 quoted execution shortfall。它包括 captured state 下的 spread/price impact，并在支持的 DEX 中纳入 pool swap fee；但它不包括 CEX account fee、gas、router、MEV、延迟和 block 后变化，因此不能称为 realized all-in slippage。

### Q5：不同 DEX pool 不是公式不同吗？

是。因此系统按 adapter 区分 V2 constant-product、V3 concentrated liquidity 和 unsupported protocol。没有协议特定、项目内实现并通过测试验证的实现时返回 null/unsupported，而不是套一个通用 `x*y=k`。

### Q6：CEX 和 DEX 深度能直接比较吗？

可以比较统一的“价格偏离 band 内 quote notional”输出，但必须保留方法和时间差异。CEX 是 REST order-book snapshot；DEX 是 fixed-block pool state。它们不是同一时刻的保证成交量，所以页面同时显示来源、状态和 snapshot skew。

### Q7：为什么 Event Fact 不直接算事件前后收益？

事件标记与价格靠得近不构成因果。正式 event study 需要预先定义事件窗口、估计窗口、benchmark、重叠事件、提前反应、多个检验和修订政策。当前层只先保证事件本身的日期、来源和版本可信。

### Q8：事件来源怎么保证可靠？

第一版只接受允许的官方 project、governance、exchange 或直接 onchain evidence；每条记录有 source URL、检查时间、evidence status、record locator 和 hash。它仍然不是“绝对真相”，所以 lifecycle 与 revision history 都保留。

### Q9：如果官方把 unlock 日期改了怎么办？

在当前 publication root 中不能覆盖旧行。对同一个 `event_id` 增加连续的新 revision，写明 revision reason，并在 bundle 保留旧 revision；网页/API显示 latest revision。延期、取消和 superseded 也有明确 lifecycle。若整个 publication root 被删除，程序本身不是外部永久存证。

### Q10：为什么 announcement time 和 effective time 要分开？

因为“什么时候宣布”和“什么时候发生”是两个不同事实。Listing 的公告日、充值开放、call auction、现货开盘和提币时间也不能混成一个事件。Compare marker 使用的是 contract 定义的 effective time。

### Q11：数据多久更新一次？

截至 2026-07-29 10:20:51 UTC，demo/staging 部署服务器上的
`cex-dex-daily.timer`
和 `cex-dex-depth.timer` 都是 active：daily OHLCV 与 TVL 每日 00:30 UTC，
depth/execution 每小时第 05 分更新。timer 状态会变化，所以每次正式展示仍要用
`systemctl --user list-timers` 现场重验。Event Fact 是 curated source-check
feed，不伪装成小时级实时数据；正式展示前应重新检查可见来源。

### Q12：网站如何知道文件更新了？

采集器验证后原子替换 SQLite 或 latest CSV。后端在 API 请求时读取 source signature，generation 改变就清空旧 cache；Market 前端看到新的 `data_generation` 后丢弃旧 Token catalog，Event API使用 `bundle_id` 并由 Event source signature 触发 cache invalidation。当前没有 WebSocket 主动推送。

### Q13：特殊值怎么处理？

真实数值零保留为 `0`；缺失是 `null/missing`；指标不适用在页面显示 `N/A`；
当前 adapter/协议未支持是 `unsupported`；本次采集或计算错误是 `failed`；
可观测但不完整是 `partial`；整个 publication/feed 不能读取是 `unavailable`。
Event feed `available + 0 rows` 则只表示当前筛选没有匹配记录。不同状态不能压成一个
0 或一个笼统 warning。

### Q14：为什么同时保留 CSV 和 SQLite？

CSV 是人工审阅和跨工具审计格式；SQLite 是网站的索引查询层。Daily 发布先验证 CSV，再 staged-build SQLite，通过 integrity 和 row-count 检查后替换。Event Fact 同时保留 revision CSV、latest CSV、SQLite 和 manifest，是为了版本历史和读取效率。

### Q15：目前手续费和 funding rate 做到哪里？

支持的 DEX pool swap fee 会进入对应 pool mechanics；CEX account-specific taker fee 明确排除，没有假定为零。Funding rate 尚未采集，因为当前 catalog 是 CEX spot 和 DEX pool，衍生品身份和结算口径需要独立 contract。

### Q16：能不能增加新 Token 或 pool？

可以，但不是在页面上随便输入 symbol。Token、chain、contract、CEX instrument 和 DEX pool 都需要稳定 identity、来源覆盖和 inventory validation。TVL/depth/execution 跟随已发布 catalog；full DEX OHLCV rebuild 可以重新发现并选择 leading pools，但不负责无边界地枚举全链资产。

### Q17：这套系统能用于实盘吗？

不能直接作为交易执行器。它适合研究、事实核查、venue screening 和数据质量诊断。实盘还需要 WebSocket book maintenance、下单和风险控制、账户 fee、latency、gas/MEV、reorg 和失败处理。

### Q18：如何证明上线版本就是测试的版本？

正式汇报展示 deployed application SHA 与 message、明确 push 目标、服务器精确
checkout SHA、服务器 Python preflight、local/server 两套测试、`/health`、
release checker、运行 `data_generation`、Event `bundle_id` 和浏览器 smoke。
本次 application SHA 与 server checkout 都是
`3524528b5160f441d0312c66504706377230ff1f`。汇报文档是随后单独提交的
docs-only commit，所以 branch HEAD 晚于服务器 application SHA 是有意设计，
不是版本不一致。
