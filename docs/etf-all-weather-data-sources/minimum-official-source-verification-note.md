# Minimum Official-Source Verification Note

## Purpose

This note closes one specific pre-development gap:

> are the official macro/rates anchors only conceptual, or do they have minimally verified direct-access paths in this environment?

This is not a full official-source ingestion project.

It is a minimum recovery-path check.

---

## Scope

Representative official-source families checked:

- `NBS`
- `PBOC`
- `Shibor`
- `Chinamoney`
- `ChinaBond`

Goal:
- verify one practical direct path per source family where possible
- record friction and operational verdict

---

## 1. `NBS` — official macro release path

### Representative path

- `https://www.stats.gov.cn/sj/zxfb/`

### Observed result

- page accessible in this environment
- recent release items visible directly from the official release page
- confirmed examples visible on-page included:
  - `2026年3月份居民消费价格同比上涨1.0%`
  - `2026年3月份工业生产者出厂价格同比由降转涨 环比涨幅扩大`
  - `2026年3月中国采购经理指数运行情况`
  - `2026年1—2月份全国固定资产投资同比增长1.8%`
  - `2026年1—2月份社会消费品零售总额增长2.8%`

### Friction

- the site is article/release oriented rather than panel-data oriented
- extracting structured history directly would still require parsing discipline

### Operational verdict

- `pass_with_caveat`

### Interpretation

- this is a real fallback path for release-date confirmation and official-text validation
- it is usable as an official anchor even if wrapper APIs drift

---

## 2. `PBOC` — official money / credit path

### Representative path

- `https://www.pbc.gov.cn/diaochatongjisi/116219/index.html`

### Observed result

- page accessible in this environment
- visible official statistics/navigation entries included:
  - `2026年统计数据`
  - `2026年一季度金融统计数据报告`
  - `2026年2月金融统计数据报告`
  - `2026年1月金融统计数据报告`

### Friction

- navigation is portal-style rather than API-style
- usable direct paths exist, but extracting a structured historical panel would still need page parsing or manual link discovery

### Operational verdict

- `pass_with_caveat`

### Interpretation

- the direct official path for money / credit releases is operationally real
- this is enough to treat PBOC as a recoverable anchor rather than a purely conceptual source

---

## 3. `Shibor` — official interbank quote path

### Representative path

- `https://www.shibor.org/shibor/web/html/shibor.html`

### Observed result

- direct fetch attempts timed out in this environment
- both browser-like fetch and command-line fetch failed within the tested timeout window

### Friction

- likely network / reachability / site-behavior sensitivity in this environment
- this means the official source exists conceptually, but direct access is not currently dependable here

### Operational verdict

- `fail_as_direct_path_in_current_environment`

### Interpretation

- for now, `Shibor` should be treated as an official reference anchor, not as a trusted direct ingestion path in this repo environment
- wrapper or secondary route remains necessary operationally

---

## 4. `Chinamoney` — official liquidity / market context path

### Representative path

- `https://www.chinamoney.com.cn/chinese/bkcurv/`

### Observed result

- HTTP header path is reachable and returns `200 OK`
- command-line body fetch did not yield stable readable content in this quick verification
- earlier higher-level fetch also did not return a clean rendered page in this environment

### Friction

- likely page rendering, anti-bot, or transport behavior is less friendly than the raw header result suggests
- this is not a clean “curl and parse” source in the current environment

### Operational verdict

- `pass_with_major_caveat`

### Interpretation

- the site is reachable, so it is not a purely imaginary fallback
- but the direct extraction path is operationally awkward enough that it should not be counted as a frictionless emergency source

---

## 5. `ChinaBond` — official curve path

### Representative path

- `https://yield.chinabond.com.cn/`

### Observed result

- page accessible in this environment
- the yield-curve interface rendered content including:
  - yield-curve views
  - maturity term selections
  - curve type selections
  - Shibor/repo comparison options

### Friction

- page is UI-heavy and not designed as a simple bulk-history API
- still requires extraction discipline and likely careful handling for historical automation

### Operational verdict

- `pass_with_caveat`

### Interpretation

- this is a real official direct path for curve-related verification
- it is suitable as an official recovery anchor, though not as a low-friction bulk pipeline by itself

---

## Overall Judgment

The official-source layer is now minimally verified as follows:

- `NBS`: operational direct path exists
- `PBOC`: operational direct path exists
- `ChinaBond`: operational direct path exists
- `Chinamoney`: partially reachable but operationally awkward
- `Shibor`: not dependable as a direct path in the current environment

This is enough to support the core research claim:

> the official-source layer is not merely conceptual; the repo has at least partial real fallback paths for key macro and curve anchors if wrapper routes drift.

But the verification also sharpens an important boundary:

> official anchor does not mean low-friction source.

For v1, this means:

- keep wrappers for convenience
- keep official pages for confirmation and recovery
- do not assume every official source is directly automation-friendly in the current environment
