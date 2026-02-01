# AKShare vs Tushare 字段对比

## 测试结果总结

### 1. 字段命名差异

| 字段含义 | AKShare 字段名 | Tushare 字段名 | 数据类型 |
|---------|---------------|---------------|---------|
| 股票代码 | 股票代码 | ts_code | string |
| 日期 | 日期 | trade_date | string (YYYYMMDD) |
| 开盘价 | 开盘 | open | float64 |
| 最高价 | 最高 | high | float64 |
| 最低价 | 最低 | low | float64 |
| 收盘价 | 收盘 | close | float64 |
| 昨收价 | - | pre_close | float64 |
| 涨跌额 | 涨跌额 | change | float64 |
| 涨跌幅 | 涨跌幅 | pct_chg | float64 |
| 成交量 | 成交量 | vol | float64 |
| 成交额 | 成交额 | amount | float64 |
| 振幅 | 振幅 | - | float64 |
| 换手率 | 换手率 | - | float64 |

### 2. AKShare 独有字段

- **振幅**：当日振幅百分比
- **换手率**：当日换手率百分比
- **涨跌额**：当日涨跌金额（Tushare 也有 change 字段）

### 3. Tushare 独有字段

- **pre_close**：昨收价
- **ts_code**：带交易所后缀的股票代码（如 000001.SZ）

### 4. 数据格式差异

| 项目 | AKShare | Tushare |
|-----|---------|---------|
| 日期格式 | YYYY-MM-DD | YYYYMMDD |
| 股票代码 | 6位代码 | 6位代码.交易所（如 000001.SZ） |
| 成交量单位 | 股 | 手（100股） |
| 成交额单位 | 元 | 千元 |

### 5. 字段映射表（用于数据标准化）

```python
FIELD_MAPPING = {
    # AKShare -> 标准字段 -> Tushare
    '日期': 'datetime',
    '股票代码': 'symbol',
    '开盘': 'open',
    '收盘': 'close',
    '最高': 'high',
    '最低': 'low',
    '成交量': 'volume',
    '成交额': 'turnover',
    '涨跌幅': 'pct_chg',
    '涨跌额': 'change',
    '振幅': 'amplitude',
    '换手率': 'turnover_rate',

    # Tushare -> 标准字段
    'trade_date': 'datetime',
    'ts_code': 'symbol',
    'open': 'open',
    'close': 'close',
    'high': 'high',
    'low': 'low',
    'vol': 'volume',
    'amount': 'turnover',
    'pct_chg': 'pct_chg',
    'change': 'change',
    'pre_close': 'pre_close',
}
```

### 6. 数据转换注意事项

1. **日期转换**
   - AKShare: `2024-01-02` → 标准: `2024-01-02`
   - Tushare: `20240102` → 标准: `2024-01-02`

2. **股票代码转换**
   - AKShare: `000001` → 标准: `000001`
   - Tushare: `000001.SZ` → 标准: `000001`

3. **成交量单位转换**
   - AKShare: 股
   - Tushare: 手（100股）
   - 建议统一转换为股

4. **成交额单位转换**
   - AKShare: 元
   - Tushare: 千元
   - 建议统一转换为元

### 7. 建议的数据标准化策略

```python
def standardize_akshare_data(df):
    """标准化 AKShare 数据"""
    # 重命名列
    rename_map = {
        '日期': 'datetime',
        '股票代码': 'symbol',
        '开盘': 'open',
        '收盘': 'close',
        '最高': 'high',
        '最低': 'low',
        '成交量': 'volume',
        '成交额': 'turnover',
        '涨跌幅': 'pct_chg',
        '涨跌额': 'change',
        '振幅': 'amplitude',
        '换手率': 'turnover_rate',
    }
    df = df.rename(columns=rename_map)

    # 日期格式已经是 YYYY-MM-DD，不需要转换
    # 股票代码已经是6位，不需要转换

    return df


def standardize_tushare_data(df):
    """标准化 Tushare 数据"""
    # 重命名列
    rename_map = {
        'trade_date': 'datetime',
        'ts_code': 'symbol',
        'open': 'open',
        'close': 'close',
        'high': 'high',
        'low': 'low',
        'vol': 'volume',  # 手 -> 股（需要乘以100）
        'amount': 'turnover',  # 千元 -> 元（需要乘以1000）
        'pct_chg': 'pct_chg',
        'change': 'change',
        'pre_close': 'pre_close',
    }
    df = df.rename(columns=rename_map)

    # 日期格式转换 YYYYMMDD -> YYYY-MM-DD
    df['datetime'] = pd.to_datetime(df['datetime'], format='%Y%m%d').dt.strftime('%Y-%m-%d')

    # 股票代码转换 000001.SZ -> 000001
    df['symbol'] = df['symbol'].str.split('.').str[0]

    # 成交量：手 -> 股
    df['volume'] = df['volume'] * 100

    # 成交额：千元 -> 元
    df['turnover'] = df['turnover'] * 1000

    return df
```

### 8. 结论

1. **字段名不同**：AKShare 使用中文，Tushare 使用英文
2. **数据内容基本一致**：核心 OHLCV 数据都可以获取
3. **需要字段映射**：建议建立统一的字段名标准
4. **单位需要转换**：注意成交量和成交额的单位差异
5. **AKShare 更丰富**：提供了振幅和换手率等额外字段
6. **Tushare 更专业**：提供了 pre_close 等专业字段

### 9. 建议

- **优先使用 AKShare**：免费，无频率限制，字段更丰富
- **备选 Tushare**：作为备用数据源，当 AKShare 不可用时使用
- **统一数据格式**：建立标准化的字段名和单位
- **数据验证**：对比两个数据源的数据一致性
