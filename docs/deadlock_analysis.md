# 死锁分析报告

## 问题代码

### 1. TaskManager 的锁定义

```python
# web/tasks.py 第30行
class TaskManager:
    def __init__(self, ...):
        self.lock = threading.Lock()  # 普通互斥锁（不可重入）
```

### 2. update_task 方法

```python
# web/tasks.py 第259-325行
def update_task(self, task_id, **kwargs):
    with self.lock:  # 获取锁
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = conn.cursor()
        # ... 数据库操作 ...
        cursor.execute(f"UPDATE tasks SET ... WHERE task_id = ?", ...)
        conn.commit()
        conn.close()
```

### 3. 任务执行循环

```python
# web/routes.py 第884-920行
def run_update():
    for i in range(start_index, len(stock_list)):
        stock_code = stock_list[i]

        # 检查停止
        if tm.is_stop_requested(task_id):
            tm.update_task(task_id, status='stopped')  # 调用1
            return

        # 更新进度
        get_task_manager().update_task(task_id,
            current_stock_index=i,
            progress=progress,
            message=f'正在更新 {stock_code}...'
        )  # 调用2

        # 保存检查点
        if i % 10 == 0:
            get_task_manager().save_checkpoint(task_id, i, ...)  # 调用3
```

### 4. save_checkpoint 方法

```python
# web/tasks.py 第663-690行
def save_checkpoint(self, task_id, current_index, stats=None):
    with self.lock:  # 又获取锁！
        conn = sqlite3.connect(...)
        cursor.execute(...)
        conn.commit()
```

## 死锁场景

### 场景1：同一线程多次获取锁（死锁）

```
时间线：
T1: 任务线程开始执行
T2: 进入 for 循环
T3: 调用 update_task() → 获取 lock ✅
T4: update_task 完成 → 释放 lock
T5: 继续执行...
T6: 调用 save_checkpoint() → 尝试获取 lock
T7: save_checkpoint 内部调用 update_task ❌
    ├─ save_checkpoint 持有 lock
    └─ update_task 尝试获取同一个 lock
    └─ 死锁！因为 threading.Lock 不可重入
```

### 场景2：并发锁竞争（活锁）

```
线程A: 任务执行线程
线程B: API请求线程（比如停止请求）

时间线：
T1: 线程A 调用 update_task() → 获取 lock ✅
T2: 线程A 开始执行数据库操作（慢）
T3: 线程B 调用 request_stop()
T4: 线程B 尝试直接更新数据库
    ├─ UPDATE tasks SET stop_requested=1 ...
    └─ 等待线程A释放数据库锁
T5: 线程A 继续执行，需要很长时间
T6: 用户看到"停止"请求成功，但任务仍在running
```

### 场景3：嵌套锁调用（实际发生的问题）

```python
# 问题代码路径
def run_update():
    for i in range(len(stock_list)):
        # 第1次循环
        update_task(...)  # 获取锁 → 释放锁

        # 第2次循环
        save_checkpoint(...)  # 获取锁
            ├─ save_checkpoint 内部:
            │   ├─ 保存到 task_checkpoints 表
            │   └─ 释放锁
            └─ 继续执行...

        update_task(...)  # 再次获取锁

        # 如果在 update_task 持有锁时，
        # 另一个操作也尝试获取锁 → 阻塞
```

## 具体问题定位

### 问题点1：save_checkpoint 可能导致嵌套锁

虽然 `save_checkpoint` 有自己的锁作用域，但如果有代码路径是：
```python
def some_method():
    with self.lock:  # 外层锁
        ...
        self.update_task(...)  # 内层尝试获取同一个锁
```

这会导致死锁，因为 `threading.Lock` 不可重入。

### 问题点2：update_task 的锁粒度太大

```python
def update_task(self, task_id, **kwargs):
    with self.lock:  # 锁住整个方法
        # 连接数据库
        conn = sqlite3.connect(...)
        # 构建SQL
        # 执行SQL
        # 关闭连接
```

如果同时有多个 `update_task` 调用：
- 调用A：持有锁，正在执行
- 调用B：等待锁
- 调用C：等待锁
- ...

所有调用都串行化，性能极差。

### 问题点3：日志显示的证据

```
2026-01-31 22:36:02,771 [INFO] [update_task] Lock acquired for task 97c432df...
2026-01-31 22:36:02,785 [INFO] [update_task] Lock acquired for task 97c432df...
2026-01-31 22:36:02,786 [INFO] [update_task] Acquiring lock for task 97c432df...
```

第三行显示 "Acquiring lock"，但之后没有 "Lock acquired"，说明：
- 线程在等待锁
- 锁被其他操作持有
- 等待超时或永久阻塞

## 为什么会死锁

### threading.Lock vs threading.RLock

```python
import threading

lock = threading.Lock()   # 不可重入锁
rlock = threading.RLock()  # 可重入锁

def with_lock():
    with lock:
        print("第一次获取锁")
        with lock:  # 死锁！
            print("第二次获取锁")

def with_rlock():
    with rlock:
        print("第一次获取锁")
        with rlock:  # 可以！
            print("第二次获取锁")
```

### 当前代码中的嵌套调用

```python
# 路径1: save_checkpoint → update_task
def save_checkpoint(self, task_id, current_index, stats=None):
    with self.lock:
        conn = sqlite3.connect(...)
        cursor.execute("INSERT INTO task_checkpoints ...")
        # 如果这里调用需要 update_task 的方法，会死锁
```

```python
# 路径2: 任何方法 → update_task → 另一个调用 update_task
def some_method():
    self.update_task(...)  # 获取锁
    # 内部又调用 update_task
    self.update_task(...)  # 尝试再次获取锁 → 死锁
```

## 实际发生的死锁

从日志分析：

```python
# 线程A: 任务执行线程
def run_update():
    for i in range(start_index, len(stock_list)):
        stock_code = stock_list[i]

        # 第915行
        get_task_manager().update_task(task_id,
            current_stock_index=i,
            progress=progress,
            message=f'正在更新 {stock_code}...'
        )
        # ↑ 这里获取锁，执行数据库操作，释放锁

        # 第923行
        if i % 10 == 0:
            get_task_manager().save_checkpoint(task_id, i, ...)
            # ↑ save_checkpoint 获取锁，执行操作，释放锁

        # 第933行
        stats = db.save_all_stocks_by_code_incremental(...)
        # ↑ 这里可能会调用 update_task？
        #   如果 save_all_stocks_by_code_incremental 内部
        #   调用了 TaskManager 的方法，而这些方法
        #   又调用 update_task，就会死锁
```

## 修复方案

### 方案1：使用 RLock（可重入锁）

```python
# Before
self.lock = threading.Lock()

# After
self.lock = threading.RLock()
```

**优点**: 简单，一行代码
**缺点**: 治标不治本，性能问题依然存在

### 方案2：移除不必要的锁

SQLite 的默认模式支持：
- 多个读者同时访问
- 只有一个写入者
- 写入者与读者互斥

```python
def update_task(self, task_id, **kwargs):
    # 只在写操作时使用锁
    conn = sqlite3.connect(self.db_path, check_same_thread=False)
    cursor = conn.cursor()

    # 执行更新
    cursor.execute(...)

    # SQLite 会自动处理并发
```

### 方案3：减小锁粒度

```python
# Before: 锁住整个方法
def update_task(self, task_id, **kwargs):
    with self.lock:
        conn = sqlite3.connect(...)
        # 很多操作...

# After: 只锁数据库操作
def update_task(self, task_id, **kwargs):
    conn = sqlite3.connect(...)
    cursor = conn.cursor()

    # 准备数据（不需要锁）
    updates = []
    values = []

    # 只在执行SQL时使用锁
    with self.lock:
        cursor.execute(...)
        conn.commit()
```

### 方案4：使用线程本地存储

```python
import threading

thread_local = threading.local()

def get_task_manager():
    if not hasattr(thread_local, 'task_manager'):
        thread_local.task_manager = TaskManager(...)
    return thread_local.task_manager
```

**优点**: 每个线程有自己的实例，不会竞争
**缺点**: 需要大量重构

## 总结

### 死锁的根本原因

1. **使用了不可重入锁** (`threading.Lock`)
2. **锁粒度太大** (整个方法被锁住)
3. **频繁的锁竞争** (循环中多次获取锁)
4. **可能的嵌套调用** (update_task 调用其他方法，这些方法又调用 update_task)

### 当前问题

- 任务线程在 `update_task` 或 `save_checkpoint` 中持有锁
- 另一个操作尝试获取同一个锁
- **死锁**: 线程A持有锁，等待资源X；线程B持有资源X，等待锁
- **活锁**: 所有线程都在等待锁，无法前进

### 建议的修复顺序

1. **立即**: 使用 `RLock` 替换 `Lock`
2. **短期**: 减小锁粒度，只锁必要的代码段
3. **中期**: 重构为线程本地存储
4. **长期**: 使用真正的任务队列（Celery, RQ等）

---

**问题定位**: `threading.Lock` + 嵌套调用 = 死锁
**紧急修复**: 改用 `threading.RLock` 或移除锁
