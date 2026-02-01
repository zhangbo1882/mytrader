# 任务执行卡住问题修复

## 问题描述

用户报告：更新管理，任务一直运行没有结束

## 根本原因

在修复停止功能时，我在代码中引入了一个bug：

```python
# 有bug的代码 (修复前)
if tm.is_stop_requested(task_id):
    tm.save_checkpoint(task_id, i, tm.get_task(task_id).get('stats'))
    #                              ^^^^^^^^^^^^^^^^^^^^^^
    #                              如果get_task返回None会崩溃
```

当`tm.get_task(task_id)`返回`None`时，调用`.get('stats')`会抛出：
```
AttributeError: 'NoneType' object has no attribute 'get'
```

这导致任务线程崩溃，任务状态永远停留在`running`。

## 修复方案

在访问属性前添加None检查：

```python
# 修复后的代码
if tm.is_stop_requested(task_id):
    task = tm.get_task(task_id)
    task_stats = task.get('stats') if task else stats  # None检查
    tm.save_checkpoint(task_id, i, task_stats)
    tm.update_task(task_id, status='stopped', message='任务已停止')
    tm.clear_stop_request(task_id)
    return
```

## 修复位置

**文件**: `web/app.py`
- **run_update_all_stocks_recovery** 函数（第148-168行）
- **run_update_favorites_recovery** 函数（无需修复）

## 测试步骤

### 方法1：浏览器UI测试

1. **刷新页面**
   ```
   按 Ctrl+Shift+R (或 Cmd+Shift+R) 强制刷新
   这会清除前端的缓存和轮询
   ```

2. **创建新任务**
   - 访问 http://localhost:5001
   - 切换到"更新管理" tab
   - 选择：自定义股票
   - 输入：600382
   - 选择：增量更新
   - 点击"开始更新"

3. **观察任务执行**
   - 任务应该在10-30秒内完成
   - 状态从"运行中"变为"已完成"
   - 进度显示 100%

### 方法2：清理数据库后测试

如果前端轮询问题持续：

```bash
# 1. 停止服务器
pkill -f "python.*app.py"

# 2. 清理所有running任务
sqlite3 /Users/zhangbo/Public/go/github.com/mytrader/data/tasks.db \
  "DELETE FROM tasks WHERE status = 'running'"

# 3. 重启服务器
cd web && python app.py &

# 4. 在浏览器中测试
```

### 方法3：全新浏览器测试

```bash
# 使用无痕模式（避免缓存）
# Chrome: Cmd+Shift+N (Mac) / Ctrl+Shift+N (Windows)
# Firefox: Cmd+Shift+P (Mac) / Ctrl+Shift+P (Windows)
# 然后访问 http://localhost:5001
```

## 预期结果

| 阶段 | 预期行为 | 时间 |
|-----|---------|------|
| 创建任务 | 任务状态: pending | < 1秒 |
| 开始执行 | 任务状态: running | 1-2秒 |
| 更新数据 | 进度更新，消息更新 | 10-30秒 |
| 完成 | 任务状态: completed | 取决于数据量 |
| UI刷新 | 显示"更新完成" | 自动 |

## 验证命令

```bash
# 检查是否有running任务
sqlite3 /Users/zhangbo/Public/go/github.com/mytrader/data/tasks.db \
  "SELECT task_id, status, progress, message FROM tasks WHERE status = 'running'"

# 应该返回空结果（没有running任务）
```

## 常见问题

### Q1: 任务状态一直是running怎么办？

**A**: 可能是任务线程崩溃了。检查日志：
```bash
tail -50 /tmp/flask.log | grep -i error
```

如果看到`'NoneType' object has no attribute 'get'`，说明修复没有生效。

### Q2: 前端一直轮询旧任务？

**A**: 强制刷新页面（Ctrl+Shift+R），清除缓存。

### Q3: 如何查看任务是否真的在执行？

**A**: 检查Flask日志：
```bash
tail -f /tmp/flask.log | grep "正在更新"
```

应该看到类似：
```
正在更新 600382 (1/1)...
```

## 技术细节

### 为什么get_task会返回None？

`get_task`方法从数据库查询任务：
```python
def get_task(self, task_id):
    conn = sqlite3.connect(self.db_path, check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM tasks WHERE task_id = ?', (task_id,))
    row = cursor.fetchone()
    conn.close()

    if not row:
        return None  # 任务不存在
    return dict(row)
```

可能返回None的情况：
1. 任务被删除了
2. 数据库连接失败
3. task_id不正确

### 为什么之前没有这个问题？

之前的代码（停止功能修复前）会在循环开始前获取task对象：
```python
task = tm.get_task(task_id)  # 只获取一次
for i in range(start_index, len(stock_list)):
    if task.get('stop_requested'):  # 使用缓存的task
        ...
```

修改后，每次检查停止时都调用get_task，但没有处理None的情况。

## 相关修复

本次修复是系列bug修复的一部分：

1. **第一次修复**：删除重复的request_stop方法（死锁）
2. **第二次修复**：使用内存标志检查（停止功能）
3. **第三次修复**：智能删除策略（删除功能）
4. **第四次修复**：添加None检查（本次修复）

## 总结

通过添加None检查，我们修复了任务执行崩溃的问题：

- ✅ **修复NoneType错误**：防止任务线程崩溃
- ✅ **任务能正常完成**：从running变为completed
- ✅ **保持停止功能**：仍然可以使用内存标志快速停止
- ✅ **保持删除功能**：stopped任务可以快速删除

---

**修复日期**: 2026-01-31
**修复位置**: web/app.py:148-168
**影响范围**: 任务执行流程
**向后兼容**: 是
**破坏性变更**: 否
