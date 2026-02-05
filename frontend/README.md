# MyTrader Frontend

智能股票分析系统的现代化 React 前端应用。

## 技术栈

- **构建工具**: Vite 6.x
- **前端框架**: React 18.3+
- **语言**: TypeScript 5.x
- **UI组件库**: Ant Design 5.x
- **路由**: React Router v7
- **状态管理**: Zustand
- **HTTP客户端**: axios
- **图表库**: Recharts
- **日期处理**: dayjs
- **代码规范**: ESLint + Prettier
- **测试**: Vitest + Playwright

## 项目结构

```
frontend/
├── src/
│   ├── main.tsx                 # 入口文件
│   ├── App.tsx                  # 根组件
│   ├── vite-env.d.ts            # Vite 类型声明
│   │
│   ├── pages/                   # 页面组件（路由级别）
│   │   ├── QueryPage.tsx        # 股票查询
│   │   ├── AIScreenPage.tsx     # AI智能筛选
│   │   ├── PredictionPage.tsx   # AI预测
│   │   ├── FavoritesPage.tsx    # 我的收藏
│   │   ├── UpdatePage.tsx       # 更新管理
│   │   ├── TasksPage.tsx        # 任务历史
│   │   ├── FinancialPage.tsx    # 财务数据
│   │   └── BoardsPage.tsx       # 板块中心
│   │
│   ├── components/              # 通用组件
│   │   ├── common/              # 通用组件
│   │   ├── query/               # 查询相关组件
│   │   ├── ai/                  # AI相关组件
│   │   ├── tasks/               # 任务相关组件
│   │   └── financial/           # 财务相关组件
│   │
│   ├── layouts/                 # 布局组件
│   │   └── AppLayout.tsx        # 主布局
│   │
│   ├── hooks/                   # 自定义Hooks
│   │   ├── useQuery.ts          # 股票查询
│   │   ├── useFavorites.ts      # 收藏管理
│   │   ├── useTaskPolling.ts    # 任务轮询
│   │   ├── useBoardData.ts      # 板块数据
│   │   └── useFinancialData.ts  # 财务数据
│   │
│   ├── stores/                  # Zustand状态管理
│   │   ├── queryStore.ts        # 查询状态
│   │   ├── favoriteStore.ts     # 收藏状态
│   │   ├── taskStore.ts         # 任务状态
│   │   ├── boardStore.ts        # 板块状态
│   │   └── uiStore.ts           # UI状态
│   │
│   ├── services/                # API服务层
│   │   ├── api.ts               # axios实例配置
│   │   ├── stockService.ts      # 股票相关API
│   │   ├── taskService.ts       # 任务相关API
│   │   ├── boardService.ts      # 板块相关API
│   │   └── financialService.ts  # 财务相关API
│   │
│   ├── utils/                   # 工具函数
│   │   ├── formatters.ts        # 格式化函数
│   │   ├── dateUtils.ts         # 日期工具
│   │   ├── storage.ts           # localStorage封装
│   │   └── constants.ts         # 常量定义
│   │
│   ├── types/                   # TypeScript类型定义
│   │   ├── stock.types.ts
│   │   ├── task.types.ts
│   │   ├── board.types.ts
│   │   ├── financial.types.ts
│   │   └── common.types.ts
│   │
│   └── styles/                  # 样式文件
│       └── global.css           # 全局样式
│
├── tests/                       # 测试文件
│   └── e2e/                     # E2E测试
│
├── index.html
├── vite.config.ts
├── tsconfig.json
├── playwright.config.ts
├── package.json
└── README.md
```

## 开发指南

### 安装依赖

```bash
cd frontend
npm install
```

### 启动开发服务器

```bash
npm run dev
```

访问 http://localhost:5173

### 构建生产版本

```bash
npm run build
```

构建产物将输出到 `dist/` 目录。

### 代码检查

```bash
# 运行 ESLint
npm run lint

# 自动修复
npm run lint:fix

# 格式化代码
npm run format
```

### 运行测试

```bash
# 单元测试
npm run test

# E2E测试
npm run test:e2e

# E2E测试UI模式
npm run test:e2e:ui
```

## 核心架构

### 状态管理（Zustand）

使用 Zustand 进行轻量级状态管理，支持 localStorage 持久化：

```typescript
// stores/queryStore.ts
import { useQueryStore } from '@/stores';

function MyComponent() {
  const { symbols, setSymbols, results } = useQueryStore();
  // ...
}
```

### API服务层

统一的 API 调用封装，包含错误处理和拦截器：

```typescript
// services/stockService.ts
import { stockService } from '@/services';

const data = await stockService.query(params);
```

### 自定义Hooks

封装可复用的业务逻辑：

```typescript
// hooks/useQuery.ts
import { useQuery } from '@/hooks';

function QueryPage() {
  const { executeQuery, loading, error, results } = useQuery();
  // ...
}
```

## 环境变量

创建 `.env` 文件配置环境变量：

```bash
VITE_API_BASE_URL=http://localhost:5000/api
```

## 后端API对接

开发服务器已配置代理，所有 `/api` 请求将转发到 `http://localhost:5000`。

在生产环境中，确保后端CORS配置正确。

## 部署

### 开发环境

```bash
npm run dev
```

### 生产环境

```bash
npm run build
```

将 `dist/` 目录部署到 Nginx 或其他静态文件服务器。

### 与Flask集成

如需通过Flask托管静态文件，修改Flask路由：

```python
@app.route('/')
@app.route('/query')
@app.route('/ai-screen')
# ... 所有前端路由
def serve_react_app():
    return send_from_directory('frontend/dist', 'index.html')

@app.route('/assets/<path:path>')
def serve_assets(path):
    return send_from_directory('frontend/dist/assets', path)
```

## 浏览器支持

- Chrome (推荐)
- Firefox
- Safari
- Edge

## 开发规范

### 组件命名

- 页面组件：`XxxPage.tsx`
- 业务组件：`XxxComponent.tsx`
- 通用组件：`Xxx.tsx`

### 目录组织

- 按功能模块组织代码
- 相关文件放在同一目录下
- 使用 `index.ts` 统一导出

### TypeScript

- 优先使用 TypeScript
- 避免使用 `any`
- 导入类型使用 `type` 关键字

## 故障排查

### 依赖安装失败

```bash
# 清除缓存重新安装
rm -rf node_modules package-lock.json
npm install
```

### 开发服务器启动失败

```bash
# 检查端口占用
lsof -i :5173

# 使用其他端口
npm run dev -- --port 3000
```

### 构建失败

```bash
# 检查 TypeScript 错误
npx tsc --noEmit
```

## 许可证

MIT
