import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'path';

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  server: {
    host: '0.0.0.0',  // 监听所有网络接口 (IPv4 和 IPv6)
    port: 5002,
    strictPort: true,  // 如果端口被占用则失败，不自动尝试其他端口
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:5001',
        changeOrigin: true,
        secure: false,
        ws: true,
        configure: (proxy, options) => {
          proxy.on('error', (err, req, res) => {
            console.log('proxy error', err);
          });
          proxy.on('proxyReq', (proxyReq, req, res) => {
            console.log('Sending:', req.method, req.url, 'to:', options.target);
          });
        },
      },
    },
  },
  build: {
    outDir: 'dist',
    sourcemap: true,
    // Optimize chunk size - set to 500kb for better bundle optimization
    chunkSizeWarningLimit: 500,
    rollupOptions: {
      output: {
        // More aggressive code splitting
        manualChunks: (id) => {
          // React core
          if (id.includes('node_modules/react/') || id.includes('node_modules/react-dom/')) {
            return 'react-core';
          }
          // React Router
          if (id.includes('node_modules/react-router/')) {
            return 'react-router';
          }
          // Ant Design
          if (id.includes('node_modules/antd/') || id.includes('node_modules/@ant-design/')) {
            return 'antd';
          }
          // Charts
          if (id.includes('node_modules/recharts/') || id.includes('node_modules/d3-')) {
            return 'charts';
          }
          // State management
          if (id.includes('node_modules/zustand/')) {
            return 'state';
          }
          // Date handling
          if (id.includes('node_modules/dayjs/')) {
            return 'date';
          }
          // HTTP client
          if (id.includes('node_modules/axios/')) {
            return 'http';
          }
          // Other node_modules
          if (id.includes('node_modules')) {
            return 'vendor';
          }
        },
        // Optimize file names for better caching
        chunkFileNames: 'assets/js/[name]-[hash].js',
        entryFileNames: 'assets/js/[name]-[hash].js',
        assetFileNames: 'assets/[ext]/[name]-[hash].[ext]',
      },
    },
    // Enable compression
    minify: 'terser',
    terserOptions: {
      compress: {
        drop_console: true, // Remove console.log in production
        drop_debugger: true,
      },
    },
  },
  // Optimize dependencies
  optimizeDeps: {
    include: ['react', 'react-dom', 'react-router-dom', 'antd', 'axios', 'dayjs', 'zustand'],
  },
});
