import axios, { AxiosError, AxiosInstance, AxiosRequestConfig, AxiosResponse } from 'axios';
import { message } from 'antd';

// Create a typed axios instance that returns response.data directly
const api = axios.create({
  baseURL: '/api',
  timeout: 60000,
  headers: {
    'Content-Type': 'application/json',
  },
}) as AxiosInstance;

// Request interceptor
api.interceptors.request.use(
  (config) => {
    return config;
  },
  (error) => {
    return Promise.reject(error);
  }
);

// Response interceptor - returns response.data directly
api.interceptors.response.use(
  (response) => {
    return response.data;
  },
  (error: AxiosError<{ error?: string; message?: string }>) => {
    const errorMessage = error.response?.data?.error || error.response?.data?.message || error.message || '请求失败';
    message.error(errorMessage);
    return Promise.reject(new Error(errorMessage));
  }
);

export default api;
