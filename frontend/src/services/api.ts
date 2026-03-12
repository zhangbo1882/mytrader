import axios, {
  AxiosError,
  InternalAxiosRequestConfig,
  type AxiosInstance,
  type AxiosRequestConfig,
  type AxiosResponse,
} from 'axios';
import { message } from 'antd';

export interface ApiError {
  error?: string;
  message?: string;
}

type ApiRequestConfig<D = unknown> = AxiosRequestConfig<D>;

interface ApiInstance extends Omit<AxiosInstance, 'request' | 'get' | 'delete' | 'head' | 'options' | 'post' | 'put' | 'patch'> {
  request<T = unknown, D = unknown>(config: ApiRequestConfig<D>): Promise<T>;
  get<T = unknown, D = unknown>(url: string, config?: ApiRequestConfig<D>): Promise<T>;
  delete<T = unknown, D = unknown>(url: string, config?: ApiRequestConfig<D>): Promise<T>;
  head<T = unknown, D = unknown>(url: string, config?: ApiRequestConfig<D>): Promise<T>;
  options<T = unknown, D = unknown>(url: string, config?: ApiRequestConfig<D>): Promise<T>;
  post<T = unknown, D = unknown>(url: string, data?: D, config?: ApiRequestConfig<D>): Promise<T>;
  put<T = unknown, D = unknown>(url: string, data?: D, config?: ApiRequestConfig<D>): Promise<T>;
  patch<T = unknown, D = unknown>(url: string, data?: D, config?: ApiRequestConfig<D>): Promise<T>;
  defaults: typeof axios.defaults;
  interceptors: typeof axios.interceptors;
}

const axiosInstance = axios.create({
  baseURL: '/api',
  timeout: 60000,
  headers: {
    'Content-Type': 'application/json',
  },
});

const api = axiosInstance as unknown as ApiInstance;

api.interceptors.request.use(
  (config: InternalAxiosRequestConfig) => config,
  (error) => Promise.reject(error)
);

api.interceptors.response.use(
  <T>(response: AxiosResponse<T>) => response.data,
  (error: AxiosError<ApiError>) => {
    const errorMessage = error.response?.data?.error || error.response?.data?.message || error.message || '请求失败';
    message.error(errorMessage);
    return Promise.reject(new Error(errorMessage));
  }
);

export default api;
