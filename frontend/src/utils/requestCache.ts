/**
 * Request cache and deduplication utility
 * Prevents duplicate concurrent requests and caches responses
 */

interface CacheEntry {
  data: any;
  timestamp: number;
}

interface PendingRequest {
  promise: Promise<any>;
  timestamp: number;
}

class RequestCache {
  private cache: Map<string, CacheEntry> = new Map();
  private pending: Map<string, PendingRequest> = new Map();
  private defaultTTL: number = 5 * 60 * 1000; // 5 minutes default

  constructor(tTL?: number) {
    if (tTL) {
      this.defaultTTL = tTL;
    }
  }

  /**
   * Generate cache key from URL and params
   */
  private generateKey(url: string, params?: Record<string, any>): string {
    if (!params) return url;
    const paramString = JSON.stringify(params, Object.keys(params).sort());
    return `${url}:${paramString}`;
  }

  /**
   * Get cached data if exists and not expired
   */
  get(url: string, params?: Record<string, any>): any | null {
    const key = this.generateKey(url, params);
    const entry = this.cache.get(key);

    if (!entry) return null;

    const now = Date.now();
    if (now - entry.timestamp > this.defaultTTL) {
      this.cache.delete(key);
      return null;
    }

    return entry.data;
  }

  /**
   * Set cache data
   */
  set(url: string, data: any, params?: Record<string, any>, ttl?: number): void {
    const key = this.generateKey(url, params);
    this.cache.set(key, {
      data,
      timestamp: Date.now(),
    });

    // Auto-expire after TTL
    const expireTime = ttl || this.defaultTTL;
    setTimeout(() => {
      this.cache.delete(key);
    }, expireTime);
  }

  /**
   * Execute request with deduplication
   * If same request is in flight, return the existing promise
   */
  async execute<T>(
    url: string,
    requestFn: () => Promise<T>,
    params?: Record<string, any>,
    options?: {
      cache?: boolean;
      ttl?: number;
      deduplicate?: boolean;
    }
  ): Promise<T> {
    const {
      cache: useCache = true,
      ttl,
      deduplicate = true,
    } = options || {};

    const key = this.generateKey(url, params);

    // Check cache first
    if (useCache) {
      const cached = this.get(url, params);
      if (cached !== null) {
        return cached as T;
      }
    }

    // Check for pending duplicate request
    if (deduplicate) {
      const pending = this.pending.get(key);
      if (pending && Date.now() - pending.timestamp < 10000) {
        // Pending request is less than 10 seconds old, reuse it
        return pending.promise as Promise<T>;
      }
    }

    // Execute new request
    const promise = requestFn()
      .then((data) => {
        // Cache successful response
        if (useCache) {
          this.set(url, data, params, ttl);
        }

        // Clean up pending
        this.pending.delete(key);

        return data;
      })
      .catch((error) => {
        // Clean up pending on error
        this.pending.delete(key);
        throw error;
      });

    // Store pending request
    if (deduplicate) {
      this.pending.set(key, {
        promise,
        timestamp: Date.now(),
      });
    }

    return promise;
  }

  /**
   * Clear all cache
   */
  clear(): void {
    this.cache.clear();
    this.pending.clear();
  }

  /**
   * Clear specific URL cache
   */
  clearUrl(url: string, params?: Record<string, any>): void {
    const key = this.generateKey(url, params);
    this.cache.delete(key);
    this.pending.delete(key);
  }

  /**
   * Clean up expired entries
   */
  cleanup(): void {
    const now = Date.now();

    // Clean expired cache entries
    for (const [key, entry] of this.cache.entries()) {
      if (now - entry.timestamp > this.defaultTTL) {
        this.cache.delete(key);
      }
    }

    // Clean old pending requests (> 10 seconds)
    for (const [key, pending] of this.pending.entries()) {
      if (now - pending.timestamp > 10000) {
        this.pending.delete(key);
      }
    }
  }
}

// Create singleton instances for different use cases
export const stockCache = new RequestCache(5 * 60 * 1000); // 5 minutes for stock data
export const financialCache = new RequestCache(10 * 60 * 1000); // 10 minutes for financial data
export const boardCache = new RequestCache(15 * 60 * 1000); // 15 minutes for board data

// Generic cache for other data
export const genericCache = new RequestCache();

// Periodic cleanup (every 5 minutes)
if (typeof window !== 'undefined') {
  setInterval(() => {
    stockCache.cleanup();
    financialCache.cleanup();
    boardCache.cleanup();
    genericCache.cleanup();
  }, 5 * 60 * 1000);
}

export default RequestCache;
