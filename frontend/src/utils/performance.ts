/**
 * Performance monitoring utilities
 */

/**
 * Measure function execution time
 */
export function measurePerformance<T extends (...args: any[]) => any>(
  fn: T,
  name: string
): T {
  return ((...args: any[]) => {
    const start = performance.now();
    const result = fn(...args);
    const end = performance.now();

    if (process.env.NODE_ENV === 'development') {
      console.log(`[Performance] ${name}: ${(end - start).toFixed(2)}ms`);
    }

    return result;
  }) as T;
}

/**
 * Measure async function execution time
 */
export async function measureAsyncPerformance<T>(
  fn: () => Promise<T>,
  name: string
): Promise<T> {
  const start = performance.now();
  try {
    const result = await fn();
    const end = performance.now();

    if (process.env.NODE_ENV === 'development') {
      console.log(`[Performance] ${name}: ${(end - start).toFixed(2)}ms`);
    }

    return result;
  } catch (error) {
    const end = performance.now();
    if (process.env.NODE_ENV === 'development') {
      console.error(`[Performance] ${name} failed after ${(end - start).toFixed(2)}ms:`, error);
    }
    throw error;
  }
}

/**
 * Get Web Vitals metrics (CLS, FID, LCP, FCP, TTFB)
 */
export function getWebVitals() {
  if (typeof window === 'undefined') return null;

  const navigation = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;

  if (!navigation) return null;

  return {
    // DNS lookup time
    dns: navigation.domainLookupEnd - navigation.domainLookupStart,
    // TCP connection time
    tcp: navigation.connectEnd - navigation.connectStart,
    // Request time
    request: navigation.responseStart - navigation.requestStart,
    // Response time
    response: navigation.responseEnd - navigation.responseStart,
    // DOM processing time
    domProcessing: navigation.domComplete - navigation.domInteractive,
    // Full page load time
    loadTime: navigation.loadEventEnd - navigation.fetchStart,
    // Time to First Byte (TTFB)
    ttfb: navigation.responseStart - navigation.fetchStart,
    // DOM Content Loaded
    dcl: navigation.domContentLoadedEventEnd - navigation.fetchStart,
  };
}

/**
 * Log resource loading performance
 */
export function getResourceTiming() {
  if (typeof window === 'undefined') return [];

  const resources = performance.getEntriesByType('resource') as PerformanceResourceTiming[];

  return resources.map((resource) => ({
    name: resource.name,
    duration: resource.duration,
    size: resource.transferSize,
    type: resource.initiatorType,
  }));
}

/**
 * Monitor long tasks (> 50ms)
 */
export function observeLongTasks(callback: (tasks: any[]) => void) {
  if (typeof window === 'undefined' || !('PerformanceObserver' in window)) {
    return null;
  }

  try {
    const observer = new PerformanceObserver((list) => {
      const entries = list.getEntries();
      callback(entries);
    });

    observer.observe({ entryTypes: ['longtask'] });

    return observer;
  } catch (e) {
    console.warn('Long task observation not supported');
    return null;
  }
}

/**
 * Check if device is low-end (for reduced animations, etc.)
 */
export function isLowEndDevice(): boolean {
  if (typeof window === 'undefined') return false;

  // Check hardware concurrency
  const cores = navigator.hardwareConcurrency || 2;
  if (cores <= 2) return true;

  // Check memory (if available)
  const memory = (navigator as any).deviceMemory;
  if (memory && memory <= 2) return true;

  return false;
}

/**
 * Get network connection type
 */
export function getNetworkType(): string {
  if (typeof window === 'undefined') return 'unknown';

  const connection = (navigator as any).connection || (navigator as any).mozConnection || (navigator as any).webkitConnection;

  if (!connection) return 'unknown';

  return connection.effectiveType || 'unknown';
}

/**
 * Debounce function with requestAnimationFrame for smooth animations
 */
export function rafDebounce<T extends (...args: any[]) => void>(fn: T): T {
  let rafId: number | null = null;

  return ((...args: any[]) => {
    if (rafId !== null) {
      cancelAnimationFrame(rafId);
    }

    rafId = requestAnimationFrame(() => {
      fn(...args);
      rafId = null;
    });
  }) as T;
}
