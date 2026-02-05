/**
 * E2E测试工具函数
 */

import { Page, expect } from '@playwright/test';

/**
 * 等待页面加载完成
 */
export async function waitForPageLoad(page: Page, timeout = 30000) {
  await page.waitForLoadState('networkidle', { timeout });
}

/**
 * 等待元素可见
 */
export async function waitForElementVisible(
  page: Page,
  selector: string,
  timeout = 10000
) {
  await page.waitForSelector(selector, { state: 'visible', timeout });
}

/**
 * 等待加载完成
 */
export async function waitForLoading(page: Page) {
  try {
    await page.waitForSelector('.ant-spin', { state: 'hidden', timeout: 30000 });
  } catch (e) {
    // 如果没有loading，继续执行
  }
}

/**
 * 截图并保存
 */
export async function takeScreenshot(
  page: Page,
  name: string,
  fullPage = false
) {
  await page.screenshot({
    path: `test-results/screenshots/${name}.png`,
    fullPage,
  });
}

/**
 * 导航到页面并等待加载
 */
export async function navigateToPage(
  page: Page,
  path: string,
  options?: { waitForLoad?: boolean }
) {
  await page.goto(path);
  if (options?.waitForLoad !== false) {
    await waitForPageLoad(page);
  }
}

/**
 * 清空localStorage
 */
export async function clearStorage(page: Page) {
  await page.evaluate(() => {
    localStorage.clear();
    sessionStorage.clear();
  });
}

/**
 * 验证URL包含路径
 */
export async function verifyURL(
  page: Page,
  expectedPath: string,
  timeout = 5000
) {
  await page.waitForURL(
    (url) => {
      return url.pathname.includes(expectedPath);
    },
    { timeout }
  );
}

/**
 * 获取表格行数
 */
export async function getTableRowCount(
  page: Page,
  tableSelector: string
): Promise<number> {
  return await page.locator(`${tableSelector} tbody tr`).count();
}

/**
 * 等待表格有数据
 */
export async function waitForTableData(
  page: Page,
  tableSelector: string,
  minRows = 1,
  timeout = 10000
) {
  await page.waitForFunction(
    ({ selector, min }) => {
      const rows = document.querySelectorAll(`${selector} tbody tr`);
      return rows.length >= min;
    },
    { selector: tableSelector, min: minRows },
    { timeout }
  );
}

/**
 * 模拟API延迟
 */
export async function delay(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
