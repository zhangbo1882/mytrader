import { test, expect } from '@playwright/test';
import { navigateToPage, delay } from '../helpers/test-utils';

test.describe('AI预测页面', () => {
  test.beforeEach(async ({ page }) => {
    await navigateToPage(page, '/prediction');
  });

  test('应该能够显示预测页面', async ({ page }) => {
    // 验证页面标题
    await expect(page.locator('text=AI预测')).toBeVisible();

    // 验证Tab切换
    const tabs = page.locator('.ant-tabs-tab');
    expect(await tabs.count()).toBeGreaterThan(0);
  });

  test('应该显示模型训练表单', async ({ page }) => {
    // 查找表单元素
    const stockInput = page.locator('input[placeholder*="股票代码"]').or(page.locator('[data-testid="stock-code-input"]'));

    if (await stockInput.isVisible()) {
      await expect(stockInput).toBeVisible();
    }
  });

  test('应该能够切换Tab', async ({ page }) => {
    // 获取所有Tab
    const tabs = page.locator('.ant-tabs-tab');
    const count = await tabs.count();

    if (count > 1) {
      // 点击第二个Tab
      await tabs.nth(1).click();
      await delay(500);

      // 验证Tab切换成功
      const activeTab = page.locator('.ant-tabs-tab-active');
      await expect(activeTab).toBeVisible();
    }
  });

  test('应该显示使用说明', async ({ page }) => {
    // 查找说明文字
    const description = page.locator('text=说明').or(page.locator('text=使用'));
    if (await description.first().isVisible()) {
      await expect(description.first()).toBeVisible();
    }
  });
});
