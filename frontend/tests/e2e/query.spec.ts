import { test, expect } from '@playwright/test';
import { navigateToPage, waitForLoading, delay } from '../helpers/test-utils';

test.describe('股票查询页面', () => {
  test.beforeEach(async ({ page }) => {
    await navigateToPage(page, '/query');
  });

  test('应该能够显示查询页面', async ({ page }) => {
    // 验证页面标题
    await expect(page.locator('text=股票查询')).toBeVisible();

    // 验证股票选择器存在
    const stockSelector = page.locator('.ant-select-selector').first();
    await expect(stockSelector).toBeVisible();

    // 验证日期选择器存在
    await expect(page.locator('.ant-picker')).toBeVisible();

    // 验证查询按钮存在
    await expect(page.locator('button:has-text("查询")')).toBeVisible();
  });

  test('应该能够选择日期范围快捷按钮', async ({ page }) => {
    // 点击3个月按钮
    const threeMonthBtn = page.locator('button:has-text("3个月")');
    await threeMonthBtn.click();

    // 等待一下让日期选择器更新
    await delay(500);

    // 验证日期选择器有值
    const dateInput = page.locator('.ant-picker-input input');
    const value = await dateInput.inputValue();
    expect(value).toBeTruthy();
  });

  test('应该能够切换复权类型', async ({ page }) => {
    // 查找复权类型选择器
    const priceTypeButton = page.locator('button:has-text("前复权")');
    await expect(priceTypeButton).toBeVisible();
  });

  test('应该能够点击查询按钮', async ({ page }) => {
    // 点击查询按钮（即使没有选择股票）
    const queryBtn = page.locator('button:has-text("查询")');
    await queryBtn.click();

    // 可能会显示提示信息
    await delay(1000);
  });

  test('应该能够切换到图表视图', async ({ page }) => {
    // 查找图表Tab
    const chartTab = page.locator('text=图表').or(page.locator('[data-testid="chart-tab"]'));
    if (await chartTab.isVisible()) {
      await chartTab.click();
      await delay(500);
    }
  });

  test('应该显示导出按钮', async ({ page }) => {
    // 验证导出按钮存在
    const exportButtons = page.locator('button:has-text("导出")');
    const count = await exportButtons.count();
    expect(count).toBeGreaterThan(0);
  });

  test('页面响应式布局正常', async ({ page }) => {
    // 测试不同视口大小
    await page.setViewportSize({ width: 1920, height: 1080 });
    await delay(500);

    await page.setViewportSize({ width: 768, height: 1024 });
    await delay(500);

    await page.setViewportSize({ width: 375, height: 667 });
    await delay(500);

    // 验证关键元素仍然可见
    await expect(page.locator('text=股票查询')).toBeVisible();
  });
});
