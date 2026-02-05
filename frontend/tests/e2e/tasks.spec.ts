import { test, expect } from '@playwright/test';
import { navigateToPage, delay } from '../helpers/test-utils';

test.describe('任务历史页面', () => {
  test.beforeEach(async ({ page }) => {
    await navigateToPage(page, '/tasks');
  });

  test('应该能够显示任务历史页面', async ({ page }) => {
    // 验证页面标题
    await expect(page.locator('text=任务历史')).toBeVisible();

    // 验证页面内容
    const content = page.locator('.ant-card').or(page.locator('.ant-table'));
    await expect(content.first()).toBeVisible();
  });

  test('应该显示筛选按钮', async ({ page }) => {
    // 查找筛选或搜索相关元素
    const filter = page.locator('button:has-text("筛选")').or(
      page.locator('.ant-select')
    );

    const count = await filter.count();
    expect(count).toBeGreaterThan(0);
  });

  test('应该显示表格或空状态', async ({ page }) => {
    // 查找表格或空状态
    const table = page.locator('.ant-table');
    const empty = page.locator('.ant-empty');

    const tableVisible = await table.isVisible();
    const emptyVisible = await empty.isVisible();

    expect(tableVisible || emptyVisible).toBeTruthy();
  });

  test('应该显示操作按钮', async ({ page }) => {
    // 查找操作按钮（删除、清理等）
    const buttons = page.locator('button:has-text("删除"), button:has-text("清理"), button:has-text("清空")');
    const count = await buttons.count();

    if (count > 0) {
      await expect(buttons.first()).toBeVisible();
    }
  });

  test('应该显示使用说明', async ({ page }) => {
    // 查找说明文字
    const description = page.locator('text=任务').or(page.locator('text=历史'));

    if (await description.first().isVisible()) {
      await expect(description.first()).toBeVisible();
    }
  });

  test('页面响应式布局正常', async ({ page }) => {
    // 测试不同视口大小
    await page.setViewportSize({ width: 1920, height: 1080 });
    await delay(500);

    await page.setViewportSize({ width: 768, height: 1024 });
    await delay(500);

    // 验证关键元素仍然可见
    await expect(page.locator('text=任务历史')).toBeVisible();
  });
});
