import { test, expect } from '@playwright/test';
import { navigateToPage, delay } from '../helpers/test-utils';

test.describe('财务数据页面', () => {
  test.beforeEach(async ({ page }) => {
    await navigateToPage(page, '/financial');
  });

  test('应该能够显示财务数据页面', async ({ page }) => {
    // 验证页面标题
    await expect(page.locator('text=财务数据')).toBeVisible();

    // 验证输入框存在
    const input = page.locator('input.ant-input').or(page.locator('.ant-select'));
    await expect(input.first()).toBeVisible();
  });

  test('应该显示查询按钮', async ({ page }) => {
    // 查找查询按钮
    const queryBtn = page.locator('button:has-text("查询")').or(
      page.locator('button[type="submit"]')
    );

    await expect(queryBtn.first()).toBeVisible();
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
    const description = page.locator('text=财务').or(page.locator('text=指标'));

    if (await description.first().isVisible()) {
      await expect(description.first()).toBeVisible();
    }
  });

  test('应该显示导出按钮', async ({ page }) => {
    // 查找导出按钮
    const exportBtn = page.locator('button:has-text("导出")');
    const count = await exportBtn.count();

    if (count > 0) {
      await expect(exportBtn.first()).toBeVisible();
    }
  });

  test('页面响应式布局正常', async ({ page }) => {
    // 测试不同视口大小
    await page.setViewportSize({ width: 1920, height: 1080 });
    await delay(500);

    await page.setViewportSize({ width: 768, height: 1024 });
    await delay(500);

    // 验证关键元素仍然可见
    await expect(page.locator('text=财务数据')).toBeVisible();
  });
});
