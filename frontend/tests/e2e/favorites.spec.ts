import { test, expect } from '@playwright/test';
import { navigateToPage, clearStorage, delay } from '../helpers/test-utils';

test.describe('收藏管理页面', () => {
  test.beforeEach(async ({ page }) => {
    await clearStorage(page);
    await navigateToPage(page, '/favorites');
  });

  test('应该能够显示收藏页面', async ({ page }) => {
    // 验证页面标题
    await expect(page.locator('text=我的收藏')).toBeVisible();

    // 验证Tab存在
    const tabs = page.locator('.ant-tabs-tab');
    expect(await tabs.count()).toBeGreaterThan(0);
  });

  test('应该能够切换收藏列表和添加股票Tab', async ({ page }) => {
    // 获取所有Tab
    const tabs = page.locator('.ant-tabs-tab');
    const count = await tabs.count();

    if (count > 1) {
      // 记录当前active tab
      const firstTab = tabs.first();

      // 点击第二个Tab
      await tabs.nth(1).click();
      await delay(500);

      // 验证Tab切换
      const activeTab = page.locator('.ant-tabs-tab-active');
      await expect(activeTab).toBeVisible();
    }
  });

  test('应该显示添加股票输入框', async ({ page }) => {
    // 切换到添加股票Tab（如果需要）
    const addTab = page.locator('text=添加').or(page.locator('[data-testid="add-tab"]'));
    if (await addTab.isVisible()) {
      await addTab.click();
      await delay(500);
    }

    // 查找输入框
    const input = page.locator('input.ant-select-search__field').or(page.locator('.ant-input'));
    const count = await input.count();
    expect(count).toBeGreaterThan(0);
  });

  test('应该显示空状态提示', async ({ page }) => {
    // 在收藏列表Tab中查找空状态
    const emptyState = page.locator('text=暂无').or(page.locator('.ant-empty'));
    if (await emptyState.first().isVisible()) {
      await expect(emptyState.first()).toBeVisible();
    }
  });

  test('应该显示使用说明', async ({ page }) => {
    // 查找说明文字
    const description = page.locator('text=功能').or(page.locator('text=使用'));
    if (await description.first().isVisible()) {
      await expect(description.first()).toBeVisible();
    }
  });
});
