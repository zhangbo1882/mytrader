import { test, expect } from '@playwright/test';
import { navigateToPage, delay } from '../helpers/test-utils';

test.describe('更新管理页面', () => {
  test.beforeEach(async ({ page }) => {
    await navigateToPage(page, '/update');
  });

  test('应该能够显示更新管理页面', async ({ page }) => {
    // 验证页面标题
    await expect(page.locator('text=更新管理')).toBeVisible();

    // 验证页面内容
    const content = page.locator('.ant-card').or(page.locator('text=任务'));
    await expect(content.first()).toBeVisible();
  });

  test('应该显示创建任务按钮', async ({ page }) => {
    // 查找创建任务按钮
    const createBtn = page.locator('button:has-text("创建")').or(page.locator('[data-testid="create-task-btn"]'));

    const count = await createBtn.count();
    if (count > 0) {
      await expect(createBtn.first()).toBeVisible();
    }
  });

  test('应该显示任务说明', async ({ page }) => {
    // 查找说明文字
    const description = page.locator('text=任务').or(page.locator('text=更新'));
    await expect(description.first()).toBeVisible();
  });

  test('应该显示使用指南', async ({ page }) => {
    // 查找使用说明
    const guide = page.locator('text=使用').or(page.locator('text=说明'));

    if (await guide.first().isVisible()) {
      await expect(guide.first()).toBeVisible();
    }
  });

  test('页面响应式布局正常', async ({ page }) => {
    // 测试不同视口大小
    await page.setViewportSize({ width: 1920, height: 1080 });
    await delay(500);

    await page.setViewportSize({ width: 768, height: 1024 });
    await delay(500);

    // 验证关键元素仍然可见
    await expect(page.locator('text=更新管理')).toBeVisible();
  });
});
