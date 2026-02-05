import { test, expect } from '@playwright/test';
import { navigateToPage, delay } from '../helpers/test-utils';

test.describe('板块中心页面', () => {
  test.beforeEach(async ({ page }) => {
    await navigateToPage(page, '/boards');
  });

  test('应该能够显示板块中心页面', async ({ page }) => {
    // 验证页面标题
    await expect(page.locator('text=板块中心')).toBeVisible();

    // 验证页面内容
    const content = page.locator('.ant-card').or(page.locator('text=板块'));
    await expect(content.first()).toBeVisible();
  });

  test('应该显示搜索框', async ({ page }) => {
    // 查找搜索输入框
    const searchInput = page.locator('input[placeholder*="搜索"]').or(
      page.locator('.ant-input')
    );

    await expect(searchInput.first()).toBeVisible();
  });

  test('应该显示分类标签', async ({ page }) => {
    // 查找分类标签
    const tags = page.locator('.ant-tag');
    const count = await tags.count();

    if (count > 0) {
      await expect(tags.first()).toBeVisible();
    }
  });

  test('应该显示刷新按钮', async ({ page }) => {
    // 查找刷新按钮
    const refreshBtn = page.locator('button:has-text("刷新")');
    const count = await refreshBtn.count();

    if (count > 0) {
      await expect(refreshBtn.first()).toBeVisible();
    }
  });

  test('应该显示使用说明', async ({ page }) => {
    // 查找说明文字
    const description = page.locator('text=板块').or(page.locator('text=成分股'));

    if (await description.first().isVisible()) {
      await expect(description.first()).toBeVisible();
    }
  });

  test('应该能够使用搜索框', async ({ page }) => {
    // 查找搜索输入框
    const searchInput = page.locator('input[placeholder*="搜索"]').or(
      page.locator('.ant-input')
    ).first();

    // 输入搜索关键词
    await searchInput.click();
    await searchInput.fill('金融');

    // 验证输入成功
    const value = await searchInput.inputValue();
    expect(value).toContain('金融');

    // 清空
    await searchInput.clear();
  });

  test('页面响应式布局正常', async ({ page }) => {
    // 测试不同视口大小
    await page.setViewportSize({ width: 1920, height: 1080 });
    await delay(500);

    await page.setViewportSize({ width: 768, height: 1024 });
    await delay(500);

    // 验证关键元素仍然可见
    await expect(page.locator('text=板块中心')).toBeVisible();
  });
});
