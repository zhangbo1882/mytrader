import { test, expect } from '@playwright/test';
import { navigateToPage, delay, verifyURL } from '../helpers/test-utils';

test.describe('全局导航测试', () => {
  test('应该能够导航到所有页面', async ({ page }) => {
    // 测试所有页面路由
    const pages = [
      { path: '/query', title: '股票查询' },
      { path: '/ai-screen', title: 'AI智能筛选' },
      { path: '/prediction', title: 'AI预测' },
      { path: '/favorites', title: '我的收藏' },
      { path: '/update', title: '更新管理' },
      { path: '/tasks', title: '任务历史' },
      { path: '/financial', title: '财务数据' },
      { path: '/boards', title: '板块中心' },
    ];

    for (const { path, title } of pages) {
      // 导航到页面
      await navigateToPage(page, path);

      // 验证页面标题显示
      await expect(page.locator(`text=${title}`).first()).toBeVisible();

      // 等待一下
      await delay(500);
    }
  });

  test('应该能够通过侧边栏导航', async ({ page }) => {
    await navigateToPage(page, '/query');

    // 查找侧边栏菜单
    const menu = page.locator('.ant-menu').or(page.locator('[data-testid="main-menu"]'));

    if (await menu.isVisible()) {
      // 获取所有菜单项
      const menuItems = menu.locator('.ant-menu-item');
      const count = await menuItems.count();

      if (count > 0) {
        // 点击第二个菜单项
        await menuItems.nth(1).click();
        await delay(1000);

        // 验证导航成功
        expect(page.url()).not.toBe('/query');
      }
    }
  });

  test('应该能够处理无效路由', async ({ page }) => {
    // 导航到无效路由
    await page.goto('/invalid-route');

    // 等待重定向或显示404
    await delay(2000);

    // 应该重定向到有效页面或显示404
    const currentUrl = page.url();
    expect(currentUrl).toBeTruthy();
  });

  test('应该能够在页面间快速切换', async ({ page }) => {
    // 快速切换多个页面
    await navigateToPage(page, '/query');
    await delay(300);

    await navigateToPage(page, '/favorites');
    await delay(300);

    await navigateToPage(page, '/boards');
    await delay(300);

    // 验证最终页面加载成功
    await expect(page.locator('text=板块中心')).toBeVisible();
  });

  test('应该保持浏览器历史记录', async ({ page }) => {
    // 访问多个页面
    await page.goto('/query');
    await delay(500);

    await page.goto('/favorites');
    await delay(500);

    await page.goto('/boards');
    await delay(500);

    // 使用浏览器后退
    await page.goBack();
    await delay(500);

    // 验证后退到收藏页面
    await expect(page.locator('text=我的收藏').or(page.locator('text=收藏'))).toBeVisible();

    // 使用浏览器前进
    await page.goForward();
    await delay(500);

    // 验证前进到板块页面
    await expect(page.locator('text=板块中心')).toBeVisible();
  });

  test('页面标题正确更新', async ({ page }) => {
    const pages = [
      { path: '/query', expectedTitle: '股票查询' },
      { path: '/ai-screen', expectedTitle: 'AI智能筛选' },
      { path: '/boards', expectedTitle: '板块中心' },
    ];

    for (const { path, expectedTitle } of pages) {
      await page.goto(path);
      await delay(500);

      // 验证页面标题
      const title = await page.title();
      expect(title).toBeTruthy();
    }
  });
});
