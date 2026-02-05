import { test, expect } from '@playwright/test';
import { navigateToPage, delay } from '../helpers/test-utils';

test.describe('AI智能筛选页面', () => {
  test.beforeEach(async ({ page }) => {
    await navigateToPage(page, '/ai-screen');
  });

  test('应该能够显示AI筛选页面', async ({ page }) => {
    // 验证页面标题
    await expect(page.locator('text=AI智能筛选')).toBeVisible();

    // 验证聊天输入框
    const textarea = page.locator('textarea.ant-input').or(page.locator('[data-testid="chat-input"]'));
    await expect(textarea).toBeVisible();

    // 验证发送按钮
    const sendBtn = page.locator('button:has-text("发送")');
    await expect(sendBtn).toBeVisible();
  });

  test('应该能够显示示例建议', async ({ page }) => {
    // 查找建议标签
    const suggestions = page.locator('.ant-tag');
    const count = await suggestions.count();

    // 应该有多个建议标签
    expect(count).toBeGreaterThan(0);
  });

  test('应该能够点击示例建议', async ({ page }) => {
    // 点击第一个建议标签
    const firstSuggestion = page.locator('.ant-tag').first();
    await firstSuggestion.click();

    // 等待输入框更新
    await delay(500);

    // 验证输入框有内容
    const textarea = page.locator('textarea.ant-input');
    const value = await textarea.inputValue();
    expect(value).toBeTruthy();
  });

  test('应该能够输入筛选条件', async ({ page }) => {
    // 输入筛选条件
    const textarea = page.locator('textarea.ant-input');
    await textarea.click();
    await textarea.fill('查找市盈率小于20的股票');

    // 验证输入成功
    const value = await textarea.inputValue();
    expect(value).toContain('市盈率');
  });

  test('应该能够清空输入', async ({ page }) => {
    // 输入内容
    const textarea = page.locator('textarea.ant-input');
    await textarea.click();
    await textarea.fill('测试内容');

    // 清空
    await textarea.clear();

    // 验证已清空
    const value = await textarea.inputValue();
    expect(value).toBe('');
  });

  test('应该显示欢迎信息', async ({ page }) => {
    // 查找欢迎信息或使用说明
    const welcomeText = page.locator('text=欢迎使用').or(page.locator('text=筛选'));
    await expect(welcomeText.first()).toBeVisible();
  });

  test('应该显示消息区域', async ({ page }) => {
    // 验证消息容器存在
    const messageContainer = page.locator('.ant-space-vertical').or(page.locator('[data-testid="messages-container"]'));
    await expect(messageContainer.first()).toBeVisible();
  });

  test('页面响应式布局正常', async ({ page }) => {
    // 测试不同视口大小
    await page.setViewportSize({ width: 1920, height: 1080 });
    await delay(500);

    await page.setViewportSize({ width: 768, height: 1024 });
    await delay(500);

    // 验证关键元素仍然可见
    await expect(page.locator('text=AI智能筛选')).toBeVisible();
  });
});
