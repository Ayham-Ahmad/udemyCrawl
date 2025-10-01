# quick_try.py
import asyncio
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

URL = "https://www.udemy.com/courses/development/web-development"

async def main():
    async with Stealth().use_async(async_playwright()) as p:
        browser = await p.chromium.launch(headless=False)   # visible = more human-like
        context = await browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/125.0.0.0 Safari/537.36"),
            locale="en-US"
        )
        page = await context.new_page()

        # quick check - should print False if stealth is applied
        try:
            print("navigator.webdriver =", await page.evaluate("() => navigator.webdriver"))
        except Exception as e:
            print("eval error:", e)

        try:
            await page.goto(URL, wait_until="domcontentloaded", timeout=60000)
        except Exception as e:
            print("goto warning:", e)

        try:
            await page.wait_for_selector("h3[data-purpose='course-title-url']", timeout=20000)
            print("Found course titles selector ✅")
        except Exception:
            print("Course selector not found (could be Cloudflare or different page)")

        # await page.screenshot(path="try.png")
        # print("Screenshot saved to try.png")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
