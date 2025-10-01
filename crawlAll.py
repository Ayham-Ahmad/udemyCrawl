import asyncio
import json
import logging
import re
from pathlib import Path
from urllib.parse import urljoin

from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
from crawl4ai.content_scraping_strategy import LXMLWebScrapingStrategy
from pyquery import PyQuery as pq

import logging
import random
from urllib.parse import urljoin
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
from crawl4ai.content_scraping_strategy import LXMLWebScrapingStrategy
from pyquery import PyQuery as pq
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

# Rotating realistic User-Agents
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

# ---------- CONFIG ----------
ROOT_INDEX_URL = "https://www.udemy.com/courses/"
OUTPUT_DIR = Path("Tutorials_ALL")
OUTPUT_DIR.mkdir(exist_ok=True)

TUTORIALS_INDEX_URL = ['development', 'business', 'finance-and-accounting', 'it-and-software', 'office-productivity',
                       'personal-development', 'design', 'marketing', 'lifestyle', 'photography-and-video',
                       'health-and-fitness']

logging.basicConfig(
    filename="crawl_log.txt",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def sanitize_filename(name: str) -> str:
    """Make a filesystem-safe lower_snake filename for the course."""
    n = name.strip().lower()
    n = re.sub(r"[^\w\s-]", "", n)   # remove punctuation
    n = re.sub(r"\s+", "_", n)       # spaces -> underscore
    n = n.strip("_")
    if not n:
        n = "course"
    return n

# ---------- CRAWLING LOGIC ----------
async def crawl_course(main_category, sub_category, tut_url: str):
    safe_sub = sub_category.replace(" ", "_").replace("/", "-")
    safe_main = main_category.replace(" ", "_")

    out_file = OUTPUT_DIR / f"{safe_main}_{safe_sub}"

    print(f"➡️  Crawling Category (stealth mode): {sub_category} -> {tut_url}")

    try:
        async with Stealth().use_async(async_playwright()) as p:
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context(
                user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/125.0.0.0 Safari/537.36"),
                locale="en-US"
            )
            page = await context.new_page()

            # navigator.webdriver check
            print("navigator.webdriver =", await page.evaluate("() => navigator.webdriver"))

            await page.goto(tut_url, wait_until="domcontentloaded")
            await page.wait_for_selector("h3[data-purpose='course-title-url'] a", timeout=30000)


            # pull HTML and pass it through pyquery
            html = await page.content()
            tut_doc = pq(html)

            # collect courses
            courses = set()
            file_index = 1
            no_more_page = False

            while True:
                # pull HTML and parse courses on this page
                html = await page.content()
                tut_doc = pq(html)

                for course in tut_doc("h3[data-purpose='course-title-url'] a"):
                    course_el = pq(course)
                    title = course_el.clone().children("div.ud-sr-only").remove().end().text().strip()
                    href = course_el.attr("href")
                    full_url = urljoin("https://www.udemy.com", href) if href else "N/A"

                    # add to set (title, url) tuple → ensures uniqueness
                    courses.add((title, full_url))

                # try to go to the next page
                try:
                    next_btn = await page.query_selector("a.pagination_next__aBqfT[aria-disabled='false']")
                    if not next_btn:
                        print("No more pages 🚀")
                        no_more_page = True

                    # add random delay before clicking (2–6 seconds for example)
                    delay = random.uniform(2, 6)
                    print(f"⏳ Waiting {delay:.2f}s before next page...")
                    await asyncio.sleep(delay)

                    await next_btn.click()
                    await page.wait_for_timeout(2000)  # wait a bit for next page to load
                    await page.wait_for_selector("h3[data-purpose='course-title-url'] a", timeout=20000)

                except Exception:
                    print("Pagination ended 🚀")
                    no_more_page = True

                # if batch threshold reached
                if len(courses) >= 500 or no_more_page:
                    data = {
                        "main_category": main_category,
                        "sub_category": sub_category,
                        "courses": [{"title": t, "url": u} for t, u in courses]
                    }

                    out_file_saved_url_title = f"{out_file}_{file_index}.json"
                    with open(out_file_saved_url_title, "w", encoding="utf-8") as f:
                        json.dump(data, f, indent=2, ensure_ascii=False)

                    print(f"💾 Saved {len(courses)} unique courses → {out_file_saved_url_title}")

                    if no_more_page:
                        break

                    file_index += 1
                    courses.clear() 

            # Debug print
            # for i, c in enumerate(courses, start=1):
            #     print(f"{i}. {c['title']} -> {c['url']}")

            ### debugging ###
            # with open(f"debug_{Category_name}.html", "w", encoding="utf-8") as f:
            #     f.write(html)

            # matches = tut_doc("h3[data-purpose='course-title-url'] a")

            # print("Matches found:", len(matches))
            # for i, match in enumerate(matches.items(), start=1):
            #     title = match.text().strip()
            #     href = match.attr("href")
            #     full_url = urljoin("https://www.udemy.com", href) if href else "N/A"
            #     print(f"{i}. {title} -> {full_url}")


            await browser.close()

    except Exception as e:
        print(f"❌ Failed to fetch course root {tut_url}: {e}")
        return
    

async def main():
    run_config = CrawlerRunConfig(scraping_strategy=LXMLWebScrapingStrategy(), verbose=True)

    async with AsyncWebCrawler(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/125.0.0.0 Safari/537.36",
        browser_args=["--disable-blink-features=AutomationControlled", "--no-sandbox", "--disable-dev-shm-usage"],
        wait_for="div[data-testid='popover-render-content']",
        wait_for_selector="div[data-testid='popover-render-content']",
        wait_until="networkidle",
    ) as crawler:
        
        # Looping on each main category in TUTORIALS_INDEX_URL
        for INDEX_URL in TUTORIALS_INDEX_URL[:1]:
            full_url = ROOT_INDEX_URL + INDEX_URL
            print(f"Main Category: {INDEX_URL}, URL: {full_url}")

            try:
                idx_results = await crawler.arun(url=full_url, config=run_config)
            except Exception as e:
                logging.error(f"Failed to fetch index {full_url}: {e}")
                print(f"Failed to fetch index {full_url}: {e}")
                return

            index_html = None
            for r in idx_results:
                if getattr(r, "html", None):
                    index_html = r.html
                    # print(f"index_html: {r.html[:50]}")
                    break
            if not index_html:
                logging.error("No HTML for tutorials index page")
                print("No HTML for tutorials index page")
                return

            index_doc = pq(index_html)
            # print(f"index_doc: {index_doc[:100]}")

            # Storing each Category in the Main Category
            course_categories = {}
            for a in index_doc("nav.subcategory-link-bar_subcategory-link-bar__hRQCP ul.ud-unstyled-list.subcategory-link-bar_nav-list__JD9R8 li a.ud-btn.ud-btn-medium.ud-btn-ghost.ud-btn-text-sm.link-bar_nav-button__CGUuC").items():
                href = a.attr("href") # Category link
                course_category = a.text().strip() # Category name
                # print(f"href: {href}")
                # print(f"Course Category: {course_category}")
                if not href or not course_category:
                    continue
                full = urljoin(full_url, href)
                if course_category not in course_categories:
                    course_categories[course_category] = full # Store it in a dict, key: category name -> value: Category link

            print(f"Course Categories Num: {len(course_categories)}") 

            # Saving main category name, for json naming later
            main_category = next(iter(course_categories))
            # print(f"Main Category Title: {Main_Category}")

            # Taking out the first Category because it is the main one
            it = iter(course_categories.items())
            next(it)
            course_categories = dict(it)
            # print(f"Course Categories: {course_categories}")


            for (sub_category, url), _ in zip(course_categories.items(), range(2)):
                file_name = sanitize_filename(main_category + '_' + sub_category) + ".json"
                # print(main_Category, sub_category)
                if (OUTPUT_DIR / file_name).exists():
                    print(f"⏭️ Skipping {sub_category} (file exists: {file_name})")
                    continue

                # print(file_name)

                await crawl_course(main_category, sub_category, url)

            print("*" * 50)


if __name__ == "__main__":
    asyncio.run(main())