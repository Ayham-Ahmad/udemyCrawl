import asyncio
import json
import logging
import re
from pathlib import Path

from pyquery import PyQuery as pq

import random
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
INPUT_DIR = Path("Tutorials_ALL_URLs")
OUTPUT_DIR = Path("Tutorials_ALL")
OUTPUT_DIR.mkdir(exist_ok=True)
DONE_FILE = Path("done_crawling.json")

logging.basicConfig(
    filename="crawl_log.txt",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def load_done_crawling():
    if DONE_FILE.exists():
        with open(DONE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

def save_done_crawling_entry(url, main_category, sub_category):
    """Immediately save a single URL entry to done_crawling.json"""
    entry = {"url": url, "main_category": main_category, "sub_category": sub_category}

    done_list = load_done_crawling()
    done_list.append(entry)

    with open(DONE_FILE, "w", encoding="utf-8") as f:
        json.dump(done_list, f, indent=2, ensure_ascii=False)

def is_already_crawled(url, main_category, sub_category, done_list):
    for entry in done_list:
        if entry["url"] == url:
            if entry["main_category"] == main_category and entry["sub_category"] == sub_category:
                return "same"
            else:
                return "different"
    return False

def load_all_course_urls(root_dir: str = "Tutorials_ALL_URLs"):
    """
    Traverse all JSON files in root_dir, load courses, and build a nested dict:
    {
        main_category: [
            {sub_category: [url1, url2, ...]},
            ...
        ],
        ...
    }
    """
    root = Path(root_dir)
    if not root.exists():
        raise FileNotFoundError(f"Directory {root_dir} not found")

    result = {}

    for file in root.rglob("*.json"):
        try:
            with open(file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue  # skip unreadable files

        main = data.get("main_category")
        sub = data.get("sub_category")
        courses = data.get("courses", [])

        if not main or not sub or not isinstance(courses, list):
            continue

        urls = [c.get("url") for c in courses if isinstance(c, dict) and c.get("url")]

        if not urls:
            continue

        if main not in result:
            result[main] = []

        result[main].append({sub: urls})

    return result

def sanitize_filename(name: str) -> str:
    """Make a filesystem-safe lower_snake filename for the course."""
    n = name.strip().lower()
    n = re.sub(r"[^\w\s-]", "", n)   # remove punctuation
    n = re.sub(r"\s+", "_", n)       # spaces -> underscore
    n = n.strip("_")
    if not n:
        n = "course"
    return n

def chunked(iterable, size):
    """Yield successive chunks of given size from iterable."""
    for i in range(0, len(iterable), size):
        yield iterable[i:i+size]

# ---------- CRAWLING LOGIC ----------
async def crawl_course(page, main_category, sub_category, cou_urls: list[str], file_index: int, done_list: list):
    safe_sub = sub_category.replace(" ", "_").replace("/", "-")
    safe_main = main_category.replace(" ", "_")

    category_dir = OUTPUT_DIR / main_category / sub_category
    category_dir.mkdir(parents=True, exist_ok=True)

    out_file = category_dir / f"{safe_main}_{safe_sub}_{file_index}.json"
    print(f"➡️ Crawling Courses for: {main_category} -> {sub_category}")

    for url in cou_urls[:5]:  # loop over each course
        try:
            status = is_already_crawled(url, main_category, sub_category, done_list)
            if status == "same":
                print(f"✅ Already crawled in this category: {url}")
                continue
            elif status == "different":
                print(f"⚠️ URL already crawled in another category. Logging and skipping: {url}")
                save_done_crawling_entry(url, main_category, sub_category)
                done_list.append({"url": url, "main_category": main_category, "sub_category": sub_category})
                continue

            # Crawl the page
            course = {}
            print(f"🌍 Visiting (stealth): {url}")
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(random.uniform(2, 5))

            html = await page.content()
            tut_doc = pq(html)

            # Title
            title_el = tut_doc("h1.ud-heading-xxl.clp-lead__title.clp-lead__title--small")
            if title_el:
                course["title"] = title_el.text().strip()

            # Bio
            bio_el = tut_doc("div.ud-text-lg.clp-lead__headline")
            if bio_el:
                course["bio"] = bio_el.text().strip()

            # Objectives
            objectives = [pq(obj).text().strip()
                          for obj in tut_doc("span.what-you-will-learn--objective-item--VZFww")]
            if objectives:
                course["objectives"] = objectives

            # Course content
            course_content = {}
            for section_el in tut_doc("div.accordion-panel-module--panel--Eb0it.section--panel--qYPjj"):
                section = pq(section_el)
                section_title = section("span.section--section-title--svpHP").text().strip()
                section_sub_titles = [pq(sub).text().strip()
                                      for sub in section(".section--course-lecture-title--lH1Wi").items()]
                if section_title:
                    course_content[section_title] = section_sub_titles
            if course_content:
                course["course_content"] = course_content

            # Crawl requirements
            requirements = []
            req_title = tut_doc("h2[data-purpose='requirements-title']")
            if req_title:
                req_block = req_title.parent()
                for requirement_el in req_block.find("ul li .ud-block-list-item-content"):
                    requirement = requirement_el.text_content().strip()
                    requirements.append(requirement)
            if requirements:
                course["requirements"] = requirements

            # Crawl description
            desc_el = tut_doc("div[data-purpose='course-description'] div[data-purpose='safely-set-inner-html:description:description']")
            if desc_el:
                description = desc_el.text().strip()
                if description:
                    course["description"] = description

            # Crawl target audience
            audience = []
            for li in tut_doc("div[data-purpose='course-description'] div[data-purpose='target-audience'] ul li"):
                audience.append(li.text_content().strip())
            if audience:
                course["target_audience"] = audience

            # Save course immediately
            if out_file.exists():
                with open(out_file, "r", encoding="utf-8") as f:
                    existing_data = json.load(f)
                existing_courses = existing_data.get("courses", [])
                existing_urls = {c.get("url") for c in existing_courses}
                if url not in existing_urls:
                    existing_courses.append({**course, "url": url})
                all_courses = existing_courses
            else:
                all_courses = [{**course, "url": url}]

            data = {
                "main_category": main_category,
                "sub_category": sub_category,
                "courses": all_courses
            }

            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            print(f"💾 Saved course → {out_file}")

            # Mark URL as done immediately
            save_done_crawling_entry(url, main_category, sub_category)
            done_list.append({"url": url, "main_category": main_category, "sub_category": sub_category})

        except Exception as e:
            print(f"❌ Error crawling {url}: {e}")
            # Mark as done even on error to prevent infinite retry
            save_done_crawling_entry(url, main_category, sub_category)
            done_list.append({"url": url, "main_category": main_category, "sub_category": sub_category})

# ------------------ PARALLEL RUNNER ------------------ #
async def crawl_multiple(targets, concurrency_limit=3):
    async with Stealth().use_async(async_playwright()) as p:
        browser = await p.chromium.launch(headless=False)

        sem = asyncio.Semaphore(concurrency_limit)

        async def sem_task(main, sub, url):
            async with sem:
                delay = random.uniform(1, 5)
                print(f"⏳ Delaying {delay:.1f}s before starting {sub}")
                await asyncio.sleep(delay)
                return await crawl_course(browser, main, sub, url)

        tasks = [sem_task(m, s, u) for m, s, u in targets]
        await asyncio.gather(*tasks)

        await browser.close()

async def main():

    all_urls = load_all_course_urls(INPUT_DIR)
    done_list = load_done_crawling()

    # Print one category
    # for main_cat, subs in all_urls.items():
    #     print(main_cat)
    #     for sub in subs:
    #         for sub_cat, urls in sub.items():
    #             print("  ", sub_cat, "->", len(urls), "urls")

    async with Stealth().use_async(async_playwright()) as p:
        browser = await p.chromium.launch(headless=False, args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage"
        ])
        context = await browser.new_context(user_agent=random.choice(USER_AGENTS))
        page = await context.new_page()

        prev_sub_cat = ''
        file_index = 1
        for i, (main_cat, subs) in enumerate(all_urls.items(), start=1):
            # print(f"{i} {main_cat} in Progress...")
            for sub in subs[:1]:
                for sub_cat, urls in sub.items():
                    if sub_cat == prev_sub_cat:
                        file_index += 1
                    else:
                        prev_sub_cat = sub_cat
                        file_index = 1
                    targets = {sub_cat: urls}
                    if i == 1:
                    #     print(targets)
                        for urls in targets.values():
                            await crawl_course(page, main_category=main_cat, sub_category=sub_cat, cou_urls=urls, file_index=file_index, done_list=done_list)


    # for batch in chunked(targets, 3):
    #     print(f"🚀 Starting batch with {len(batch)} targets...")
    #     await crawl_multiple(batch, concurrency_limit=3)  # <= run 3 in parallel
    #     print("✅ Batch finished")


if __name__ == "__main__":
    asyncio.run(main())