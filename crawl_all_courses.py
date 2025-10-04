import asyncio
import json
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
INPUT_DIR = Path("Tutorials_ALL_URLs")
OUTPUT_DIR = Path("Tutorials_ALL")
OUTPUT_DIR.mkdir(exist_ok=True)
DONE_FILE = Path("done_crawling.json")

BATCH_SIZE = 5          # change to 10 (or any number) if you want bigger batches
CONCURRENCY_LIMIT = 5   # how many browsers/pages open at once

class Page:
    def __init__(self, html: str):
        self._cou_doc = pq(html)

    def title(self) -> str | None:
        title_el = self._cou_doc("h1.ud-heading-xxl.clp-lead__title.clp-lead__title--small")
        if title_el and title_el.text().strip():
            return title_el.text().strip()
        return None

    def bio(self) -> str | None:
        bio_el = self._cou_doc("div.ud-text-lg.clp-lead__headline")
        if bio_el and bio_el.text().strip():
            return bio_el.text().strip()
        return None

    def objectives(self) -> list[str]:
        objs = [pq(obj).text().strip()
                for obj in self._cou_doc("span.what-you-will-learn--objective-item--VZFww").items()]
        if objs:
            return objs
        return []

    def course_content(self) -> dict[str, list[str]]:
        content = {}
        for section_el in self._cou_doc("div.accordion-panel-module--panel--Eb0it.section--panel--qYPjj"):
            section = pq(section_el)
            section_title = section("span.section--section-title--svpHP").text().strip()
            section_sub_titles = [pq(sub).text().strip()
                                  for sub in section(".section--course-lecture-title--lH1Wi").items()]
            if section_title and section_sub_titles:
                content[section_title] = section_sub_titles
            elif section_title:
                content[section_title] = []
        return content

    def requirements(self) -> list[str]:
        reqs = []
        req_title = self._cou_doc("h2[data-purpose='requirements-title']")
        if req_title:
            req_block = req_title.parent()
            for requirement_el in req_block.find("ul li .ud-block-list-item-content"):
                text = requirement_el.text_content().strip()
                if text:
                    reqs.append(text)
        return reqs

    def description(self) -> str | None:
        desc_el = self._cou_doc(
            "div[data-purpose='course-description'] div[data-purpose='safely-set-inner-html:description:description']")
        if desc_el:
            desc = desc_el.text().strip()
            if desc:
                return desc
        return None

    def target_audience(self) -> list[str]:
        audience_list = []
        for li in self._cou_doc(
            "div[data-purpose='course-description'] div[data-purpose='target-audience'] ul li"):
            text = li.text_content().strip()
            if text:
                audience_list.append(text)
        return audience_list

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
            main_cat = entry["main_category"]
            sub_cat = entry["sub_category"]
            if  main_cat == main_category and sub_cat== sub_category:
                return "same", None, None
            else:
                return "different", main_cat, sub_cat
    return False, None, None

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
        yield iterable[i:i + size]

def path_maker(main_category, sub_category, file_index):
    safe_main = sanitize_filename(main_category)
    # print("safe main is good")
    safe_sub = sanitize_filename(sub_category)
    # print("safe sub is good")

    category_dir = OUTPUT_DIR / safe_main / safe_sub
    category_dir.mkdir(parents=True, exist_ok=True)

    out_file_path = category_dir / f"{safe_main}_{safe_sub}_{file_index}.json"

    return out_file_path

def save_course(out_file: str | Path, course: dict, main_category: str, sub_category: str):
    """
    Save a course dict into a JSON file for a given category/subcategory.
    - Prevents duplicate URLs.
    - Creates the file if it doesn't exist.
    
    Args:
        out_file (str | Path): Path to the JSON file.
        course (dict): The course data to save.
        main_category (str): Main category name.
        sub_category (str): Subcategory name.
    """
    out_file = Path(out_file)
    
    if out_file.exists():
        with open(out_file, "r", encoding="utf-8") as f:
            existing_data = json.load(f)
        existing_courses = existing_data.get("courses", [])
        existing_urls = {c.get("url") for c in existing_courses}
        if course.get("url") not in existing_urls:
            existing_courses.append(course)
        all_courses = existing_courses
    else:
        all_courses = [course]

    data = {
        "main_category": main_category,
        "sub_category": sub_category,
        "courses": all_courses
    }

    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"💾 Saved course → {out_file}")

# ---------- CRAWLING LOGIC ----------
async def crawl_course(page, main_category, sub_category, cou_urls: list[str], out_file: str, done_list):
    print(f"➡️ Started crawling: {main_category} -> {sub_category}")

    for url in cou_urls:
        try:
            status, crawled_in_main_cat, crawled_in_sub_cat = is_already_crawled(url, main_category, sub_category, done_list)

            course = {}

            # Handle URL already crawled in this category
            if status == "same":
                print(f"✅ Already crawled in this category: {url}")
                continue

            # Handle URL crawled in a different category
            if status == "different":
                print(f"⚠️ URL already crawled in another category: {url}")
                course["crawled_in_main_category"] = crawled_in_main_cat
                course["crawled_in_sub_category"] = crawled_in_sub_cat
                course["url"] = url

            else:
                # Crawl the page normally
                print(f"🌍 Visiting (stealth): {url}")
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                await asyncio.sleep(random.uniform(2, 5))

                html_content = await page.content()
                course_page = Page(html_content)

                # Title
                course["title"] = course_page.title()

                # URL
                course["url"] = url

                # Bio
                course["bio"] = course_page.bio()

                # Objectives
                course["objectives"] = course_page.objectives()

                # Course content
                course["course_content"] = course_page.course_content()

                # Requirements
                course["requirements"] = course_page.requirements()

                # Description
                course["description"] = course_page.description()

                # Target audience
                course["target_audience"] = course_page.target_audience()

            # ---------------- SAVE COURSE ---------------- #
            save_course(out_file, course, main_category, sub_category)

            # Mark URL as done
            save_done_crawling_entry(url, main_category, sub_category)

        except Exception as e:
            print(f"❌ Error crawling {url}: {e}")
            save_done_crawling_entry(url, main_category, sub_category)

# ------------------ PARALLEL RUNNER ------------------ #
async def crawl_multiple(targets, concurrency_limit=CONCURRENCY_LIMIT): 
    """ 
    Crawl multiple sub-category URLs in parallel, limited by concurrency_limit. 
    Each task runs in its own browser context (like a new window). 
    Skips creating tasks if the URL was already crawled. 
    """ 
    async with Stealth().use_async(async_playwright()) as p: 
        browser = await p.chromium.launch(headless=False) 
        sem = asyncio.Semaphore(concurrency_limit) 
 
        # Load once instead of reloading for every URL 
        done_list = load_done_crawling() 
 
        async def sem_task(main, sub, url, out_file): 
            async with sem: 
                delay = random.uniform(1, 5) 
                print(f"⏳ Delaying {delay:.1f}s before starting {sub}") 
                await asyncio.sleep(delay) 
 
                context = await browser.new_context(user_agent=random.choice(USER_AGENTS)) 
                page = await context.new_page() 
                try: 
                    return await crawl_course( 
                        page, 
                        main_category=main, 
                        sub_category=sub, 
                        cou_urls=[url], 
                        out_file=out_file, 
                        done_list=done_list 
                    ) 
                finally: 
                    await context.close() 
 
        tasks = [] 
        for m, s, u in targets: 
            # Check only once here before scheduling 
            status, crawled_main, crawled_sub = is_already_crawled(u, m, s, done_list) 
            if status == "same": 
                print(f"✅ Already crawled in this category: {u}") 
                continue 
            elif status == "different": 
                print(f"⚠️ Already crawled in another category ({crawled_main}/{crawled_sub}): {u}") 
                save_done_crawling_entry(u, m, s) 
                continue 
 
            out_file = path_maker(m, s, 1) 
            tasks.append(sem_task(m, s, u, out_file)) 
 
        if tasks: 
            await asyncio.gather(*tasks) 
 
        await browser.close()

# --------------------- MAIN FUNCTION ----------------- #
async def main():
    all_urls = load_all_course_urls(INPUT_DIR)

    for (main_cat, subs) in list(all_urls.items())[3:]:
        print(main_cat)
        for sub in subs:
            for sub_cat, urls in sub.items():
                
                # Find the next available file index for this sub_cat
                file_index = 1
                while True:
                    file_path = Path(path_maker(main_cat, sub_cat, file_index))
                    if not file_path.exists():
                        break
                    file_index += 1

                # Load already crawled URLs from existing files for this sub_cat
                crawled_urls = set()
                for old_index in range(1, file_index):  # all earlier files
                    old_file = Path(path_maker(main_cat, sub_cat, old_index))
                    if old_file.exists():
                        try:
                            with open(old_file, "r", encoding="utf-8") as f:
                                data = json.load(f)
                                for c in data.get("courses", []):
                                    if "url" in c:
                                        crawled_urls.add(c["url"])
                        except Exception as e:
                            print(f"⚠️ Error reading {old_file}: {e}")

                # Filter out URLs that were already crawled
                new_urls = [u for u in urls if u not in crawled_urls]
                if not new_urls:
                    print(f"✅ All URLs already crawled for {main_cat}/{sub_cat}")
                    continue

                # Crawl in batches of 5 
                for batch in chunked(new_urls, BATCH_SIZE): 
                    print(f"🚀 Crawling batch of {len(batch)} for {main_cat}/{sub_cat}") 
                    targets = [(main_cat, sub_cat, u) for u in batch] 
                    await crawl_multiple(targets, CONCURRENCY_LIMIT) 
                    print("✅ Batch finished")
if __name__ == "__main__":
    asyncio.run(main())