import asyncio
import json
import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup as bs
from fake_useragent import UserAgent

# logging setup
LOG_FILE = 'olx-scraper.log'
handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=10*1024*1024,
    backupCount=5,
    encoding='utf-8'
)
logger = logging.getLogger("olx-log")
logger.setLevel(logging.INFO)
logger.propagate = False
formatter = logging.Formatter(
    fmt = "[{levelname} {name} {asctime}] {message}",
    style="{"
)
handler.setFormatter(formatter)
logger.addHandler(handler)
handler_cout = logging.StreamHandler()
handler_cout.setFormatter(formatter)
logger.addHandler(handler_cout)
# parser setup
LAST_ID = "last_id.txt"
def get_last_id():
    if os.path.exists(LAST_ID):
        with open(LAST_ID, 'r') as f:
            return f.read().strip()
    return None

def save_last_id(rec_id):
    with open(LAST_ID, 'w') as f:
        f.write(str(rec_id))

def is_promoted(i):
    link_el = i.find('a')
    href = link_el.get('href', '') if link_el else ""
    is_promoted = 'promoted' in href
    return is_promoted

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ua = UserAgent(browsers=['chrome', 'edge', 'firefox'], os=['windows', 'macos']).random
        context = await browser.new_context(
            user_agent=ua,
            viewport={'width': 1920, 'height': 1080},
            is_mobile=False
        )
        page = await context.new_page()
        new_top_id = None
        last_processed_id = get_last_id()
        results = []
        logger.info(f"Start scraping on: {ua[:30]}...")
        try:
            stop_scraping = False
            page_num = 1
            set_ids = set()
            while not stop_scraping:
                url = f'https://www.olx.ua/uk/nedvizhimost/kvartiry/dolgosrochnaya-arenda-kvartir/kiev/?currency=UAH&page={page_num}&search%5Border%5D=created_at:desc'
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_selector('div[data-testid="listing-grid"]', timeout=15000)
                await page.get_by_test_id('pagination-wrapper').scroll_into_view_if_needed()
                logger.info("Scrolling page...")

                html = await page.content()
                soup = bs(html, "html.parser")
                items = soup.find_all("div", attrs={"data-testid": "l-card"})
                if not items: 
                    break
                logger.info(f"Page {page_num}, found {len(items)}")
                time_collected = datetime.now().isoformat()
                for i in items:
                    curr_id = i.get('id')
                    if curr_id in set_ids:
                        continue
                    promoted = is_promoted(i)
                    if page_num == 1 and not promoted and new_top_id is None:
                        new_top_id = curr_id
                        logger.info(f"The latest id {new_top_id} is written to last_id.txt")
                    if curr_id == last_processed_id and not promoted:
                        logger.info("Found all new records")
                        stop_scraping = True
                        break

                    title_wrapper = i.find(attrs={"data-testid": "ad-card-title"})
                    h4_el = title_wrapper.find("h4") if title_wrapper else None
                    price_el = i.find(attrs={"data-testid": "ad-price"})
                    loc_el = i.find(attrs={"data-testid": "location-date"})
                    size_el = i.find("span", attrs={"data-nx-name": "P5"})
                    if h4_el and price_el:
                        set_ids.add(curr_id)
                        record = {
                            "id": curr_id,
                            "title": h4_el.get_text(strip=True),
                            "price": price_el.get_text(strip=True),
                            "location": loc_el.get_text(strip=True) if loc_el else "N/A",
                            "size": size_el.get_text(strip=True) if size_el else "N/A",
                            "url": "https://www.olx.ua" + title_wrapper.find("a")["href"] if title_wrapper.find("a") else "",
                            "time_collected": time_collected,
                            "source": "olx_kyiv"
                        }
                        results.append(record)
                if not soup.find('a', attrs={'data-testid': 'pagination-forward'}):
                    logger.info(f"Reached last page: {page_num}")
                    break
                page_num += 1

            if results:
                filename = f"olx_data_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(results, f, ensure_ascii=False, indent=4)
                logger.info(f"{len(results)} new records saved to: {filename}")
                if new_top_id:
                    save_last_id(new_top_id)
            else:
                logger.warning("No new records found")
        except Exception as e:
            logger.error(f"Error occured during scraping: {e}", exc_info=True)
            await page.screenshot(path="error_debug.png")
        finally:
            await browser.close()
            logger.info("Scraping finished")

if __name__ == "__main__":
    asyncio.run(main())