import asyncio
import json
import os
import io
import logging
from datetime import datetime
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup as bs
from fake_useragent import UserAgent
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

load_dotenv()
# logging setup
LOG_FILE = 'olx-scraper.log'
logger = logging.getLogger("olx-log")
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    fmt = "[{levelname} {name} {asctime}] {message}",
    style="{"
)
handler_cout = logging.StreamHandler()
handler_cout.setFormatter(formatter)
logger.addHandler(handler_cout)

# parser setup
LAST_ID = "last_id.txt"
URL_HOST = os.getenv("URL_HOST")
TOKEN = os.getenv("TOKEN")
DATABRICKS_PATH = "/Volumes/workspace/default/olx_flats_data"

w = WorkspaceClient(host=URL_HOST,token=TOKEN)

def get_last_id():
    try:
        with w.files.download(f"{DATABRICKS_PATH}/last_id.txt").contents as f:
            last_id = f.read().decode('utf-8').strip()
            logger.info(f"Last id {last_id} read from databricks")
            return last_id
    except Exception as e:
        logger.warning(f"No last id file found: {e}")
        return None

def save_last_id(rec_id):
    try:
        data_stream = io.BytesIO(str(rec_id).encode('utf-8'))
        w.files.upload(f"{DATABRICKS_PATH}/last_id.txt", data_stream, overwrite=True)
        logger.info(f"New last_id downloaded to databricks: {rec_id}")
    except Exception as e:
        logger.error(f"Failed to save state to cloud: {e}")

def is_promoted(i):
    link_el = i.find('a')
    href = link_el.get('href', '') if link_el else ""
    is_promoted = 'promoted' in href
    return is_promoted

def upload_databricks(results):
    try:
        filename = f"olx_data_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
        remote_path = f"{DATABRICKS_PATH}/{filename}"
        json_data = json.dumps(results, ensure_ascii=False, indent=4).encode('utf-8')
        data_stream = io.BytesIO(json_data)
        logger.info(f"Downloading {filename} into Databricks")
        w.files.upload(remote_path, data_stream)
        logger.info("Data downloaded to Databricks")
    except Exception as e:
        logger.error(f"Loading error: {e}")

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
                        save_last_id(new_top_id)
                    if curr_id == last_processed_id and not promoted:
                        logger.info("Last record was reached, stop scraping")
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
                upload_databricks(results)
                logger.info(f"Found {len(results)} new records")
            else:
                logger.warning("No new records found")
        except Exception as e:
            logger.error(f"Error occured during scraping: {e}", exc_info=True)
        finally:
            await browser.close()
            logger.info("Scraping finished")

if __name__ == "__main__":
    asyncio.run(main())