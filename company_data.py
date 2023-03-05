import asyncio
import logging
import sqlite3
import concurrent.futures
from playwright.async_api import async_playwright


class Database:
    def __init__(self, db_name):
        self.db_name = db_name

    def __enter__(self):
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
        return self

    def create_table(self):
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS indian_company_data (id INTEGER PRIMARY KEY AUTOINCREMENT,
                page_no INTEGER(8),cin VARCHAR(30) UNIQUE, company_name TEXT UNIQUE, roc VARCHAR, status VARCHAR)''')
        self.conn.commit()
        logging.info("Table created successfully")

    def insert_data(self, page_no, cin, company_name, roc, status):
        try:
            sqlite_insert_with_param = """INSERT INTO indian_company_data (page_no, cin, company_name, roc, status)
                              VALUES (?, ?, ?, ?, ?);"""
            data_tuple = (page_no, cin, company_name, roc, status)
            self.cursor.execute(sqlite_insert_with_param, data_tuple)
            self.conn.commit()

        except sqlite3.Error as error:
            logging.error("Failed to insert data into table", error)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.conn.close()
        logging.info("Connection closed")


async def run():
    async with async_playwright() as playwright:
        db_name = 'company_master_data_test.db'
        db = Database(db_name)
        with db:
            db.create_table()
            browser = await playwright.chromium.launch(headless=True, timeout=600000)
            context = await browser.new_context()
            page = await context.new_page()
            start_page = 1
            await page.goto(f"https://www.startupwala.com/list-of-registered-companies-in-india-P{start_page}",
                            wait_until="networkidle", timeout=0)
            for page_no in range(start_page, 97023):
                logging.info(f"page number: {page_no}")
                for i in range(25):
                    cin = await page.locator('[class="corporateIdentificationNumber"]').nth(i).inner_text()
                    company = await page.locator('[class="companyName"]').nth(i).inner_text()
                    roc = await page.locator('[class="roc"]').nth(i).inner_text()
                    status = await page.locator('[class="statusLink"]').nth(i).inner_text()
                    logging.info(f"({page_no}) {i}. {cin}, {company}, {roc}, {status}")
                    db.insert_data(page_no=page_no, cin=cin, company_name=company, roc=roc, status=status)
                await page.goto(f"https://www.startupwala.com/list-of-registered-companies-in-india-P{page_no + 1}",
                                wait_until="domcontentloaded", timeout=0)
        await browser.close()



if __name__ == '__main__':
    with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
        futures = [executor.submit(asyncio.run, run()) for _ in range(25)]
        concurrent.futures.wait(futures)

