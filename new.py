import asyncio
import pandas as pd
from playwright.async_api import async_playwright



async def run(csv_name):
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=False, timeout=600000)
        context = await browser.new_context()
        data = pd.read_csv(csv_name)
        base_url = "https://www.startupwala.com/company-registration/private-limited"
        cin_list = data['cin'].tolist()
        company_list = data['company_name'].tolist()
        roc_list = data['roc'].tolist()
        target_url_list = []
        for i in range(len(cin_list)):
            print(roc_list[i])
            build_url = base_url + "-" + company_list[i].replace(" ", "-") + roc_list[i][3:] + "-" + cin_list[i]
            target_url_list.append(build_url)


        for url in target_url_list:
            page = await context.new_page()
            await page.goto(url)
            company_name = await page.locator(
                '#tableContainer > table > tbody > tr:nth-child(2) > td:nth-child(2)').text_content()
            cin_number = await page.locator(
                '#tableContainer > table > tbody > tr:nth-child(3) > td:nth-child(2)').inner_text()
            roc_name = await page.locator(
                '#tableContainer > table > tbody > tr:nth-child(4) > td:nth-child(2)').inner_text()
            date_of_incorporation = await page.locator(
                '#tableContainer > table > tbody > tr:nth-child(5) > td:nth-child(2)').inner_text()
            registered_address = await page.locator(
                '#tableContainer > table > tbody > tr:nth-child(6) > td:nth-child(2)').inner_text()
            auth_capital = await page.locator(
                '#tableContainer > table > tbody > tr:nth-child(7) > td:nth-child(2)').inner_text()
            paid_up_capital = await page.locator(
                '#tableContainer > table > tbody > tr:nth-child(8) > td:nth-child(2)').inner_text()
            date_of_last_agm = await page.locator(
                '#tableContainer > table > tbody > tr:nth-child(9) > td:nth-child(2)').inner_text()
            date_of_last_balance_sheet = await page.locator(
                '#tableContainer > table > tbody > tr:nth-child(10) > td:nth-child(2)').inner_text()
            company_status = await page.locator(
                '#tableContainer > table > tbody > tr:nth-child(11) > td:nth-child(2)').inner_text()
            print(
                f"{company_name}, {cin_number}, {roc_name}, {date_of_incorporation},{registered_address}, {auth_capital}, {paid_up_capital}, {date_of_last_agm}, {date_of_last_balance_sheet}, {company_status}")
    await context.close()
    await browser.close()

asyncio.run(run(csv_name="/home/mohanraj/projects/mca_data/db_merger/100.csv"))

