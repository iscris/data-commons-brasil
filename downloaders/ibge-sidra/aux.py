from playwright.sync_api import sync_playwright, Playwright, expect
import time

def run(playwright: Playwright):
    browser = playwright.chromium.launch(headless=False, args=["--start-maximized"])
    context = browser.new_context(no_viewport=True)

    page = context.new_page()
    page.goto("https://sidra.ibge.gov.br/home/abate/brasil")

    # a = page.get_by_role("tablist").all()
    for row in page.get_by_role("tab").all():
        row.click()
        print(row.text_content())
        # time.sleep(1)
        time.sleep(1)


    # print(len(page.get_by_role("tab").all()))

    context.close()

with sync_playwright() as playwright:
    run(playwright)
