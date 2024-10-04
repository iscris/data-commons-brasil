from playwright.sync_api import Playwright, sync_playwright


def run(playwright: Playwright):
    browser = playwright.chromium.launch(headless=False, args=["--start-maximized"])
    context = browser.new_context(no_viewport=True)

    page = context.new_page()
    page.goto("https://sidra.ibge.gov.br/home/primpec/brasil")

    title = page.locator("xpath=//title")
    print(title.text_content())

    tabs_xpath = "/html/body/div[6]/div/div/div/div[1]/ul"
    title = page.locator("xpath=" + tabs_xpath)
    print(title)

    page.pause()

    context.close()


with sync_playwright() as playwright:
    run(playwright)
