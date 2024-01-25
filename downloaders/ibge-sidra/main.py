from playwright.sync_api import sync_playwright, Playwright, expect
import time

def run(playwright: Playwright):
    browser = playwright.chromium.launch(headless=False, args=["--start-maximized"])
    context = browser.new_context(no_viewport=True)

    page = context.new_page()
    page.goto("https://sidra.ibge.gov.br/home/abate/brasil")

    # getting to each table individually

    button_text = "Ir para a página da pesquisa"

    page.get_by_title(button_text).click()
    time.sleep(1)

    links_table = page.get_by_title("Abrir Tabela").all()
    links = []
    for l in links_table:
        links.append(l.get_attribute("href"))
    links = list(set(links))


    # following table links
    for l in links:
        page.goto("https://sidra.ibge.gov.br"+l)
        # essa logica aqui serve para todas as paginas de tabela. Generalizar
        for checkbox in page.get_by_title("Desmarcar todos os elementos listados").all()[:-1]:
            checkbox.click()

        for checkbox in page.get_by_title("Marcar todos os elementos listados",exact=True).all()[:-1]:
            checkbox.click()

        page.get_by_role("button", name="Download").click()

        time.sleep(1)

        expect(page.get_by_role("heading", name="Download")).to_be_visible()
        page.get_by_role("checkbox", name="Comprimir (.zip)").check()
        page.click("#opcao-downloads")


        break
    page.pause()

    # page.go_back()

    page.pause()

    context.close()

with sync_playwright() as playwright:
    run(playwright)
