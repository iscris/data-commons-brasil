from playwright.sync_api import sync_playwright, Playwright, expect
import time

def default_run(playwright: Playwright, url, switch_tab=False, downloads_path="downloads/"):
    browser = playwright.chromium.launch(headless=False, args=["--start-maximized"],downloads_path=downloads_path)
    context = browser.new_context(no_viewport=True)

    page = context.new_page()
    page.goto(url)

    # getting to each table individually
    button_text = "Ir para a página da pesquisa"

    page.get_by_title(button_text).click()
    time.sleep(1)

    if switch_tab:
        new_page_url = page.url.replace("quadros","tabelas")
        page.goto(new_page_url)

    links_table = page.get_by_title("Abrir Tabela").all()
    links = []
    for l in links_table:
        links.append(l.get_attribute("href"))
    links = list(set(links))


    # following table links
    for l in links:
        page.goto("https://sidra.ibge.gov.br"+l)
        time.sleep(5)
        # essa logica aqui serve para todas as paginas de tabela. Generalizar
        for checkbox in page.get_by_title("Desmarcar todos os elementos listados").all()[:-1]:
            checkbox.click()

        for checkbox in page.get_by_title("Marcar todos os elementos listados",exact=True).all()[:-1]:
            checkbox.click()

        page.get_by_role("button", name="Download").click()

        time.sleep(1)

        expect(page.get_by_role("heading", name="Download")).to_be_visible()

        page.get_by_role("checkbox", name="Comprimir (.zip)").check()

        page.get_by_role("checkbox", name="Exibir siglas de níveis territoriais").check()
        page.get_by_role("checkbox", name="Exibir códigos de territórios").check()
        page.get_by_role("checkbox", name="Exibir nomes de territórios").check()
        page.get_by_role("checkbox", name="Exibir unidades de medida como coluna").check()
        

        # Start waiting for the download
        with page.expect_download() as download_info:
            # Perform the action that initiates download
            page.click("#opcao-downloads")
            download = download_info.value

            # Wait for the download process to complete and save the downloaded file somewhere
            download.save_as(download.suggested_filename)

    page.pause()

    context.close()


with sync_playwright() as playwright:
    default_run(playwright, url="https://sidra.ibge.gov.br/home/inpc/brasil", switch_tab=True)
