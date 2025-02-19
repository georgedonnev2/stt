import time
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By

chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument(
    "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36"
)

# driver = Chrome("./chromedriver", options=chrome_options)
driver = webdriver.Chrome(
    service=Service("/usr/local/bin/chromedriver"), options=chrome_options
)

# with open("/Users/kingname/test_pyppeteer/stealth.min.js") as f:
with open("stealth.min.js") as f:
    js = f.read()

driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": js})
driver.get("https://bot.sannysoft.com/")
time.sleep(5)
driver.save_screenshot("walkaround.png")

# 你可以保存源代码为 html 再双击打开，查看完整结果
source = driver.page_source
with open("result.html", "w") as f:
    f.write(source)
