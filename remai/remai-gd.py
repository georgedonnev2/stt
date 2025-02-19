#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By

import time

import logging

logging.basicConfig(level=logging.INFO)

# set some options for Chrome
b_option = Options()
b_option.add_argument("--no-sandbox")
# b_option.add_argument("--headless")
b_option.add_argument(
    "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
)
b_option.add_experimental_option(name="detach", value=True)


# get a browser
g_browser = webdriver.Chrome(
    service=Service("/usr/local/bin/chromedriver"), options=b_option
)
g_browser.implicitly_wait(30)

# open qcc
logging.info(f"open qcc.")
g_browser.get("https://www.qcc.com/")
time.sleep(30)  # to scan QR to login.
logging.info("opened.")

#
# g_browser.maximize_window()
logging.info("input company name and search.")
input_box = g_browser.find_element(By.ID, value="searchKey")
input_box.clear()
input_box.send_keys("腾讯科技（深圳）有限公司")
time.sleep(3)
search_btn = g_browser.find_element(By.CLASS_NAME, value="input-group-btn")
search_btn.click()
time.sleep(3)
logging.info("searched.")

logging.info("get link of detailinfo")
maininfo = g_browser.find_element(
    By.XPATH,
    value="/html/body/div[1]/div[2]/div[2]/div[3]/div/div[2]/div/table/tr[1]/td[3]/div[2]/span/span[1]/a",
)
logging.info(f"maininfo={maininfo}")

maininfo.click()

# switch to detail page
g_browser.switch_to.window(g_browser.window_handles[-1])
company_name_h1 = g_browser.find_element(By.TAG_NAME, value="h1")
print(f"company_name={company_name_h1}, company={company_name_h1.text}")


# max / min windows
"""
ggl_browser.maximize_window()
time.sleep(3)
ggl_browser.minimize_window()
time.sleep(3)
ggl_browser.maximize_window()
time.sleep(3)
"""

# position + size
"""
ggl_browser.set_window_position(x=200.0, y=200.0)
time.sleep(3)
ggl_browser.set_window_position(x=0, y=0)
time.sleep(3)
win_size = ggl_browser.get_window_size()
print(f"win_size={win_size}")
ggl_browser.set_window_size(width=800, height=800)
time.sleep(3)
ggl_browser.set_window_size(width=win_size["width"], height=win_size["height"])
time.sleep(3)
"""

# get element by ID, input text and press buttion to search
"""
input_box = ggl_browser.find_element(By.ID, value="kw")
input_box.send_keys("华为")
time.sleep(2)
input_box.clear()
time.sleep(2)
input_box.send_keys("华为")

search_btn = ggl_browser.find_element(By.ID, value="su")
search_btn.click()

time.sleep(3)
"""

# get element by XPATH, input text and press buttion to search
"""
print(f"select input box...")
input_box = ggl_browser.find_element(
    By.XPATH, value="/html/body/div[1]/div[1]/div[5]/div/div/form/span[1]/input"
)


print(f"send keys")
input_box.send_keys("华为")
time.sleep(2)
input_box.clear()
time.sleep(2)
input_box.send_keys("华为")

search_btn = ggl_browser.find_element(By.CSS_SELECTOR, value="#su")
search_btn.click()

time.sleep(3)
"""

# ggl_browser.close()
# ggl_browser.quit()


# from selenium import webdriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# import time

# # 设置 Chrome 选项
# chrome_options = Options()
# chrome_options.add_argument("--headless")  # 无头模式（可选）
# chrome_options.add_argument("--disable-gpu")  # 禁用 GPU（可选）
# chrome_options.add_experimental_option(name="detach", value=True)

# # 指定 ChromeDriver 路径（如果已添加到 PATH，可以省略）
# service = Service("/usr/local/bin/chromedriver")

# # 创建 WebDriver 实例
# driver = webdriver.Chrome(service=service, options=chrome_options)

# # 访问企查查网站
# driver.get("https://www.qcc.com/")

# # 等待页面加载（根据需要调整）
# time.sleep(5)

# driver.close()


# from selenium import webdriver
# from selenium.webdriver.chrome.service import Service

# # from webdriver_manager.chrome import ChromeDriverManager

# # 使用 webdriver_manager 自动管理 ChromeDriver（可选）
# # service = Service(ChromeDriverManager().install())
# # driver = webdriver.Chrome(service=service)

# # 或者手动指定 ChromeDriver 的路径（如果未使用 webdriver_manager）
# driver_path = "/usr/local/bin/chromedriver"  # 替换为你的 ChromeDriver 路径
# driver = webdriver.Chrome(executable_path=driver_path)

# # 打开百度首页
# driver.get("https://www.baidu.com")

# # 等待几秒钟以便观察结果（可选）
# import time

# time.sleep(5)

# # 关闭浏览器
# driver.quit()


# 最完美方案！如何防止 Selenium 被检测出来
# https://blog.csdn.net/cqcre/article/details/110944075
