import requests
from bs4 import BeautifulSoup

def search_company_info(company_name):
    # 构造请求 URL
    url = 'https://aiqicha.baidu.com/s?q='+ company_name
    print(url)
    # 发送 HTTP 请求
    response = requests.get(url)
    
    # 解析 HTML 内容
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # 提取公司信息
    company_info = soup.find('div', class_='content').text
    
    return company_info

# 查询并打印公司信息
company_name = '华为技术有限公司'
info = search_company_info(company_name)
print(info)
