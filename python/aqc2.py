# -*- coding:utf-8 -*-
#爬虫获取企业工商注册信息

from selenium import webdriver
import requests
import re
import json
import os

headers = {'User-Agent': 'Chrome/76.0.3809.132'}

#需要安装phantomjs，然后将phantomjs.exe路径指定到path
path = os.path.join(os.getcwd(), 'static', 'phantomjs', 'phantomjs.exe')
# print(path)
# 正则表达式提取数据
re_get_js = re.compile(r'<script>([\s\S]*?)</script>')
re_resultList = re.compile(r'"resultList":(\[{.+?}\]),"totalNumFound')
has_statement = False


def get_company_info(name):
    '''
        @func: 通过百度企业信用查询企业基本信息
    '''
    url = 'https://aiqicha.baidu.com/s?q=%s' % name
    res = requests.get(url, headers=headers)

    if res.status_code == 200:
        html = res.text
        print("==html==", html)
        js = re_get_js.findall(html)[1]
        print("==js==",js)
        data = re_resultList.search(js)
        if not data:
            return
        company = json.loads(data.group(1))[0]
        url = 'https://aiqicha.baidu.com/company_detail_{}'.format(company['pid'])

        # 调用环境变量指定的PhantomJS浏览器创建浏览器对象
        driver = webdriver.PhantomJS(path)
        driver.set_window_size(1366, 768)
        driver.get(url)

        # 获取页面名为wraper的id标签的文本内容
        data = driver.find_element_by_class_name('zx-detail-basic-table').text
        data = data.split()
        data_return = {}
        need_info = ['法定代表人', '经营状态', '注册资本', '实缴资本', '曾用名', '所属行业', '统一社会信用代码', '纳税人识别号', '工商注册号', '组织机构代码', '登记机关',
                     '成立日期', '企业类型', '营业期限', '审核/年检日期', '注册地址', '经营范围']
        for epoch, item in enumerate(data):
            if item in need_info:
                if data[epoch + 1] in need_info or data[epoch + 1] == '-':
                    data_return[item] = None
                    continue
                if item == '法定代表人':
                    if len(data[epoch + 1]) == 1:
                        data_return[item] = data[epoch + 2]
                    else:
                        data_return[item] = data[epoch + 1]
                    pass
                elif item == '营业期限':
                    if data[epoch + 2] == '至':
                        data_return[item] = data[epoch + 1] + ' ' + data[epoch + 2] + ' ' + data[epoch + 3]
                else:
                    data_return[item] = data[epoch + 1]
        return data_return
    else:
        print('无法获取%s的企业信息' % name)


get_company_info('江苏苏宁')

