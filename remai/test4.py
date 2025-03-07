import logging
import re

# 获取记录器
logger = logging.getLogger("my_module")
logger.setLevel(logging.DEBUG)
# 创建处理器
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# 创建格式器
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
# 添加处理器到记录器
logger.addHandler(ch)
# 记录不同级别的消息
logger.debug("debug message")
logger.info("info message")
logger.warning("warn message")
logger.error("error message")
logger.critical("critical message")


# 示例字符串
text = "今天的日期是2025-02-23，明天的日期可能是2025-1-10。"

# 正则表达式模式，匹配YYYY-M-D或YYYY-MM-DD格式的日期，其中M和D可以是一位或两位数字
pattern = r"\d{4}-(?:0[1-9]|1[0-2]|[1-9])-(?:0[1-9]|[12][0-9]|3[01]|[1-9])"
# pattern = r"\d{4}-\d{2}-\d{2}"

# 使用re.findall()找到所有匹配的日期
dates = re.findall(pattern, text)

# 输出匹配的日期
print(dates)

text = "下载数据日期为2025-02-23，点击企业名称可查看最新数据。更多企业信息请访问：aiqicha.baidu.com "

# 使用re.findall()找到所有匹配的日期
dates = re.findall(pattern, text)

# 输出匹配的日期
print(dates)

p = r"^下载数据日期"
t = re.match(p, text)
print(f"t = {t}")

print(f"{text.startswith('下载数据日期')}")

text.startswith
