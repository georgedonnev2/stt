import logging
import os

# 获取当前脚本的目录
current_directory = os.path.dirname(os.path.abspath(__file__))

# 日志文件的完整路径
log_file_path = os.path.join(current_directory, "app_1.log")


# 配置基本的日志设置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(log_file_path)],
)

logger = logging.getLogger(__name__)

# 记录不同级别的日志
logger.debug("这是一个DEBUG级别的日志")
logger.info("这是一个INFO级别的日志")
logger.warning("这是一个WARNING级别的日志")
logger.error("这是一个ERROR级别的日志")
logger.critical("这是一个CRITICAL级别的日志")
