import glob
import logging
import os
from datetime import datetime

SUCCESS_LEVEL_NUM = 25
logging.addLevelName(SUCCESS_LEVEL_NUM, "SUCCESS")


class DualFormatter(logging.Formatter):
    """
    自定义格式化器：
    如果日志记录中包含 'is_raw=True'，则只输出消息内容；
    否则输出带时间戳和等级的标准格式。
    """

    def format(self, record):
        # 检查 record 中是否有 is_raw 属性且为 True
        if getattr(record, 'is_raw', False):
            return record.getMessage()
        return super().format(record)


class LoggerWrapper:
    """
    Logger 的包装类，支持 raw 参数
    """

    def __init__(self, logger):
        self._logger = logger

    def _sanitize(self, msg):
        if isinstance(msg, str):
            return msg.encode('utf-8', 'surrogateescape').decode('utf-8', 'replace')
        return msg

    def debug(self, msg, raw=False):
        """记录 debug 级别日志，raw=True 时不带格式"""
        self._logger.debug(self._sanitize(msg), extra={'is_raw': raw})

    def info(self, msg, raw=False):
        """记录 info 级别日志，raw=True 时不带格式"""
        self._logger.info(self._sanitize(msg), extra={'is_raw': raw})

    def success(self, msg, raw=False):
        """记录 success 级别日志，raw=True 时不带格式"""
        self._logger.log(SUCCESS_LEVEL_NUM, self._sanitize(msg), extra={'is_raw': raw})

    def warning(self, msg, raw=False):
        """记录 warning 级别日志"""
        self._logger.warning(self._sanitize(msg), extra={'is_raw': raw})

    def error(self, msg, raw=False):
        """记录 error 级别日志，raw=True 时不带格式"""
        self._logger.error(self._sanitize(msg), extra={'is_raw': raw})

    # 如果需要访问底层 logger 的其他方法（如 warning, debug），可以通过 getattr 委托
    def __getattr__(self, name):
        return getattr(self._logger, name)


class DailyRotatingFileHandler(logging.FileHandler):
    """
    按天滚动的日志处理器，每天生成一个新的日志文件，并清理旧日志。
    """
    def __init__(self, log_dir, max_log_files, encoding='utf-8'):
        self.log_dir = log_dir
        self.max_log_files = max_log_files
        self.current_date = datetime.now().strftime("%Y%m%d")
        filename = os.path.join(log_dir, f"smartarchiver-{self.current_date}.log")
        super().__init__(filename, encoding=encoding)

    def emit(self, record):
        new_date = datetime.now().strftime("%Y%m%d")
        if new_date != self.current_date:
            self.current_date = new_date
            self.close()
            filename = os.path.join(self.log_dir, f"smartarchiver-{self.current_date}.log")
            self.baseFilename = os.path.abspath(filename)
            self.stream = self._open()
            
            # 日期变化时，执行旧日志清理
            if self.max_log_files > 0:
                clean_old_logs(self.log_dir, self.max_log_files)
                
        super().emit(record)


def setup_logger(log_dir, max_log_files=0, log_level="INFO"):
    """
    配置日志记录器，格式：2025-01-01 08:00:00 [INFO] 消息内容
    """
    if not os.path.exists(log_dir): os.makedirs(log_dir)

    # 获取原生 logger
    logger = logging.getLogger("smartarchiver")
    
    # 设置日志级别
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    logger.setLevel(numeric_level)

    # 清除旧的 handler 防止重复添加
    if logger.handlers:
        logger.handlers = []

    file_handler = DailyRotatingFileHandler(log_dir, max_log_files, encoding='utf-8')

    # 执行旧日志清理 (启动时清理一次)
    if max_log_files > 0:
        # 注意：这里会把今天刚生成的文件也算在总数里（如果文件已存在）
        clean_old_logs(log_dir, max_log_files)

    # 定义标准格式字符串
    fmt_str = '%(asctime)s [%(levelname)s] %(message)s'

    # 使用自定义的 Formatter
    formatter = DualFormatter(fmt_str, datefmt='%Y-%m-%d %H:%M:%S')

    # 将标准日志级别映射为中文以匹配原脚本习惯
    # logging.addLevelName(logging.INFO, "信息")
    # logging.addLevelName(logging.ERROR, "错误")

    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # 同时输出到控制台
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 返回包装后的 Logger 对象
    return LoggerWrapper(logger)


def clean_old_logs(log_dir, max_files):
    """
    清理旧日志文件，只保留最近的 max_files 个
    """
    if max_files <= 0:
        return

    # 匹配日志文件模式
    log_pattern = os.path.join(log_dir, "smartarchiver-*.log")

    # 获取所有匹配的日志文件
    files = glob.glob(log_pattern)

    # 按照文件名排序 (因为文件名包含 YYYYMMDD，所以按字母排序即按时间排序)
    # 从旧到新排序
    files.sort()

    # 如果文件数量超过限制
    if len(files) > max_files:
        # 计算需要删除的文件数量
        num_to_delete = len(files) - max_files
        # 获取最旧的那些文件
        files_to_delete = files[:num_to_delete]

        for f in files_to_delete:
            try:
                os.remove(f)
                print(f"已删除日志: {f}")
            except OSError as e:
                # 即使删除失败也不要在此时崩溃，仅打印错误
                print(f"删除日志失败: {f}\n{e}")

# def clean_logs(log_dir, retention_days, logger=None):
#     """
#     清理 X 天前的日志文件
#     """
#     if not os.path.exists(log_dir):
#         return
#
#     logger.info(f"开始清理 {retention_days} 天前的旧日志...")
#
#     now = time.time()
#     cutoff_seconds = retention_days * 86400
#     deleted_count = 0
#
#     for filename in os.listdir(log_dir):
#         file_path = os.path.join(log_dir, filename)
#
#         # 只处理文件，且扩展名为 .log (为了安全)
#         if os.path.isfile(file_path) and filename.endswith('.log'):
#             try:
#                 mtime = os.path.getmtime(file_path)
#                 if (now - mtime) > cutoff_seconds:
#                     os.remove(file_path)
#                     logger.info(f"已删除旧日志: {filename}")
#                     deleted_count += 1
#             except Exception as e:
#                 logger.error(f"删除日志失败 {filename}: {e}")
#
#     logger.info(f"日志清理完成，共删除 {deleted_count} 个文件。")
