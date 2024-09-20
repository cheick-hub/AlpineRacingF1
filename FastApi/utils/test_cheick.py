import logging
from log_handler import CustomLogger

logging.setLoggerClass(CustomLogger)
logger : CustomLogger = logging.getLogger("Mon logger")
logger.set_url("http://127.0.0.1:8000")
logger.configure_handlers(".")

logger.error("This is a test log message.")
logger.info("[info] This is a test log message.")
logger.send_logs({"tata": "toto"})