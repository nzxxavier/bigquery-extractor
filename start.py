import sys
import traceback
from datetime import datetime, timedelta
from log import logger
from utils import alarm
from reader import Reader


if __name__ == "__main__":
    argv = sys.argv[1:]
    reader = None
    if argv.__len__() >= 1:
        if argv[0] not in ('minute', 'hour', 'day'):
            logger.info("python start.py [minute, hour, day] [0, 1, 2, 3 ...]")
            exit()
        if int(argv[1]) < 0:
            logger.info("python start.py [minute, hour, day] [0, 1, 2, 3 ...]")
            exit()
        today = datetime.now()
        day_diff = timedelta(days=float(argv[1]))
        day = (today-day_diff).strftime("%Y%m%d")
        logger.set_name(f"realtime_{argv[0]}_{day}")
        reader = Reader(f"realtime_{argv[0]}_{day}", argv[0], int(argv[1]))
    else:
        logger.info("python start.py [minute, hour, day] [0, 1, 2, 3 ...]")
        exit()
    try:
        reader.read()
    except Exception:
        logger.error(traceback.format_exc())
        alarm("[error] bigquery同步异常退出\n"
              f"{traceback.format_exc()}")
