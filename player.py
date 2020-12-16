import asyncio
import websockets
import traceback
import json
import logging
from logging.handlers import TimedRotatingFileHandler
from config import Config
import os
import sys
import traceback
from datetime import datetime
import time

def configure_logging(name,level = logging.DEBUG):
    # https://docs.python.org/3/howto/logging-cookbook.html

    logging.getLogger('websockets').setLevel(logging.INFO)
    logging.getLogger('asyncio').setLevel(logging.INFO)

    logger = logging.getLogger(name)  # by default root.
    '''
    The level set in the logger determines which severity of messages it will pass to its handlers.
    The level set in each handler determines which messages that handler will send on.
    '''
    logger.setLevel(level)

    formatter = logging.Formatter(
        '[%(asctime)s] {%(name)s:%(filename)s:%(lineno)d} %(levelname)s - %(message)s')

    stream_handler = logging.StreamHandler(stream=sys.stdout)
    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    logger.info("Console logging configured OK")

    if (os.path.exists("./logs")):
        filename = "./logs/" + name + "-" + str(int(time.time())) +".log"
        file_hanlder = TimedRotatingFileHandler(
            filename, when="midnight", interval=1)
        file_hanlder.suffix = "%Y%m%d"
        file_hanlder.setLevel(level)
        file_hanlder.setFormatter(formatter)
        logger.addHandler(file_hanlder)
        logger.info("File logging configured OK")
    else:
        logger.warning(
            "File logging not configured. Path ./log does not exist")

    return logger


async def main():
    uri = "ws://localhost:8082/subscribe"
    async with websockets.connect(uri) as websocket:
        await websocket.send(json.dumps(["BTCUSD","ETHUSD"]))
        while True:
            try:
                r = await websocket.recv()
            except websockets.exceptions.ConnectionClosed as e:
                logger.error(f"websockets.exceptions.ConnectionClosed:{e}>{traceback.format_exc()}")
                break
            except Exception as e:
                logger.error(f"Exception:{e}->{traceback.format_exc()}")
                break
            else:
                #response = json.loads(r)
                logger.debug(f"response:{r}")



if __name__ == "__main__":
    logger = configure_logging("player.py")
    logger.info("Configure logging OK")
    logger.info("Start Player")
    asyncio.run(main())