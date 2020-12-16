import asyncio
import websockets
import random
from collections import defaultdict
import concurrent.futures
import time
import json
import threading
import functools
from aiohttp import web  
import logging
from logging.handlers import TimedRotatingFileHandler
from config import Config
import os
import sys
import traceback
from datetime import datetime


CACHE = defaultdict(tuple)
CHANNELS = defaultdict(set)
ALLOWED_CHANNELS = ("BTCUSD","LTCUSD","ETHUSD")
GEMMINI_CHANNELS = ("BTCUSD","LTCUSD","ETHUSD")
GEMINI_CANDLES_TYPE = "candles_5m"

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


async def notify(pair,action,epoch):
    message = {'type':'signal' ,'action': action,'pair':pair,'epoch': epoch}
    logger.debug(f"notify {threading.get_ident()}")
    tasks = [client.send(json.dumps(message)) for client in CHANNELS[pair]]
    if tasks:
        await asyncio.wait(tasks)

async def unsubscribe (websocket):

    for pair in CHANNELS:
        CHANNELS[pair].discard(websocket)
        
    logger.debug(f"unsubscribe  CHANNELS:{CHANNELS}")       

async def subscribe(websocket,pairs):
    # do something with the message
    logger.debug(f"subscribe {threading.get_ident()}")
    for pair in pairs:
        CHANNELS[pair].add(websocket)
    logger.debug(f"subscribe  CHANNELS:{CHANNELS}") 

async def incoming_handler(websocket, path):
    logger.debug(f"incoming_handler {threading.get_ident()}")
    try :
        async for message in websocket:
            logger.debug(f"{websocket} : {path}")
            logger.debug(message)
            if path == "/subscribe":
                pairs = json.loads(message)
                logger.debug(f"pairs:{pairs}")
                if type(pairs) == list and pairs:
                    logger.debug(f"Type list not empty")
                    await subscribe(websocket,pairs)
    except Exception as e:
        logger.error(f"Exception in websocket message:{e}")
        return

async def outgoing_handler(websocket, path):
    logger.debug(f"outgoing_handler {threading.get_ident()}")
    message = {'type': 'heartbeat'}
    while True:
        await asyncio.sleep(10)
        logger.debug(f"just keeping connection alive!{threading.get_ident()}")
        await websocket.send(json.dumps(message))
        


async def handler(websocket, path):
    logger.debug(f"handler {threading.get_ident()}")

    inconming_task = asyncio.create_task(incoming_handler(websocket, path))
    outgoing_task = asyncio.create_task(outgoing_handler(websocket, path))
    done, pending = await asyncio.wait([inconming_task, outgoing_task],
        return_when=asyncio.FIRST_COMPLETED,
    )

    for task in done:
        try:
            if task is inconming_task:
                logger.debug(f"Done inconming_task:{websocket}")
                await unsubscribe(websocket)
            elif task is outgoing_task:
                logger.debug("Done outgoing_task")
            else:
                logger.debug("?")
        except Exception as e:
                logger.error(f"Exception in done: {e}")

    for task in pending:
        logger.debug(f"Canceling:{task}")
        task.cancel()
    
    logger.debug("End Handler")


def worker(pair):
    
    logger.debug(f"worker {pair}: {threading.get_ident()}")
    CACHE[pair] = ("stay",int(time.time()))
    time.sleep(random.randint(1,10))
    guess = random.random()
    logger.debug(f"worker {pair}: guess:{guess}")
    if guess > 0.8:
        CACHE[pair] = ("sell",int(time.time()))
        logger.debug(f"sell {pair}: by guess{guess}")
        return 'sell'
    if guess < 0.2:
        CACHE[pair] = ("buy",int(time.time()))
        logger.debug(f"buy {pair}: by guess{guess}")
        return 'buy'
    return None

def parse_gemmini_candle_response(response: dict) -> dict:

    if type(response) == dict and all( (key in response for key in ['type',"symbol","changes"]) ):
        if response['type'] == GEMINI_CANDLES_TYPE + "_updates":
            if response['symbol'] in GEMMINI_CHANNELS:
                changes = list(response['changes'])
                return parse_gemmini_candle(response['symbol'],changes[0])
    else:
        return None
def parse_gemmini_candle(pair: str, candle: list) -> dict:
    if type(candle) == list and len(candle) == 6:
        new_candle = {}
        new_candle['source'] = "gemini"
        new_candle['frame'] = GEMINI_CANDLES_TYPE.split("_")[1]
        new_candle['epoch'] = int(candle[0]/1000) #in seconds
        new_candle['date'] = datetime.utcfromtimestamp(new_candle['epoch']).strftime('%Y-%m-%d %H:%M:%S')
        new_candle['pair'] = str(pair)
        new_candle['open'] = float(candle[1])
        new_candle['high'] = float(candle[2])
        new_candle['low'] = float(candle[3])
        new_candle['close'] = float(candle[4])
        new_candle['volume'] = float(candle[5])
        return new_candle
    else:
        return None


async def brain_N_one(pair):
    #always must be running catch all execptions
    ws_uri = "wss://api.gemini.com/v2/marketdata"

    while True:
        try:
            logger.debug(f"Starting connection with Gemmini:{pair}")
            async with websockets.connect(ws_uri) as websocket:
                data = {"type": "subscribe","subscriptions":[{"name":GEMINI_CANDLES_TYPE,"symbols":[pair]}]}
                await websocket.send(json.dumps(data))
                while True:
                    try:
                        r = await websocket.recv()
                    except websockets.exceptions.ConnectionClosed as e:
                        logger.error(
                            f"websockets.exceptions.ConnectionClosed:{e}>{traceback.format_exc()}")
                        break
                    except Exception as e:
                        logger.error(
                            f"Exception:{e}->{traceback.format_exc()}")
                        break
                    else:
                        response = json.loads(r)
                        candle = parse_gemmini_candle_response(response)

                    if candle is not None:
                        logger.debug(f'New candle:{candle}')
                        logger.debug(f'Brain_N channels:{CHANNELS}')
                        with concurrent.futures.ThreadPoolExecutor() as pool:
                            signal = await asyncio.get_event_loop().run_in_executor(pool,worker,pair)
                            if signal is not None:
                                await notify(pair,signal,int(time.time()))
        except Exception as e:
                logger.error(f"Exception:{e}->{traceback.format_exc()}")

        logger.debug(f"Connection lost with Gemmini pair:{pair}")
        await asyncio.sleep(5)


async def brain_N_all():
    #always must be running catch all execptions
    ws_uri = "wss://api.gemini.com/v2/marketdata"
   
    while True:
        try:
            logger.debug("Starting connection with Gemini")
            async with websockets.connect(ws_uri) as websocket:
                data = {"type": "subscribe","subscriptions":[{"name":GEMINI_CANDLES_TYPE,"symbols":GEMMINI_CHANNELS}]}
                await websocket.send(json.dumps(data))
                while True:
                    try:
                        r = await websocket.recv()
                    except websockets.exceptions.ConnectionClosed as e:
                        logger.error(
                            f"websockets.exceptions.ConnectionClosed:{e}>{traceback.format_exc()}")
                        break
                    except Exception as e:
                        logger.error(
                            f"Exception:{e}->{traceback.format_exc()}")
                        break
                    else:
                        response = json.loads(r)
                        candle = parse_gemmini_candle_response(response)

                    if candle is not None:
                        logger.debug(f'New candle:{candle}')
                        logger.debug(f'Brain_N channels:{CHANNELS}')
                        with concurrent.futures.ThreadPoolExecutor() as pool:
                            signal = await asyncio.get_event_loop().run_in_executor(pool,worker,candle['pair'])
                            if signal is not None:
                                await notify(candle['pair'],signal,int(time.time()))
        except Exception as e:
                logger.error(f"Exception:{e}->{traceback.format_exc()}")

        logger.debug("Connection lost with Gemini")
        await asyncio.sleep(5)


async def cache(request):
    return web.json_response(CACHE)

async def state(request):
    res = {'state': 'OK', 'epoch': int(time.time())}
    return web.json_response(res)


async def channels(request):
    res = [key for key in CHANNELS]
    return web.json_response(res)

async def webserver():
    app = web.Application()
    app.add_routes([web.get('/', state),web.get('/cache', cache),web.get('/channels', channels)])

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8081)
    await site.start()
    while True:
        await asyncio.sleep(3600) #sleep forever waiting connections
    await runner.cleanup()
    

if __name__ == "__main__":
    logger = configure_logging(Config.LOGGING_NAME)
    logger.info("Configure logging OK")
    logger.info("Start")
    logger.info(f"Start {threading.get_ident()}")

    ws_server = websockets.serve(handler,"localhost",8082)
    tasks = [asyncio.get_event_loop().create_task(brain_N_one(pair)) for pair in ALLOWED_CHANNELS]
    tasks.append(ws_server)
    #tasks.append(asyncio.get_event_loop().create_task(brain_N_all()))
    '''
    for pair in ALLOWED_CHANNELS:
        tasks.append( asyncio.get_event_loop().create_task( brain_N_one(pair) ) )
    '''
    tasks.append(asyncio.get_event_loop().create_task(webserver()))
    asyncio.get_event_loop().run_until_complete(asyncio.wait(tasks))
    asyncio.get_event_loop().run_forever()

    logger.info("ENDMAIN")