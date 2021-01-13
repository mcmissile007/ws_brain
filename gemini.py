import logging
import asyncio
import websockets
import json
import traceback
import sys
import time
from collections import defaultdict
from pymongo import MongoClient
from datetime import datetime

from config import Config


class Gemini():

    def __init__(self, pair: str):

        self.logger = logging.getLogger(
            Config.LOGGING_NAME + "." + str(__name__))
        self.logger.debug(f"Init {str(__name__)}")

        self.websocket = None
        self.pair = pair
        self.candles_type = "candles_5m"
        self.candles = []

    def __parse_candle_response(self, response: dict) -> dict:

        if type(response) == dict and all((key in response for key in ['type', "symbol", "changes"])):
            if response['type'] == self.candles_type + "_updates":
                if response['symbol'] == self.pair:
                    changes = list(response['changes'])
                    if len(changes) == 1:
                        self.candles.append(self.__parse_candle(response['symbol'], changes[0]))
                        self.logger.debug(f"Update candles {self.candles[-4:-1]}")
                    elif len(changes) > 1:
                        self.candles = [self.__parse_candle(response['symbol'], change) for change in changes[::-1]]
                        self.logger.debug(f"Initialize candles {self.candles[:5]} ... {self.candles[-4:-1]}")
                    else:
                        return None
                    return self.candles[-1]

        else:
            return None

    def __parse_candle(self, pair: str, candle: list) -> dict:
        if type(candle) == list and len(candle) == 6:
            new_candle = {}
            new_candle['source'] = "gemini"
            new_candle['frame'] = self.candles_type.split("_")[1]
            new_candle['epoch'] = int(candle[0]/1000)  # in seconds
            new_candle['date'] = datetime.utcfromtimestamp(
                new_candle['epoch']).strftime('%Y-%m-%d %H:%M:%S')
            new_candle['pair'] = str(pair)
            new_candle['open'] = float(candle[1])
            new_candle['high'] = float(candle[2])
            new_candle['low'] = float(candle[3])
            new_candle['close'] = float(candle[4])
            new_candle['volume'] = float(candle[5])
            return new_candle
        else:
            return None

    async def connect(self):
        ws_uri = "wss://api.gemini.com/v2/marketdata"
        self.logger.debug("Starting connection with Gemini")
        try:
            self.websocket = await websockets.client.connect(ws_uri)
            data = {"type": "subscribe", "subscriptions": [
                {"name": self.candles_type, "symbols": [self.pair]}]}
            await self.websocket.send(json.dumps(data))
            await self.initialize_candles()
        except Exception as e:
            self.logger.error(
                f"Error connecting to Gemmini: {e}>{traceback.format_exc()}")
            raise e


    async def disconnet(self):
        self.logger.debug("End connection with Gemini")
        try:
            await self.websocket.close()
        except Exception as e:
            self.logger.error(
                f"Error disconnecting to Gemmini: {e}>{traceback.format_exc()}")

    async def get_new_candle(self):
        try:
            r = await self.websocket.recv()
        except websockets.exceptions.ConnectionClosed as e:
            self.logger.error(
                f"websockets.exceptions.ConnectionClosed:{e}>{traceback.format_exc()}")
            raise e
        except Exception as e:
            self.logger.error(
                f"get_new_candle exception:{e}->{traceback.format_exc()}")
            raise e
        else:
            response = json.loads(r)
            #self.logger.debug(f"Gemini:{response}")
            return self.__parse_candle_response(response)

    async def initialize_candles(self):
        try:
            r = await self.websocket.recv()
        except websockets.exceptions.ConnectionClosed as e:
            self.logger.error(
                f"websockets.exceptions.ConnectionClosed:{e}>{traceback.format_exc()}")
            raise e
        except Exception as e:
            self.logger.error(
                f"get_new_candle exception:{e}->{traceback.format_exc()}")
            raise e
        else:
            response = json.loads(r)
            #self.logger.debug(f"Gemini:{response}")
            self.__parse_candle_response(response)
         
