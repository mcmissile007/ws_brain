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
from exchange import Exchange

class Gemini(Exchange):

    def __init__(self, pair: str):

        self.logger = logging.getLogger(
            Config.LOGGING_NAME + "." + str(__name__))
        self.logger.debug(f"Init {str(__name__)}")

        self.websocket = None
        self.pair = pair
        self.candles_type = "candles_5m"


    def __parse_candle_response(self, response: dict) -> dict:

        if type(response) == dict and all( (key in response for key in ['type',"symbol","changes"]) ):
            if response['type'] == self.candles_type + "_updates":
                if response['symbol'] == self.pair:
                    changes = list(response['changes'])
                    return self.__parse_candle(response['symbol'],changes[0])
        else:
            return None
    def __parse_candle(self,pair: str, candle: list) -> dict:
        if type(candle) == list and len(candle) == 6:
            new_candle = {}
            new_candle['source'] = "gemini"
            new_candle['frame'] = self.candles_type.split("_")[1]
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

    async def  connect (self):
        ws_uri = "wss://api.gemini.com/v2/marketdata"
        self.logger.debug("Starting connection with Gemini")
        try:
            self.websocket=  await websockets.client.connect(ws_uri)
            data = {"type": "subscribe","subscriptions":[{"name":self.candles_type,"symbols":[self.pair]}]}
            await self.websocket.send(json.dumps(data))
            return True
        except Exception as e:
            self.logger.error(f"Error connecting to Gemmini: {e}>{traceback.format_exc()}")
            raise e

    async def disconnet(self):
        self.logger.debug("End connection with Gemini")
        try:
            await self.websocket.close()
        except Exception as e:
            self.logger.error(f"Error disconnecting to Gemmini: {e}>{traceback.format_exc()}")
    
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
           

    async def get_tickets(self):
        ws_uri = "wss://api.gemini.com/v2/marketdata"
        while True:
            try:
                self.logger.debug("Starting connection with Gemini")
                async with websockets.connect(ws_uri) as websocket:
                    data = {"type": "subscribe","subscriptions":[{"name":self.candles_type,"symbols":[self.pair]}]}
                    await websocket.send(json.dumps(data))
                    while True:
                        try:
                            r = await websocket.recv()
                        except websockets.exceptions.ConnectionClosed as e:
                            self.logger.error(
                                f"websockets.exceptions.ConnectionClosed:{e}>{traceback.format_exc()}")
                            break
                        except Exception as e:
                            self.logger.error(
                                f"Exception:{e}->{traceback.format_exc()}")
                            break
                        else:
                            response = json.loads(r)
                            #self.logger.debug(f"Gemini:{response}")
                            candle = self.__parse_candle_response(response)
                            if candle is not None:
                                self.logger.debug(f"Gemini candle:{candle}")
                                
            except Exception as e:
                self.logger.error(f"Exception:{e}->{traceback.format_exc()}")

            self.logger.info("Connection lost with Gemini")
            await asyncio.sleep(5)

