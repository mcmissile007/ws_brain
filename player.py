import asyncio
import websockets
import traceback
import json

async def main():
    uri = "ws://localhost:8082/subscribe"
    async with websockets.connect(uri) as websocket:
        await websocket.send(json.dumps(["USDT_ETH","USDT_BTC"]))
        while True:
            try:
                r = await websocket.recv()
            except websockets.exceptions.ConnectionClosed as e:
                print(f"websockets.exceptions.ConnectionClosed:{e}>{traceback.format_exc()}")
                break
            except Exception as e:
                print(f"Exception:{e}->{traceback.format_exc()}")
                break
            else:
                #response = json.loads(r)
                print(f"response:{r}")



if __name__ == "__main__":
    print("Start player")
    asyncio.run(main())