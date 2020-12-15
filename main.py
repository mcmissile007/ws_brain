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





CACHE = defaultdict(tuple)
CHANNELS = defaultdict(list)
ALLOWED_CHANNELS = ("USDT_BTC","USDT_LTC","USDT_ETH")


async def notify(pair,action,epoch):
    message = {'type':'signal' ,'action': action,'pair':pair,'epoch': epoch}
    print(f"notify {threading.get_ident()}")
    tasks = [client.send(json.dumps(message)) for client in CHANNELS[pair]]
    if tasks:
        await asyncio.wait(tasks)

async def unsubscribe (websocket):

    for channel, clients in CHANNELS.items():
        if websocket in clients:
            try:
                CHANNELS[channel].remove(websocket)
            except ValueError as e:
                print(f"ValueError in unsubscribe:{e}")
    print(CHANNELS)       

async def subscribe(websocket,pairs):
    # do something with the message
    print(f"subscribe {threading.get_ident()}")
    for pair in pairs:
        CHANNELS[pair].append(websocket)
    print (CHANNELS)



async def incoming_handler(websocket, path):
    print(f"incoming_handler {threading.get_ident()}")
    try :
        async for message in websocket:
            print(f"{websocket} : {path}")
            print(message)
            if path == "/subscribe":
                pairs = json.loads(message)
                print(f"pairs:{pairs}")
                if type(pairs) == list and pairs:
                    print(f"Type list not empty")
                    await subscribe(websocket,pairs)
    except Exception as e:
        print(f"Exception in websocket message:{e}")
        return

async def outgoing_handler(websocket, path):
    print(f"outgoing_handler {threading.get_ident()}")
    message = {'type': 'heartbeat'}
    while True:
        await asyncio.sleep(10)
        print(f"just keeping connection alive!{threading.get_ident()}")
        await websocket.send(json.dumps(message))
        


async def handler(websocket, path):
    print(f"handler {threading.get_ident()}")
   
   

    inconming_task = asyncio.create_task(incoming_handler(websocket, path))
    outgoing_task = asyncio.create_task(outgoing_handler(websocket, path))
    done, pending = await asyncio.wait([inconming_task, outgoing_task],
        return_when=asyncio.FIRST_COMPLETED,
    )

    for task in done:
        try:
            if task is inconming_task:
                print(f"Done inconming_task:{websocket}")
                await unsubscribe(websocket)
            elif task is outgoing_task:
                print("Done outgoing_task")
            else:
                print("?")
        except Exception as e:
                print(f"Exception in done: {e}")

    for task in pending:
        print(f"Canceling:{task}")
        task.cancel()
    
    print("End Handler")


def worker():
    
    print(f"worker {threading.get_ident()}")
    for pair in ALLOWED_CHANNELS:
        CACHE[pair] = ("stay",int(time.time()))
        time.sleep(10)
        print(f"working brain..{pair}..sleeping end")
        guess = random.random()
        print (f"guess:{guess}")
        if guess > 0.8:
            CACHE[pair] = ("sell",int(time.time()))
            print (f"sell:{pair}")
        if guess < 0.2:
            CACHE[pair] = ("buy",int(time.time()))
            print (f"buy:{pair}")


async def brain_N():
    #always must be running catch all execptions
    while True:
        await asyncio.sleep(5) #simulate await new candle from gemmini
        print(f'Brain_N:{CHANNELS}')
        with concurrent.futures.ThreadPoolExecutor() as pool:
                    await asyncio.get_event_loop().run_in_executor(pool,worker)
        for pair in ALLOWED_CHANNELS:
            if CACHE[pair][0] != "stay":
                await notify(pair,CACHE[pair][0],CACHE[pair][1])


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
    print("Start")
    print(f"Start {threading.get_ident()}")


    ws_server = websockets.serve(handler,"localhost",8082)
    tasks = []
    tasks.append(ws_server)
    tasks.append(asyncio.get_event_loop().create_task(brain_N()))
    tasks.append(asyncio.get_event_loop().create_task(webserver()))
    asyncio.get_event_loop().run_until_complete(asyncio.wait(tasks))
    asyncio.get_event_loop().run_forever()

    print("ENDMAIN")