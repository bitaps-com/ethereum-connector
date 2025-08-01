import aiohttp
import json
import asyncio
import traceback
try:
    import zmq
    import zmq.asyncio
except:
    pass
from . import node

async def client(app):
    if not app.socket_url:
        app.log.warning('socket disabled')
        app.connected.set_result(True)
        return
    while True:
        if not app.active: raise asyncio.CancelledError
        try:
            tcp_connector = aiohttp.TCPConnector(verify_ssl=False)
            session = aiohttp.ClientSession(connector=tcp_connector)
            app.ws = await session.ws_connect(app.socket_url, autoclose=True, autoping=True)
            app.connected.set_result(True)
            app.log.info('websocket connected')
            if app.block_subscription_id: await unsubscribe_blocks(app)
            if app.tx_subscription_id: await unsubscribe_transactions(app)
            while True:
                msg = await app.ws.receive()
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                    except ValueError:
                        app.log.warning("Can't parse data")
                        continue
                    try:
                        if "id" in data:
                            if data["id"] == 1:
                                app.block_subscription_id = data["result"]
                                app.log.info("Blocks subscription %s" % data["result"])
                            elif data["id"] == 2:
                                app.tx_subscription_id = data["result"]
                                app.log.info("Transactions subscription %s" % data["result"])
                        if "method" in data:
                            if data["params"]["subscription"] == app.tx_subscription_id:
                                tx_task = app.new_transaction(data["params"]["result"])
                                app.log.debug("websocket new transaction %s" %data["params"]["result"])
                                app.loop.create_task(tx_task)
                            if data["params"]["subscription"] == app.block_subscription_id:
                                block_task = app.new_block(data["params"]["result"])
                                app.log.debug("websocket new block %s" %int(data["params"]["result"]["number"],16))
                                app.loop.create_task(block_task)
                    except Exception as err:
                        app.log.error("error>: %s" % str(err))
                        app.log.error(traceback.format_exc())
                elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
                    app.log.error("websocket error %s" % msg.type.name)
                    app.connected = asyncio.Future()
                    break
        except asyncio.CancelledError:
            await app.ws.close()
            app.log.info("websocket disconnected")
            app.connected = asyncio.Future()
            break
        except Exception as err:
            app.log.error("websocket error %s" % err)
            app.log.error(str(traceback.format_exc()))
            app.connected.cancel()
            app.connected = asyncio.Future()
        finally:
            await session.close()
        await asyncio.sleep(1)


async def zeromq_handler(app):
    if not app.socket_url:
        app.log.warning('socket disabled')
        app.connected.set_result(True)
        return
    while True:
        try:
            app.zmqContext = zmq.asyncio.Context()
            app.zmqSubSocket = app.zmqContext.socket(zmq.SUB)
            app.zmqSubSocket.connect(app.socket_url)
            app.zmqSubSocket.setsockopt_string(zmq.SUBSCRIBE,'blockTrigger')
            app.zmqSubSocket.setsockopt_string(zmq.SUBSCRIBE,'transactionTrigger')
            app.log.info("Zeromq started")
            app.connected.set_result(True)

            while True:
                try:
                    msg = await app.zmqSubSocket.recv_multipart()
                    topic = msg[0]

                    if topic == b"blockTrigger":
                        body = json.loads(msg[1])
                        block = await node.get_block_by_height(app, body["blockNumber"])
                        if block:
                            block_task = app.new_block(block)
                            app.log.debug("websocket new block %s" % body["blockNumber"])
                            app.loop.create_task(block_task)

                    elif topic == b"transactionTrigger":
                        body = json.loads(msg[1])
                        tx_task = app.new_transaction(body["transactionId"])
                        app.log.debug("websocket new transaction %s" % body["transactionId"])
                        app.loop.create_task(tx_task)

                    if not app.active:
                        break
                except asyncio.CancelledError:
                    app.connected = asyncio.Future()
                    app.log.warning("Zeromq handler terminating ...")
                    raise
                except Exception as err:
                    app.connected = asyncio.Future()
                    app.log.error(str(err))

        except asyncio.CancelledError:
            app.zmqContext.destroy()
            app.connected = asyncio.Future()
            app.log.warning("Zeromq handler terminated")
            break
        except Exception as err:
            app.log.error(str(err))
            await asyncio.sleep(1)
            app.connected = asyncio.Future()
            app.log.warning("Zeromq handler reconnecting ...")
        if not app.active:
            app.connected = asyncio.Future()
            app.log.warning("Zeromq handler terminated")
            break

async def subscribe_blocks(app):
    if app.subscribe_blocks:
        await app.ws.send_str('{"jsonrpc":"2.0", "id": 1, "method":"eth_subscribe", "params": ["newHeads"]}')

async def unsubscribe_blocks(app):
    if app.subscribe_blocks:
        await app.ws.send_str('{"jsonrpc":"2.0","id": 3, "method":"eth_unsubscribe", "params": ["%s"]}' % app.block_subscription_id)
        app.tx_subscription_id = None
        app.log.info("Blocks subscription canceled")

async def subscribe_transactions(app):
    if app.subscribe_txs:
        await app.ws.send_str('{"jsonrpc":"2.0","id": 2, "method": "eth_subscribe", "params": ["newPendingTransactions"]}')

async def unsubscribe_transactions(app):
    if app.subscribe_txs:
        await app.ws.send_str('{"jsonrpc":"2.0","id": 4, "method": "eth_unsubscribe", "params": ["%s"]}' % app.tx_subscription_id)
        app.tx_subscription_id = None
        app.log.info("Transactions subscription canceled")