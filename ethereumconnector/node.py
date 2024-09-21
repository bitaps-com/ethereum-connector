import asyncio
from .utils import *
import traceback

async def health_check(app):
    try:
        return await app.rpc.eth_syncing()
    except Exception:
        app.log.error(str(traceback.format_exc()))
        app.log.error("Health check failed")
        raise

async def check_client(app):
    try:
        return await app.rpc.web3_clientVersion()
    except Exception:
        app.log.error(str(traceback.format_exc()))
        app.log.error("client check failed")
        raise

async def get_last_block(app):
    try:
        return await app.rpc.eth_blockNumber()
    except Exception:
        app.log.error(str(traceback.format_exc()))
        app.log.error("Get last_block failed")
        raise

async def get_transaction(app, tx_hash):
    try:
        data = await app.redis.get(tx_hash, namespace="%s.eth_getTransactionByHash" %app.network) if app.redis else None
        if not data:
            data = await app.rpc.eth_getTransactionByHash(tx_hash)
            if app.redis: await app.redis.set(tx_hash, data, ttl=3600*6, namespace="%s.eth_getTransactionByHash" %app.network)
        return data
    except Exception:
        app.log.error(str(traceback.format_exc()))
        app.log.error("Get transaction %s failed" % tx_hash)
        raise

async def get_block_uncles(app, block_hash, index):
    try:
        return await app.rpc.eth_getUncleByBlockHashAndIndex(block_hash, hex(index))
    except Exception:
        app.log.error(str(traceback.format_exc()))
        app.log.error("Get uncle %s failed" % block_hash)
        raise

async def get_transaction_receipt(app, tx_hash):
    try:
        data = await app.redis.get(tx_hash, namespace="%s.eth_getTransactionReceipt" %app.network) if app.redis else None
        if not data:
            data = await app.rpc.eth_getTransactionReceipt(tx_hash)
            if app.redis: await app.redis.set(tx_hash, data, ttl=3600*6, namespace="%s.eth_getTransactionReceipt" % app.network)
        return data
    except Exception:
        app.log.error(str(traceback.format_exc()))
        app.log.error("Get get_transaction_receipt %s failed" % tx_hash)
        raise

async def get_block_by_height(app, block_height):
    try:
        data = await app.redis.get(hex(block_height), namespace="%s.eth_getBlockByNumber" % app.network) if app.redis else None
        if not data:
            block = await app.rpc.eth_getBlockByNumber(hex(block_height), True)
            if block is None:
                await asyncio.sleep(1)
            else:
                if not(block["number"] == hex(block_height)): raise Exception
                await get_block_trace_and_receipt(app,block_height, block["hash"], block["transactions"])
                uncles_data = []
                if block["uncles"]:
                    for index in range(len(block["uncles"])):
                        u_data = await get_block_uncles(app,block["hash"], index)
                        uncles_data.append(u_data)
                block['uncles_data'] = uncles_data
                block['details'] = True
            data = block
            if app.redis: await app.redis.set(hex(block_height), data, ttl=3600*6, namespace="%s.eth_getBlockByNumber" % app.network)
        return data
    except Exception:
        app.log.error(str(traceback.format_exc()))
        app.log.error("Get block by height %s failed" % block_height)
        raise

async def get_block_trace_and_receipt(app, block_height, block_hash, transactions):
    if transactions:
        trace = {}
        if app.trace:
            block_trace = await app.rpc.trace_block(hex(block_height))
            if not (block_trace[0]['blockHash'] == block_hash):
                raise Exception ('block trace hash %s block hash %s' %(block_trace[0]['blockHash'],block_hash))
            for tx in block_trace:
                if 'author' in tx['action']:continue
                if not tx['transactionHash'] in trace: trace[tx['transactionHash']] = list()
                trace[tx['transactionHash']].append(tx)
        receipt = {}
        if app.get_receipts:
            if CLIENTS[app.client]["getBlockReceipts"]["method"]:
                func_name = CLIENTS[app.client]["getBlockReceipts"]["method"]
                func = getattr(app.rpc, func_name)
                if CLIENTS[app.client]["getBlockReceipts"]["params"] == "height":
                    block_receipt = await func(hex(block_height))
                elif CLIENTS[app.client]["getBlockReceipts"]["params"] == "hash":
                    block_receipt = await func(block_hash)
                else:
                    CLIENTS[app.client]["getBlockReceipts"]["method"] = None
                    raise Exception('unknown getBlockReceipts functions setup params for %s client' % app.client)
                if not (block_receipt[0]['blockHash'] == block_hash):
                    raise Exception('block receipt hash %s block hash %s' % (block_receipt[0]['blockHash'], block_hash))
            else:
                block_receipt = []
                tx_receipt_tasks = [app.loop.create_task(get_transaction_receipt(app,tx["hash"])) for tx in transactions]
                done, pending = await asyncio.wait(tx_receipt_tasks, return_when=asyncio.FIRST_EXCEPTION)
                if pending: raise
                for future in done: block_receipt.append(future.result())
            for tx in block_receipt:
                if not tx['transactionHash'] in receipt:receipt[tx['transactionHash']] = {}
                receipt[tx['transactionHash']] = tx
        for tx in transactions:
            if tx['hash'] in receipt:
                tx.update(receipt[tx['hash']])
            if 'status' in tx:
                tx['status']= int(tx['status'],16)
            else:
                tx['status'] = 1
            if tx['hash'] in trace:
                tx['trace'] = trace[tx['hash']]
                if 'error' in trace[tx['hash']][0]:
                    if tx['status']==1:
                        app.log.warning("tx %s with status OK, but error during tracing" %tx["hash"])
                        tx['status'] = 0




