import asyncio
from . import node,connector_db
from .utils import *
import traceback

@transaction
async def pending_tx_expire(app, pendings_expired_hash_list, **kwargs):
    conn = kwargs.get("conn", None)
    if app.pending_tx_expire_handler:
        await app.pending_tx_expire_handler(pendings_expired_hash_list, conn=conn, asset = app.asset)
    if app.connector_db:
        await connector_db.pending_tx_expire_handler(app, pendings_expired_hash_list, conn)

@transaction
async def confirmed_tx_expire(app, **kwargs):
    conn = kwargs.get("conn", None)
    if app.connector_db:
        await connector_db.confirmed_tx_expire_handler(app, conn)

@transaction
async def pending_tx_update(app, bin_tx_hash, last_seen_timestamp, **kwargs):
    conn = kwargs.get("conn", None)
    if app.pending_tx_update_handler:
        await app.pending_tx_update_handler(bin_tx_hash, last_seen_timestamp, conn=conn,  asset = app.asset)
    if app.connector_db:
        await connector_db.pending_tx_update_handler(app, bin_tx_hash, last_seen_timestamp, conn)

@transaction
async def tx(app, tx, **kwargs):
    conn = kwargs.get("conn", None)
    tx["handler_result"] = 0
    #avoid duplicates in db
    new = True
    if app.connector_db:
        new = await connector_db.tx_handler(app, tx, conn)
    if app.tx_handler and new:
        await app.tx_handler(tx, conn=conn, asset = app.asset)
        if tx["handler_result"] != 0 and tx["handler_result"] != 1: raise Exception('tx handler error')

@transaction
async def orphan(app,orphan_block_height,orphan_bin_block_hash, **kwargs):
    conn = kwargs.get("conn", None)
    if app.orphan_handler:
        await app.orphan_handler(orphan_block_height, orphan_bin_block_hash, conn=conn, asset = app.asset)
    if app.connector_db:
        await connector_db.orphan_handler(app, orphan_block_height, orphan_bin_block_hash, conn)

@transaction
async def block(app, block, **kwargs):
    conn=kwargs.get("conn",None)
    if app.block_handler:
        await app.block_handler(block, conn=conn, asset = app.asset)
    if app.connector_db:
        await connector_db.block_handler(app, block, conn)

async def before_block(app, block):
    if app.before_block_handler:
        await app.before_block_handler(block, asset = app.asset)

async def preload_blocks(app):
    while True:
        if not app.active: break
        try:
            if app.last_block_height!=-1:
                start_preload_block_height= max(app.last_block_height + 1000, app.last_preload_block_height)
                if app.node_last_block > start_preload_block_height:
                    stop_preload_block_height = min(start_preload_block_height + max(DEFAULT_BLOCK_PRELOAD_CACHE_SIZE - app.block_preload_cache.len(),0),app.node_last_block)
                    if stop_preload_block_height>start_preload_block_height+1000:
                        app.log.info("start preloading block task from %s to %s" % (start_preload_block_height,stop_preload_block_height))
                        preload_tasks=[]
                        for i in range(app.preload_workers):
                            preload_tasks.append(app.loop.create_task(preload_blocks_worker(app,i, start_preload_block_height+i,stop_preload_block_height)))
                        if preload_tasks: await asyncio.wait(preload_tasks)
                        app.log.info("DONE preloading block task from %s to %s" % (start_preload_block_height, stop_preload_block_height))
        except asyncio.CancelledError:
            app.log.info("connector preload_block terminated")
            break
        except:
            app.log.error(str(traceback.format_exc()))
        await asyncio.sleep(5)

async def preload_blocks_worker(app, i, start_preload_block_height,stop_preload_block_height):
    preload_block_height = start_preload_block_height
    while True:
        if not app.active: raise asyncio.CancelledError
        if preload_block_height < app.last_block_height:
            preload_block_height += app.preload_workers
            continue
        ex = app.block_preload_cache.get(preload_block_height)
        if ex:
            preload_block_height += app.preload_workers
        else:
            app.log.debug('preload block height %s worker %s' % (preload_block_height, i))
            try:
                block = await node.get_block_by_height(app, preload_block_height)
            except:
                block=None
                app.log.error("preload worker can't receive block height %s" %preload_block_height)
            if block:
                app.block_preload_cache.set(preload_block_height, block)
                app.last_preload_block_height = max(app.last_preload_block_height, preload_block_height)
                preload_block_height += app.preload_workers
        if preload_block_height > stop_preload_block_height:
            break

