import asyncio
import asyncpg
import aiojsonrpc
import time
from . import websocket,handler,connector_db,connector_cache,node
from .utils import *
import traceback

class Connector:
    def __init__(self,
                 loop,
                 logger,
                 rpc_url,
                 socket_url,
                 client,
                 trace = False,
                 subscribe_blocks = True,
                 subscribe_txs = True,
                 get_receipts = True,
                 connector_db = True,  # use connector database
                 postgresql_dsn = None,  # connector database settings
                 postgresql_pool_max_size=50,  # connector database settings
                 expired_pending_tx_time = DEFAULT_EXPIRED_PENDING_TX_TIME,
                 block_handler_timeout = DEFAULT_BLOCK_HANDLER_TIMEOUT,
                 preload=False,  # initialize blocks preload
                 start_block=None,  # start block height sync, if not specified - use current block
                 rollback_block=None,  # used for long orphans chain
                 tx_handler=None,
                 before_block_handler=None,
                 block_handler=None,
                 orphan_handler=None,
                 pending_tx_update_handler=None,
                 pending_tx_expire_handler = None,
                 cache_load_handler=None,
                 watchdog_sleep=30,
                 network = "ERC20",
                 token = None,
                 redis = None,
                 redis_ttl = 3600
                 ):
        self.loop = loop
        self.log = logger
        self.rpc_url = rpc_url
        self.rpc_timeout = DEFAULT_RPC_TIMEOUT
        self.socket_url = socket_url
        self.client = client
        if client not in CLIENTS:
            raise Exception("client %s is not supported" %client)
        self.trace=False
        if trace:
            if CLIENTS[client]["trace"]:
                self.trace=True
            else:
                logger.warning("trace module for client %s is not supported" % client)
        self.subscribe_blocks = subscribe_blocks
        self.subscribe_txs = subscribe_txs
        self.get_receipts = get_receipts
        self.connector_db = connector_db
        self.postgresql_dsn = postgresql_dsn
        self.postgresql_pool_max_size = postgresql_pool_max_size
        if self.connector_db == False:
            if cache_load_handler is None:
                raise Exception ("please provide cache_load_handler function or set connector_db=True")
        else:
            if not self.postgresql_dsn:
                raise Exception("incorrect postgresql_dsn config")
        self.confirmed_tx_cache = Cache(max_size=DEFAULT_CONFIRMED_TX_CACHE_SIZE)
        self.pending_tx_cache = Cache(max_size=0) #unlimited
        self.block_cache = Cache(max_size=DEFAULT_BLOCK_CACHE_SIZE)
        self.start_block = start_block
        self.rollback_block=rollback_block
        self.tx_handler = tx_handler
        self.before_block_handler = before_block_handler
        self.block_handler = block_handler
        self.block_handler_timeout = block_handler_timeout
        self.orphan_handler = orphan_handler
        self.pending_tx_expire_handler = pending_tx_expire_handler
        self.pending_tx_update_handler= pending_tx_update_handler
        self.cache_load_handler = cache_load_handler
        self.expired_pending_tx_time=expired_pending_tx_time
        self.preload = preload
        self.block_preload_cache = Cache(max_size=DEFAULT_BLOCK_PRELOAD_CACHE_SIZE)
        self.preload_workers = 10

        self.redis = redis
        self.redis_ttl = redis_ttl
        self.watchdog_sleep = watchdog_sleep
        self.active = True
        self.tx_subscription_id = False
        self.block_subscription_id = False
        self.db_pool = False
        if network and token:
            postfix = '_'+network+'_' + token
            self.asset = network+'_' + token
            self.network = network
        else:
            postfix = ''
            self.asset = ''
            self.network = network
        self.block_table = 'connector_block'+postfix
        self.transaction_table = 'connector_transaction'+postfix

        self.active_block = asyncio.Future()
        self.active_block.set_result(True)
        self.active_block_txs = asyncio.Future()
        self.active_block_await_tx_list = list()

        self.tx_in_process = set()

        self.last_block_height = -1
        self.last_preload_block_height = -1
        self.node_last_block = -1

        self.connected = asyncio.Future()
        self.tasks = list()
        self.zmqContext = None
        self.rpc = None
        self.ws = None

        self.log.info("Ethereum node connector started")
        asyncio.ensure_future(self.start(), loop=self.loop)

    async def start(self):
        try:
            if self.connector_db:
                conn=False
                try:
                    conn = await asyncpg.connect(dsn=self.postgresql_dsn)
                    await connector_db.init_db(self,conn)
                    self.log.info("Wait cache loading ...")
                    await connector_db.cache_load_handler(self, conn)
                finally:
                    if conn: await conn.close()
            else:
                self.log.info("Wait cache loading ...")
                self.confirmed_tx_cache,self.pending_tx_cache, self.block_cache = await self.cache_load_handler()
            if self.postgresql_dsn:
                await connector_db.create_pool(self)
            self.last_block_height = connector_cache.get_last_block_height(self)
            self.rpc = aiojsonrpc.rpc(self.rpc_url, self.loop, timeout=self.rpc_timeout)
            health_check = await self.health_check()
            if not health_check:
                raise Exception("Health check failed")
            node_client = await node.check_client(self)
            if self.client not in node_client.lower():
                raise Exception("Incorrect client")
        except Exception as err:
            self.log.error("Start failed")
            self.log.error(str(traceback.format_exc()))
            self.active=False
            await self.stop()
            return
        if self.client.lower()!='tron':
            self.tasks.append(self.loop.create_task(websocket.client(self)))
        else:
            self.tasks.append(self.loop.create_task(websocket.zeromq_handler(self)))
        await asyncio.sleep(1)
        self.tasks.append(self.loop.create_task(self.watchdog_task()))
        if self.preload:
            self.tasks.append(self.loop.create_task(handler.preload_blocks(self)))


    async def health_check(self):
        try:
            await node.health_check(self)
            return True
        except:
            return False

    async def stop(self):
        self.active = False
        self.log.warning("Stopping connector")
        self.log.warning("New block processing restricted")
        self.connected.cancel()
        if not self.active_block.done():
            await self.active_block
        [task.cancel() for task in self.tasks]
        if self.tasks: await asyncio.wait(self.tasks)
        if self.db_pool:
            await self.db_pool.close()
        if self.block_subscription_id:
           try:
               await websocket.unsubscribe_blocks(self)
           except:
               pass
        if self.tx_subscription_id:
            try:
                await websocket.unsubscribe_transactions(self)
            except:
                pass
        if self.zmqContext:
            self.zmqContext.destroy()
        self.log.warning('Connector ready to shutdown')

    async def new_transaction(self, tx_hash, tx = None, block_height = -1, block_time = None):
        if not self.active: return
        if tx_hash in self.tx_in_process: return
        bin_tx_hash=hex_to_bytes(tx_hash)
        tx_timestamp = int(time.time())
        self.tx_in_process.add(tx_hash)
        try:
            tx_cache_confirmed = self.confirmed_tx_cache.get(bin_tx_hash)
            if not tx_cache_confirmed:
                tx_cache = self.pending_tx_cache.get(bin_tx_hash)
                if tx_cache:
                    # pending transaction is rebroadcasted
                    if block_height==-1:
                        await handler.pending_tx_update(self, bin_tx_hash, tx_timestamp, db_pool=self.db_pool)
                        self.pending_tx_cache.set(bin_tx_hash, (-1, tx_timestamp))
                        return
                else:
                    # new transaction
                    if tx is None:
                        try:
                            tx = await node.get_transaction(self,tx_hash)
                            if not(tx_hash == tx["hash"]): raise Exception
                        except:
                            if tx_hash in self.active_block_await_tx_list:
                                raise
                            else:
                                # some pending transactions return None, do not handle it
                                return
                    tx["timestamp"] = tx_timestamp
                    await handler.tx(self, tx, db_pool=self.db_pool)
                    self.pending_tx_cache.set(bin_tx_hash, (-1, tx_timestamp))
            if block_height!=-1:
                if tx_hash in self.active_block_await_tx_list:
                    self.active_block_await_tx_list.remove(tx_hash)
                    if not self.active_block_await_tx_list:
                        self.active_block_txs.set_result(True)
        except Exception as err:
            self.log.error(str(traceback.format_exc()))
            self.log.error("new transaction error %s " % err)
            if tx_hash in self.active_block_await_tx_list:
                self.active_block_await_tx_list = []
                self.active_block_txs.cancel()
        finally:
            self.tx_in_process.remove(tx_hash)


    async def new_block(self, block):
        if not self.active: return
        if not self.active_block.done(): return
        self.active_block = asyncio.Future()
        bin_block_hash = hex_to_bytes(block["hash"])
        bin_previousblock_hash = hex_to_bytes(block["parentHash"]) if "parentHash" in block else None
        start_handle_timestamp = time.time()
        block_height = int(block["number"], 16)
        block_time=int(block['timestamp'],16)
        next_block_height = None
        block_exist = self.block_cache.get(bin_block_hash)
        try:
            if block_exist is not None:
                self.log.debug("block %s already exists %s" %(block_height,block["hash"]))
                return
            if bin_previousblock_hash is not None:
                parent_exist = self.block_cache.get(bin_previousblock_hash)
            else:
                self.log.warning("parent block hash doesnt exist in db %s" % block["parentHash"])
                parent_exist = None
            if self.last_block_height ==-1:
                if self.start_block is not None and block_height > self.start_block:
                    self.log.info("Start from block %s" % self.start_block)
                    next_block_height = self.start_block
                    return
            else:
                if self.last_block_height + 1 > block_height:
                    self.log.info("received old block %s %s" % (block["hash"],block_height))
                    return
                elif self.last_block_height + 1 < block_height:
                    next_block_height = self.last_block_height + 1
                    return
                else: # self.last_block_height + 1 == block_height
                    if self.last_block_height == self.rollback_block:
                        self.log.warning("connector stacked on rollback block %s" % self.rollback_block)
                        next_block_height = None
                        return
                    if parent_exist is None or (self.rollback_block and self.last_block_height>self.rollback_block):
                        orphan_block_height=self.last_block_height
                        if self.confirmed_tx_cache.len() < DEFAULT_CONFIRMED_TX_CACHE_SIZE/10 or self.block_cache.len()<DEFAULT_BLOCK_CACHE_SIZE/10:
                            if self.connector_db:
                                conn = await asyncpg.connect(dsn=self.postgresql_dsn)
                                await connector_db.cache_load_handler(self, conn)
                                await conn.close()
                            else:
                                self.confirmed_tx_cache,self.pending_tx_cache, self.block_cache = await self.cache_load_handler()
                        orphan_bin_block_hash = connector_cache.get_block_hash_by_height(self, orphan_block_height)
                        await handler.before_block(self,block)
                        await handler.orphan(self,orphan_block_height,orphan_bin_block_hash, db_pool=self.db_pool)
                        connector_cache.remove_orphan(self,orphan_block_height,orphan_bin_block_hash)
                        self.log.warning("removed orphan %s hash 0x%s" % (orphan_block_height, hexlify(orphan_bin_block_hash).decode()))
                        if self.redis: await self.redis.delete(hex(orphan_block_height),namespace="%s.eth_getBlockByNumber" % self.network)
                        self.last_block_height -= 1
                        next_block_height = self.last_block_height + 1
                        if self.active_block_txs: self.active_block_txs.cancel()
                        return
            if not 'details' in block:
                block = await node.get_block_by_height(self,block_height)
                if not block: raise Exception('cant get block by height')
            if block["transactions"]:
                self.active_block_txs = asyncio.Future()
                self.active_block_await_tx_list = [tx["hash"] for tx in block["transactions"]]
                for tx in block["transactions"]:
                    self.log.debug("block new transaction %s" % tx["hash"])
                    self.loop.create_task(self.new_transaction(tx["hash"], tx=tx, block_height=block_height, block_time=block_time))
                await asyncio.wait_for(self.active_block_txs, timeout=self.block_handler_timeout)
            self.log.info( "%s transactions [%s] processing time [%s]" % (block_height, len(block["transactions"]), round(time.time() - start_handle_timestamp, 4)))
            await handler.before_block(self, block)
            await handler.block(self, block, db_pool=self.db_pool)
            for tx in block["transactions"]:
                tx_cache = self.pending_tx_cache.pop(hex_to_bytes(tx["hash"]))
                self.confirmed_tx_cache.set(hex_to_bytes(tx["hash"]), (block_height, tx_cache[1]))
            self.block_cache.set(bin_block_hash, block_height)
            self.last_block_height = block_height
            if self.node_last_block > self.last_block_height:
                next_block_height = self.last_block_height+1
            self.log.info( "%s block processing time [%s]" % (block_height, round(time.time() - start_handle_timestamp, 4)))
        except Exception as err:
            self.log.error(str(traceback.format_exc()))
            self.log.error("new block error %s" % str(err))
            self.log.error("await_tx_list %s" % self.active_block_await_tx_list)
            next_block_height = block_height

        finally:
            self.active_block.set_result(True)
            if next_block_height is not None:
                next_block = self.block_preload_cache.pop(next_block_height)
                if not next_block:
                    next_block = await node.get_block_by_height(self,next_block_height)
                if next_block: self.loop.create_task(self.new_block(next_block))


    async def watchdog_task(self):
        while True:
            if not self.active: break
            try:
                while True:
                    try:
                        # 1 check database connection
                        if self.postgresql_dsn:
                            try:
                                await connector_db.ping(self)
                            except:
                                await connector_db.create_pool(self)
                        # 2 enable/disable subsriptions
                        data = await node.get_last_block(self)
                        self.node_last_block = int(data, 16)
                        self.log.info('backlog %s blocks [%s - node, %s - client]' % (self.node_last_block - self.last_block_height, self.node_last_block,self.last_block_height))
                        if self.client.lower() != 'tron':
                            if self.node_last_block > self.last_block_height + 1000:
                                if self.block_subscription_id: await websocket.unsubscribe_blocks(self)
                                if self.tx_subscription_id: await websocket.unsubscribe_transactions(self)
                            else:
                                if not self.block_subscription_id: await websocket.subscribe_blocks(self)
                                if not self.tx_subscription_id: await websocket.subscribe_transactions(self)
                        # 3 check last block
                        if self.node_last_block > self.last_block_height and self.active_block.done():
                            next_block_height = self.last_block_height+1 if self.last_block_height!=-1 else self.node_last_block
                            block = await node.get_block_by_height(self, next_block_height)
                            if block:
                                self.log.info('new block watchdog info %s' % int(block["number"], 16))
                                self.loop.create_task(self.new_block(block))
                        # 4 clear expired pending transactions
                        pendings_expired_hash_list = connector_cache.get_expired_tx(self.pending_tx_cache,self.expired_pending_tx_time)
                        if pendings_expired_hash_list:
                            await handler.pending_tx_expire(self,pendings_expired_hash_list, db_pool=self.db_pool)
                            [self.pending_tx_cache.pop(tx_hash) for tx_hash in pendings_expired_hash_list]
                        await handler.confirmed_tx_expire(self, db_pool=self.db_pool)
                    except:
                        raise
                    finally:
                        await asyncio.sleep(self.watchdog_sleep)
            except asyncio.CancelledError:
                break
            except Exception as err:
                self.log.error(str(traceback.format_exc()))
                self.log.error("watchdog error %s " % err)
