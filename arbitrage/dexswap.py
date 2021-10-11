import math
from concurrent.futures import ThreadPoolExecutor, wait
import time
import os
import sys
from os import path   
file_path = os.path.dirname(os.path.realpath(__file__))
parent_path = os.path.dirname(file_path)
sys.path.append(parent_path)
from arbitrage.config import *
from brownie import *
from typing import Any
from arbitrage.logger import Logger
from arbitrage.abi import *
from arbitrage.calladmin import *
import json
from waiting import wait as wait_conf
import func_timeout
from func_timeout import func_set_timeout
import traceback
from web3.exceptions import TransactionNotFound
import threading
from web3 import Web3

lock = threading.Lock()

class DexSwap(object):
    def __init__(self, _log, login=True):
        self.init_gas_price = 1000000000000
        self.gas_price = 1500000000000
        self.max_gas_price = 4000000000000

        self.init(login)
        self.log = _log
        self.precisions = self.get_precision()
        self.prices = {}
        self.balances = {}
        self.reserves = {}
        self.bal_reduced = {}
        self.reset_bal_reduce()
        self.reward_percent = {}
        self.last_update_reward = 0
        self.last_order_time = int(time.time())
        self.broadcast_finish = True
        self.spiex_multi_pairs = self.find_multi_paths('SPIEX')
        self.spooky_multi_pairs = self.find_multi_paths('SPKY')
        self.sushi_multi_pairs = self.find_multi_paths('SUSHIEX')
        # self.w3 = Web3(Web3.WebsocketProvider('wss://wsapi.fantom.network'))
        # self.w3 = Web3(Web3.HTTPProvider('https://rpc.neist.io'))
        # self.w3 = Web3(Web3.HTTPProvider('https://nameless-white-cloud.fantom.quiknode.pro/e4fca9b59eee4c6ea1a7cc0fdc5c2b21171bf35a/'))
        self.w3 = Web3(Web3.WebsocketProvider('wss://nameless-white-cloud.fantom.quiknode.pro/e4fca9b59eee4c6ea1a7cc0fdc5c2b21171bf35a/'))

    def init(self, login):
        # self.netid='ftm_neist'
        self.netid='quicknode'
        # self.netid='ftm2'
        network.connect(self.netid)
        network.gas_price(self.gas_price)
        network.gas_limit(2100000)
        accounts.clear()
        if login:
            print('Enter controller password:')
            accounts.load('arbitrage.json')
            self.acct = accounts[0]
        else:
            accounts.at('0x9D945d909Ca91937d19563e30bB4DAc12C860189', force=True)
            self.acct = accounts[0]

        router_abi_obj = json.loads(router_abi)
        self.spiex_router = Contract.from_abi("Router", spirit_router_address, router_abi_obj)
        self.spooky_router = Contract.from_abi("Router", spooky_router_address, router_abi_obj)
        self.sushi_router = Contract.from_abi("Router", sushi_router_address, router_abi_obj)
        abi = json.loads(query_abi)
        self.query = Contract.from_abi("Query", '0x7CAE4F73eAFc482efA0d02205C6e6E71c3cdcEEd', abi)
        
    def reconnect(self):
        self.broadcast_finish = True
        network.disconnect()
        network.connect(self.netid)
        network.gas_price(self.gas_price)
        network.gas_limit(2100000)

    def get_precision(self):
        precisions = {}
        for asset in assets:
            if asset == 'BTC':
                precisions[asset] = 8
            elif asset == 'USDC':
                precisions[asset] = 6
            elif asset == 'USDT':
                precisions[asset] = 6
            else:
                precisions[asset] = 18
        return precisions


    def find_multi_paths(self, dexname):
        all_pairs = []
        if dexname == 'SPIEX':
            all_pairs = spirit_all_pairs
        elif dexname == 'SPKY':
            all_pairs = spooky_pairs
        elif dexname == 'SUSHIEX':
            all_pairs = sushi_pairs

        all_pairs = [pair.replace(dexname, '') for pair in all_pairs]
        multi_pairs = []
        nl = len(all_pairs)
        while(nl > 1):
            first_pair = all_pairs[0]
            all_pairs = all_pairs[1:]
            nl = len(all_pairs)
            first_pair_symbols = first_pair.split('_')
            symbol0 = ''
            symbol1 = ''
            for pair in all_pairs:
                symbols = pair.split('_')
                m_pair = ''
                if first_pair_symbols[0] == symbols[0]:
                    m_pair = first_pair_symbols[1] + '_' + symbols[0] + '_' + symbols[1]
                    m_pair_r = symbols[1] + '_' + symbols[0] + '_' + first_pair_symbols[1]
                    symbol0 = first_pair_symbols[1]
                    symbol1 = symbols[1]
                    if symbols[1] == 'FTM':
                        m_pair, m_pair_r = m_pair_r, m_pair
                elif first_pair_symbols[0] == symbols[1]:
                    m_pair = first_pair_symbols[1] + '_' + symbols[1] + '_' + symbols[0]
                    m_pair_r = symbols[0] + '_' + symbols[1] + '_' + first_pair_symbols[1]
                    symbol0 = first_pair_symbols[1]
                    symbol1 = symbols[0]
                    if symbols[0] == 'FTM':
                        m_pair, m_pair_r = m_pair_r, m_pair
                elif first_pair_symbols[1] == symbols[0]:
                    m_pair = first_pair_symbols[0] + '_' + symbols[0] + '_' + symbols[1]
                    m_pair_r = symbols[1] + '_' + symbols[0] + '_' + first_pair_symbols[0]
                    symbol0 = first_pair_symbols[0]
                    symbol1 = symbols[1]
                    if symbols[1] == 'FTM':
                        m_pair, m_pair_r = m_pair_r, m_pair
                elif first_pair_symbols[1] == symbols[1]:
                    m_pair = first_pair_symbols[0] + '_' + symbols[1] + '_' + symbols[0]
                    m_pair_r = symbols[0] + '_' + symbols[1] + '_' + first_pair_symbols[0]
                    symbol0 = first_pair_symbols[0]
                    symbol1 = symbols[0]
                    if symbols[0] == 'FTM':
                        m_pair, m_pair_r = m_pair_r, m_pair
                if symbol0 not in assets or symbol1 not in assets or m_pair == '':
                    continue
                if m_pair not in multi_pairs and m_pair_r not in multi_pairs:
                    multi_pairs.append(m_pair)
        return multi_pairs

    def set_low_gas_price(self):
        network.gas_price(2010000000)
        self.gas_price = 10000000000
        network.gas_limit(210000)

    def asset_balance(self, asset):
        if asset == 'FTM':
            return self.balances[asset] - self.bal_reduced[asset] - 1000 * 10 ** 18
        return self.balances[asset] - self.bal_reduced[asset]

    def reset_bal_reduce(self):
        for asset in assets:
            self.bal_reduced[asset] = 0

    def get_borrow_balance(self, asset):
        return 0

    # @func_set_timeout(3)
    # def update_information(self):
    #     asset_addrs = [eval(asset) for asset in assets[1:]]
    #     pairs = [eval(pair+'SPIEX') for pair in spirit_all_pairs]
    #     pairs += [eval(pair) for pair in spooky_pairs]
    #     pairs += [eval(pair) for pair in sushi_pairs]
    #     (reserves, balances) = self.query.get_all_information(self.acct, asset_addrs, pairs)
    #     for i in range(len(assets)):
    #         asset = assets[i]
    #         free_balance = balances[i]
    #         free_balance = free_balance * 10 ** (18 - self.precisions[asset])
    #         self.balances[assets[i]] = free_balance

    #     i = 0
    #     for pair in spirit_all_pairs:
    #         symbols = pair.split("_")
    #         if symbols[0] == 'FTM':
    #             base_address = WFTM
    #         else:
    #             base_address = eval(symbols[0])
    #         if symbols[1] == 'FTM':
    #             quote_address = WFTM
    #         else:
    #             quote_address = eval(symbols[1])
    #         self.reserves[pair+'SPIEX'] = (reserves[i][0], reserves[i][1]) if int(base_address, 16) < int(quote_address, 16) else (reserves[i][1], reserves[i][0])
    #         i += 1

    #     for pair in spooky_pairs:
    #         pair = pair.replace('SPKY', '')
    #         symbols = pair.split("_")
    #         if symbols[0] == 'FTM':
    #             base_address = WFTM
    #         else:
    #             base_address = eval(symbols[0])
    #         if symbols[1] == 'FTM':
    #             quote_address = WFTM
    #         else:
    #             quote_address = eval(symbols[1])
    #         self.reserves[pair+'SPKY'] = (reserves[i][0], reserves[i][1]) if int(base_address, 16) < int(quote_address, 16) else (reserves[i][1], reserves[i][0])
    #         i += 1

    #     for pair in sushi_pairs:
    #         pair = pair.replace('SUSHIEX', '')
    #         symbols = pair.split("_")
    #         if symbols[0] == 'FTM':
    #             base_address = WFTM
    #         else:
    #             base_address = eval(symbols[0])
    #         if symbols[1] == 'FTM':
    #             quote_address = WFTM
    #         else:
    #             quote_address = eval(symbols[1])
    #         self.reserves[pair+'SUSHIEX'] = (reserves[i][0], reserves[i][1]) if int(base_address, 16) < int(quote_address, 16) else (reserves[i][1], reserves[i][0])
    #         i += 1

    @func_set_timeout(3)
    def update_information(self):
        abi = json.loads(query_abi)
        query = self.w3.eth.contract('0x7CAE4F73eAFc482efA0d02205C6e6E71c3cdcEEd', abi=abi)
        asset_addrs = [eval(asset) for asset in assets[1:]]
        pairs = [eval(pair+'SPIEX') for pair in spirit_all_pairs]
        pairs += [eval(pair) for pair in spooky_pairs]
        pairs += [eval(pair) for pair in sushi_pairs]
        (reserves, balances) = query.functions.get_all_information('0x9D945d909Ca91937d19563e30bB4DAc12C860189', asset_addrs, pairs).call()
        for i in range(len(assets)):
            asset = assets[i]
            free_balance = balances[i]
            free_balance = free_balance * 10 ** (18 - self.precisions[asset])
            self.balances[assets[i]] = free_balance

        i = 0
        for pair in spirit_all_pairs:
            symbols = pair.split("_")
            if symbols[0] == 'FTM':
                base_address = WFTM
            else:
                base_address = eval(symbols[0])
            if symbols[1] == 'FTM':
                quote_address = WFTM
            else:
                quote_address = eval(symbols[1])
            self.reserves[pair+'SPIEX'] = (reserves[i][0], reserves[i][1]) if int(base_address, 16) < int(quote_address, 16) else (reserves[i][1], reserves[i][0])
            i += 1

        for pair in spooky_pairs:
            pair = pair.replace('SPKY', '')
            symbols = pair.split("_")
            if symbols[0] == 'FTM':
                base_address = WFTM
            else:
                base_address = eval(symbols[0])
            if symbols[1] == 'FTM':
                quote_address = WFTM
            else:
                quote_address = eval(symbols[1])
            self.reserves[pair+'SPKY'] = (reserves[i][0], reserves[i][1]) if int(base_address, 16) < int(quote_address, 16) else (reserves[i][1], reserves[i][0])
            i += 1

        for pair in sushi_pairs:
            pair = pair.replace('SUSHIEX', '')
            symbols = pair.split("_")
            if symbols[0] == 'FTM':
                base_address = WFTM
            else:
                base_address = eval(symbols[0])
            if symbols[1] == 'FTM':
                quote_address = WFTM
            else:
                quote_address = eval(symbols[1])
            self.reserves[pair+'SUSHIEX'] = (reserves[i][0], reserves[i][1]) if int(base_address, 16) < int(quote_address, 16) else (reserves[i][1], reserves[i][0])
            i += 1

    def get_reserve(self, base_asset, quote_asset):
        return self.reserves[base_asset + '_' + quote_asset]

    def get_price(self, base_asset, quote_asset, side, input_amount, dex_name):
        try:
            if side.lower() == 'buy':
                real_input_amount = input_amount
                _quote_asset = quote_asset.replace(dex_name, '')
                real_input_amount = int(input_amount / 10 ** (18 - self.precisions[_quote_asset]))
                output_amt = self.getOutputAmount(real_input_amount, quote_asset, base_asset, dex_name)
                output_amt = output_amt * 10 ** (18 - self.precisions[base_asset])
                return input_amount / output_amt
            elif side.lower() == 'sell':
                real_input_amount = input_amount
                real_input_amount = int(input_amount / 10 ** (18 - self.precisions[base_asset]))
                output_amt = self.getOutputAmount(real_input_amount, base_asset, quote_asset, dex_name)
                _quote_asset = quote_asset.replace(dex_name, '')
                output_amt = output_amt * 10 ** (18 - self.precisions[_quote_asset])
                return output_amt / input_amount
        except ZeroDivisionError:
            print('ZeroDivisionError:', base_asset, quote_asset, side, input_amount, dex_name)

    def _timestamp(self):
        return int(time.time())

    def reset_gas_price(self):
        if self.gas_price > self.init_gas_price:
            self.gas_price = self.init_gas_price
            network.gas_price(self.gas_price)

    @func_set_timeout(80)
    def transfer(self, to, amount, call_data):
        tx = self.acct.transfer(to, amount, data=call_data, required_confs=0)
        print('transfer send succeed')
        return tx

    @func_set_timeout(80)
    def wait(self, tx):
        tx.wait(1)

    # @func_set_timeout(28)
    def sell(self, base_asset, quote_asset, amount_in, min_amount_out):
        amount_in = int(amount_in)
        org_amount_in = amount_in
        amount_out = 0
        gasfee = 0
        succeed = False
        tx = None
        dexname=''
        router = None
        try:
            if 'SPIEX' in base_asset or 'SPIEX' in quote_asset:
                router = self.spiex_router
                router_addr = spirit_router_address
                dexname = 'SPIEX'
            elif 'SPKY' in base_asset or 'SPKY' in quote_asset:
                router = self.spooky_router
                router_addr = spooky_router_address
                dexname = 'SPKY'
            elif 'SUSHIEX' in base_asset or 'SUSHIEX' in quote_asset:
                router = self.sushi_router
                router_addr = sushi_router_address
                dexname = 'SUSHIEX'

            amount_in = amount_in // 10 ** (18 - self.precisions[base_asset])
            base_asset = base_asset.replace(dexname, '')
            quote_asset = quote_asset.replace(dexname, '')
            amount_out = self.getOutputAmount(amount_in, base_asset, quote_asset, dexname)
            if amount_out * 10 ** (18 - self.precisions[quote_asset]) < min_amount_out * 9999 / 10000:
                raise Exception('current amout_out is {:.4f}, min_amount_out is {:.4f}'.format(amount_out / 10 ** (self.precisions[quote_asset]), min_amount_out / 10 ** 18))
            org_amount_out = amount_out
            amount_out = int(amount_out * slippage_numerator / slippage_senominator)
            eth_amount = 0
            if quote_asset == 'FTM':
                path = [eval(base_asset), WFTM]
                calldata = router.swapExactTokensForETH.encode_input(amount_in, amount_out, path, self.acct, self._timestamp() + 60)
            elif base_asset == 'FTM':
                path = [WFTM, eval(quote_asset)]
                calldata = router.swapExactETHForTokens.encode_input(amount_out, path, self.acct, self._timestamp() + 60)
                eth_amount = amount_in
            else:
                path = [eval(base_asset), eval(quote_asset)]
                calldata = router.swapExactTokensForTokens.encode_input(amount_in, amount_out, path, self.acct, self._timestamp() + 60)
            with lock:
                if not self.broadcast_finish:
                    raise Exception('current has timeout transaction')
                try:
                    tx = self.transfer(router_addr, eth_amount, calldata)
                    self.broadcast_finish = True
                except func_timeout.exceptions.FunctionTimedOut:
                    self.broadcast_finish = False
                    print('{} sell {} time out'.format(dexname, base_asset + quote_asset))
                    self.reconnect()
                    time.sleep(2)
                    return
            self.wait(tx)
            if tx.gas_price is None or tx.gas_used is None:
                print('tx.gas_price or tx.gas_used is none, txid={}'.format(tx.txid))
            if tx.status == -1:
                self.broadcast_finish = False
                self.log.logger.info('{} dex sell fail: {} pending timeout, txid={}'.format(dexname, base_asset + '_' + quote_asset, tx.txid))
                raise Exception('time out')
            self.tx_time_out_cnt = 0
            if tx.status == 0:
                raise Exception('revert')
            print('gas_used={}, gas_used={}'.format(tx.gas_used, tx.gas_used))
            gas_used = 0 if tx.gas_price is None else tx.gas_price
            gas_used = 0 if tx.gas_used is None else tx.gas_used
            gasfee = tx.gas_price * tx.gas_used / 10 ** 18
            amount_out = org_amount_out / 10 ** 18
            self.log.logger.info('{} dex sell {} txid: {}'.format(dexname, base_asset + '_' + quote_asset, tx.txid))
            self.log.logger.info('{} dex sell {}: amount_in={:.4f}, amount_out={:.4f}'.format(dexname, base_asset, org_amount_in / 10 ** 18, amount_out))
            succeed = True
            self.last_order_time = int(time.time())
            self.reset_gas_price()
        except Exception as e:
            # traceback.print_exc()
            self.log.logger.info('dex sell fail: {}'.format(str(e)))
            if 'underpriced' in str(e):
                self.gas_price = self.gas_price * 1.1
                print('new gas price={}'.format(self.gas_price))
                if self.gas_price > self.max_gas_price:
                    self.gas_price = self.max_gas_price / 2
                network.gas_price(self.gas_price)
            gasfee = 33834 * self.gas_price / 10 ** 18
            amount_out = 0
            succeed = False
            if hasattr(e, 'txid'):
                tx = chain.get_transaction(e.txid)
                gasfee = tx.gas_price * tx.gas_used / 10 ** 18
        except func_timeout.exceptions.FunctionTimedOut:
            print('{} sell {} time out'.format(dexname, base_asset + quote_asset))
            time.sleep(2)
            self.reconnect()
        finally:
            return succeed, amount_out, gasfee

    def buy(self, base_asset, quote_asset, amount_in, min_amount_out):
        amount_in = int(amount_in)
        amount_out = 0
        gasfee = 0
        succeed = False
        tx = None
        dexname=''
        router = None
        try:
            if 'SPIEX' in base_asset or 'SPIEX' in quote_asset:
                router = self.spiex_router
                router_addr = spirit_router_address
                dexname = 'SPIEX'
            elif 'SPKY' in base_asset or 'SPKY' in quote_asset:
                router = self.spooky_router
                router_addr = spooky_router_address
                dexname = 'SPKY'
            elif 'SUSHIEX' in base_asset or 'SUSHIEX' in quote_asset:
                router = self.sushi_router
                router_addr = sushi_router_address
                dexname = 'SUSHIEX'

            base_asset = base_asset.replace(dexname, '')
            quote_asset = quote_asset.replace(dexname, '')
            amount_in = amount_in // 10 ** (18 - self.precisions[quote_asset])
            amount_out = self.getOutputAmount(amount_in, quote_asset, base_asset, dexname)
            if amount_out * 10 ** (18 - self.precisions[base_asset]) < min_amount_out * 9999 / 10000:
                raise Exception('current amout_out is {:.4f}, min_amount_out is {:.4f}'.format(amount_out / 10 ** (self.precisions[base_asset]), min_amount_out / 10 ** 18))
            org_amount_out = amount_out
            amount_out = int(amount_out * slippage_numerator / slippage_senominator)
            if quote_asset == 'FTM':
                path = [WFTM, eval(base_asset)]
                calldata = router.swapExactETHForTokens.encode_input(amount_out, path, self.acct, self._timestamp() + 60)
                eth_amount = amount_in
            elif base_asset == 'FTM':
                path = [eval(quote_asset), WFTM]
                calldata = router.swapExactTokensForETH.encode_input(amount_in, amount_out, path, self.acct, self._timestamp() + 60)
                eth_amount = 0
            else:
                path = [eval(quote_asset), eval(base_asset)]
                calldata = router.swapExactTokensForTokens.encode_input(amount_in, amount_out, path, self.acct, self._timestamp() + 60)
                eth_amount = 0
            with lock:
                if not self.broadcast_finish:
                    raise Exception('current has timeout transaction')
                try:
                    tx = self.transfer(router_addr, eth_amount, calldata)
                    self.broadcast_finish = True
                except func_timeout.exceptions.FunctionTimedOut:
                    self.broadcast_finish = False
                    print('{} sell {} time out'.format(dexname, base_asset + quote_asset))
                    self.reconnect()
                    time.sleep(2)
                    return
            # (ret, tx) = wait_tx_confirmed(tx)
            self.wait(tx)
            # tx.wait(1)
            if tx.gas_price is None or tx.gas_used is None:
                print('tx.gas_price or tx.gas_used is none, txid={}'.format(tx.txid))
            if tx.status == -1:
                self.broadcast_finish = False
                self.log.logger.info('{} dex buy fail: {} pending timeout, txid={}'.format(dexname, base_asset + '_' + quote_asset, tx.txid))
                raise Exception('time out')
            self.tx_time_out_cnt = 0
            if tx.status == 0:
                raise Exception('revert')
            print('gas_used={}, gas_used={}'.format(tx.gas_used, tx.gas_used))
            gas_used = 0 if tx.gas_price is None else tx.gas_price
            gas_used = 0 if tx.gas_used is None else tx.gas_used
            gasfee = tx.gas_price * tx.gas_used / 10 ** 18
            amount_out = org_amount_out / 10 ** self.precisions[base_asset]            
            self.log.logger.info('{} dex buy {} txid: {}'.format(dexname, base_asset + '_' + quote_asset, tx.txid))
            self.log.logger.info('{} dex buy {}: amount_in={:.4f}, amount_out={:.4f}'.format(dexname, base_asset, amount_in / 10 ** 18, amount_out))
            succeed = True
            self.last_order_time = int(time.time())
            self.reset_gas_price()
        except Exception as e:
            # traceback.print_exc()
            self.log.logger.info('dex buy fail: {}'.format(str(e)))
            if 'underpriced' in str(e):
                self.gas_price = self.gas_price * 1.1
                print('new gas price={}'.format(self.gas_price))
                if self.gas_price > self.max_gas_price:
                    self.gas_price = self.max_gas_price / 2
                network.gas_price(self.gas_price)
            gasfee = 33834 * self.gas_price / 10 ** 18
            amount_out = 0
            succeed = False
            if hasattr(e, 'txid'):
                tx = chain.get_transaction(e.txid)
                gasfee = tx.gas_price * tx.gas_used / 10 ** 18
        except func_timeout.exceptions.FunctionTimedOut:
            print('{} sell {} time out'.format(dexname, base_asset + quote_asset))
            time.sleep(2)
            self.reconnect()
        finally:
            return succeed, amount_out, gasfee

    def extGetOutputAmount(self, amountIn, tokenIn, tokenOut, dex_name):
        asset_in = tokenIn.replace(dex_name, '')
        real_input_amount = int(amountIn / 10 ** (18 - self.precisions[asset_in]))
        output_amt = self.getOutputAmount(real_input_amount, tokenIn, tokenOut, dex_name)
        asset_out = tokenOut.replace(dex_name, '')
        output_amt = output_amt * 10 ** (18 - self.precisions[asset_out])
        return output_amt

    def getOutputAmount(self, amountIn, tokenIn, tokenOut, dex_name):
        ti = tokenIn.replace(dex_name, '')
        to = tokenOut.replace(dex_name, '')
        if ti+'_'+to+dex_name in self.reserves:
            reserve = self.reserves[ti+'_'+to+dex_name]
            reserveIn = reserve[0]
            reserveOut = reserve[1]
        elif to+'_'+ti+dex_name in self.reserves:
            reserve = self.reserves[to+'_'+ti+dex_name]
            reserveIn = reserve[1]
            reserveOut = reserve[0]
        else:
            print(self.reserves)
            print(ti, to)
            print('xxxx')
            return 0
        assert amountIn > 0 and reserveIn > 0 and reserveOut > 0
        if dex_name == 'SPIEX' or dex_name == 'SUSHIEX':
            fee_rate = 9970
        elif dex_name == 'SPKY':
            fee_rate = 9980
        else:
            print('SSSS dexname {} is not found'.format(dex_name))
            raise
        amountInWithFee = amountIn * fee_rate
        numerator = amountInWithFee * reserveOut
        denominator = reserveIn * 10000 + amountInWithFee
        amountOut = numerator // denominator
        return amountOut

    def binary_search(self, base_asset, quote_asset, current_commitment, side, price_limit, dex_name):
        max_search = 100
        max_current_commitment = current_commitment
        next_step_size = int(current_commitment / 2)
        for i in range(max_search):
            if next_step_size < 1000000:
                break
            if side == 'buy':
                price = self.get_price(base_asset, quote_asset, side, current_commitment, dex_name)
                if price > price_limit:
                    current_commitment = current_commitment - next_step_size
                    next_step_size = int(next_step_size / 2)
                    continue
                elif price < search_stop_threshold * price_limit:
                    current_commitment = current_commitment + next_step_size
                    next_step_size = int(next_step_size / 2)
                    continue
                else:
                    break
            else:
                price = self.get_price(base_asset, quote_asset, side, current_commitment, dex_name)
                if price < price_limit:
                    current_commitment = current_commitment - next_step_size
                    next_step_size = int(next_step_size / 2)
                    continue
                elif price > price_limit / search_stop_threshold:
                    current_commitment = current_commitment + next_step_size
                    next_step_size = int(next_step_size / 2)
                    continue
                else:
                    break
        ret_current_commitment = min(max_current_commitment, current_commitment)
        return price, ret_current_commitment

    # @func_set_timeout(28)
    def sell_multi_hop(self, base_asset, mid_asset, quote_asset, amount_in, min_amount_out):
        amount_in = int(amount_in)
        org_amount_in = amount_in
        amount_out = 0
        gasfee = 0
        succeed = False
        tx = None
        dexname=''
        router = None
        try:
            if 'SPIEX' in base_asset or 'SPIEX' in quote_asset:
                router = self.spiex_router
                router_addr = spirit_router_address
                dexname = 'SPIEX'
            elif 'SPKY' in base_asset or 'SPKY' in quote_asset:
                router = self.spooky_router
                router_addr = spooky_router_address
                dexname = 'SPKY'
            elif 'SUSHIEX' in base_asset or 'SUSHIEX' in quote_asset:
                router = self.sushi_router
                router_addr = sushi_router_address
                dexname = 'SUSHIEX'
            else:
                raise Exception('sell_multi_hop do not have dexname')

            amount_in = amount_in // 10 ** (18 - self.precisions[base_asset])
            base_asset = base_asset.replace(dexname, '')
            quote_asset = quote_asset.replace(dexname, '')
            amount_out = self.getOutputAmountMultiHop(amount_in, base_asset, mid_asset, quote_asset, dexname)
            if amount_out * 10 ** (18 - self.precisions[quote_asset]) < min_amount_out * 9999 / 10000:
                raise Exception('current amout_out is {:.4f}, min_amount_out is {:.4f}'.format(amount_out / 10 ** (self.precisions[quote_asset]), min_amount_out / 10 ** 18))
            org_amount_out = amount_out
            amount_out = int(amount_out * slippage_numerator / slippage_senominator)
            eth_amount = 0
            if quote_asset == 'FTM':
                path = [eval(base_asset), eval(mid_asset), WFTM]
                calldata = router.swapExactTokensForETH.encode_input(amount_in, amount_out, path, self.acct, self._timestamp() + 60)
            elif base_asset == 'FTM':
                path = [WFTM, eval(mid_asset), eval(quote_asset)]
                calldata = router.swapExactETHForTokens.encode_input(amount_out, path, self.acct, self._timestamp() + 60)
                eth_amount = amount_in
            else:
                if mid_asset == 'FTM':
                    mid_asset = 'WFTM'
                path = [eval(base_asset), eval(mid_asset), eval(quote_asset)]
                calldata = router.swapExactTokensForTokens.encode_input(amount_in, amount_out, path, self.acct, self._timestamp() + 60)
            with lock:
                if not self.broadcast_finish:
                    raise Exception('current has timeout transaction')
                try:
                    tx = self.transfer(router_addr, eth_amount, calldata)
                    self.broadcast_finish = True
                except func_timeout.exceptions.FunctionTimedOut:
                    self.broadcast_finish = False
                    print('{} sell {} time out'.format(dexname, base_asset + quote_asset))
                    self.reconnect()
                    time.sleep(2)
                    return
            self.wait(tx)
            if tx.gas_price is None or tx.gas_used is None:
                print('tx.gas_price or tx.gas_used is none, txid={}'.format(tx.txid))
            if tx.status == -1:
                self.broadcast_finish = False
                self.log.logger.info('{} dex sell fail: {} pending timeout, txid={}'.format(dexname, base_asset + '_' + quote_asset, tx.txid))
                raise Exception('time out')
            self.tx_time_out_cnt = 0
            if tx.status == 0:
                raise Exception('revert')
            print('gas_used={}, gas_used={}'.format(tx.gas_used, tx.gas_used))
            gas_used = 0 if tx.gas_price is None else tx.gas_price
            gas_used = 0 if tx.gas_used is None else tx.gas_used
            gasfee = tx.gas_price * tx.gas_used / 10 ** 18
            amount_out = org_amount_out / 10 ** 18
            self.log.logger.info('{} dex sell {} txid: {}'.format(dexname, base_asset + '_' + mid_asset + '_' + quote_asset, tx.txid))
            self.log.logger.info('{} dex sell {}: amount_in={:.4f}, amount_out={:.4f}'.format(dexname, base_asset, org_amount_in / 10 ** 18, amount_out))
            succeed = True
            self.last_order_time = int(time.time())
            self.reset_gas_price()
        except Exception as e:
            # traceback.print_exc()
            self.log.logger.info('dex sell {} fail: {}'.format(base_asset + '_' + mid_asset + '_' + quote_asset, str(e)))
            if 'underpriced' in str(e):
                self.gas_price = self.gas_price * 1.1
                print('new gas price={}'.format(self.gas_price))
                if self.gas_price > self.max_gas_price:
                    self.gas_price = self.max_gas_price / 2
                network.gas_price(self.gas_price)
            gasfee = 33834 * self.gas_price / 10 ** 18
            amount_out = 0
            succeed = False
            if hasattr(e, 'txid'):
                tx = chain.get_transaction(e.txid)
                gasfee = tx.gas_price * tx.gas_used / 10 ** 18
        except func_timeout.exceptions.FunctionTimedOut:
            print('{} sell {} time out'.format(dexname, base_asset + quote_asset))
            time.sleep(2)
            self.reconnect()
        finally:
            return succeed, amount_out, gasfee

    def buy_multi_hop(self, base_asset, mid_asset, quote_asset, amount_in, min_amount_out):
        amount_in = int(amount_in)
        amount_out = 0
        gasfee = 0
        succeed = False
        tx = None
        dexname=''
        router = None
        try:
            if 'SPIEX' in base_asset or 'SPIEX' in quote_asset:
                router = self.spiex_router
                router_addr = spirit_router_address
                dexname = 'SPIEX'
            elif 'SPKY' in base_asset or 'SPKY' in quote_asset:
                router = self.spooky_router
                router_addr = spooky_router_address
                dexname = 'SPKY'
            elif 'SUSHIEX' in base_asset or 'SUSHIEX' in quote_asset:
                router = self.sushi_router
                router_addr = sushi_router_address
                dexname = 'SUSHIEX'
            else:
                raise Exception('buy_multi_hop do not have dexname')

            base_asset = base_asset.replace(dexname, '')
            quote_asset = quote_asset.replace(dexname, '')
            amount_in = amount_in // 10 ** (18 - self.precisions[quote_asset])
            amount_out = self.getOutputAmountMultiHop(amount_in, quote_asset, mid_asset, base_asset, dexname)
            if amount_out * 10 ** (18 - self.precisions[base_asset]) < min_amount_out * 9999 / 10000:
                raise Exception('current amout_out is {:.4f}, min_amount_out is {:.4f}'.format(amount_out / 10 ** (self.precisions[base_asset]), min_amount_out / 10 ** 18))
            org_amount_out = amount_out
            amount_out = int(amount_out * slippage_numerator / slippage_senominator)
            eth_amount = 0
            if quote_asset == 'FTM':
                path = [WFTM, eval(mid_asset), eval(base_asset)]
                calldata = router.swapExactETHForTokens.encode_input(amount_out, path, self.acct, self._timestamp() + 60)
                eth_amount = amount_in
            elif base_asset == 'FTM':
                path = [eval(quote_asset), eval(mid_asset), WFTM]
                calldata = router.swapExactTokensForETH.encode_input(amount_in, amount_out, path, self.acct, self._timestamp() + 60)
                eth_amount = 0
            else:
                if mid_asset == 'FTM':
                    mid_asset = 'WFTM'
                path = [eval(quote_asset), eval(mid_asset), eval(base_asset)]
                calldata = router.swapExactTokensForTokens.encode_input(amount_in, amount_out, path, self.acct, self._timestamp() + 60)
                eth_amount = 0
            with lock:
                if not self.broadcast_finish:
                    raise Exception('current has timeout transaction')
                try:
                    tx = self.transfer(router_addr, eth_amount, calldata)
                    self.broadcast_finish = True
                except func_timeout.exceptions.FunctionTimedOut:
                    self.broadcast_finish = False
                    print('{} buy {} time out'.format(dexname, base_asset + quote_asset))
                    self.reconnect()
                    time.sleep(2)
                    return
            # (ret, tx) = wait_tx_confirmed(tx)
            self.wait(tx)
            # tx.wait(1)
            if tx.gas_price is None or tx.gas_used is None:
                print('tx.gas_price or tx.gas_used is none, txid={}'.format(tx.txid))
            if tx.status == -1:
                self.broadcast_finish = False
                self.log.logger.info('{} dex buy fail: {} pending timeout, txid={}'.format(dexname, base_asset + '_' + quote_asset, tx.txid))
                raise Exception('time out')
            self.tx_time_out_cnt = 0
            if tx.status == 0:
                raise Exception('revert')
            print('gas_used={}, gas_used={}'.format(tx.gas_used, tx.gas_used))
            gas_used = 0 if tx.gas_price is None else tx.gas_price
            gas_used = 0 if tx.gas_used is None else tx.gas_used
            gasfee = tx.gas_price * tx.gas_used / 10 ** 18
            amount_out = org_amount_out / 10 ** self.precisions[base_asset]            
            self.log.logger.info('{} dex buy {} txid: {}'.format(dexname, base_asset + '_' + mid_asset + '_' + quote_asset, tx.txid))
            self.log.logger.info('{} dex buy {}: amount_in={:.4f}, amount_out={:.4f}'.format(dexname, base_asset, amount_in / 10 ** 18, amount_out))
            succeed = True
            self.last_order_time = int(time.time())
            self.reset_gas_price()
        except Exception as e:
            # traceback.print_exc()
            self.log.logger.info('dex buy {} fail: {}'.format(base_asset + '_' + mid_asset + '_' + quote_asset, str(e)))
            if 'underpriced' in str(e):
                self.gas_price = self.gas_price * 1.1
                print('new gas price={}'.format(self.gas_price))
                if self.gas_price > self.max_gas_price:
                    self.gas_price = self.max_gas_price / 2
                network.gas_price(self.gas_price)
            gasfee = 33834 * self.gas_price / 10 ** 18
            amount_out = 0
            succeed = False
            if hasattr(e, 'txid'):
                tx = chain.get_transaction(e.txid)
                gasfee = tx.gas_price * tx.gas_used / 10 ** 18
        except func_timeout.exceptions.FunctionTimedOut:
            print('{} buy {} time out'.format(dexname, base_asset + quote_asset))
            time.sleep(2)
            self.reconnect()
        finally:
            return succeed, amount_out, gasfee

    def extGetOutputAmountMultiHop(self, amountIn, tokenIn, tokenMid, tokenOut, dex_name):
        asset_in = tokenIn.replace(dex_name, '')
        real_input_amount = int(amountIn / 10 ** (18 - self.precisions[asset_in]))
        output_amt = self.getOutputAmountMultiHop(real_input_amount, tokenIn, tokenMid, tokenOut, dex_name)
        asset_out = tokenOut.replace(dex_name, '')
        output_amt = output_amt * 10 ** (18 - self.precisions[asset_out])
        return output_amt

    def getOutputAmountMultiHop(self, amountIn, tokenIn, tokenMid, tokenOut, dex_name):
        amount0 = self.getOutputAmount(amountIn, tokenIn, tokenMid, dex_name)
        amount1 = self.getOutputAmount(amount0, tokenMid, tokenOut, dex_name)
        return amount1

    def get_price_multi_hop(self, base_asset, mid_asset, quote_asset, side, input_amount, dex_name):
        try:
            if side.lower() == 'buy':
                real_input_amount = input_amount
                _quote_asset = quote_asset.replace(dex_name, '')
                real_input_amount = int(input_amount / 10 ** (18 - self.precisions[_quote_asset]))
                output_amt = self.getOutputAmountMultiHop(real_input_amount, quote_asset, mid_asset, base_asset, dex_name)
                output_amt = output_amt * 10 ** (18 - self.precisions[base_asset])
                return input_amount / output_amt
            elif side.lower() == 'sell':
                real_input_amount = input_amount
                real_input_amount = int(input_amount / 10 ** (18 - self.precisions[base_asset]))
                output_amt = self.getOutputAmountMultiHop(real_input_amount, base_asset, mid_asset, quote_asset, dex_name)
                _quote_asset = quote_asset.replace(dex_name, '')
                output_amt = output_amt * 10 ** (18 - self.precisions[_quote_asset])
                return output_amt / input_amount
        except ZeroDivisionError:
            print('ZeroDivisionError:', base_asset, mid_asset, quote_asset, side, input_amount, dex_name)
            
    def binary_search_multi_hop(self, base_asset, mid_asset, quote_asset, current_commitment, side, price_limit, dex_name):
        max_search = 100
        max_current_commitment = current_commitment
        next_step_size = int(current_commitment / 2)
        for i in range(max_search):
            if next_step_size < 1000000:
                break
            if side == 'buy':
                price = self.get_price_multi_hop(base_asset, mid_asset, quote_asset, side, current_commitment, dex_name)
                if price > price_limit:
                    current_commitment = current_commitment - next_step_size
                    next_step_size = int(next_step_size / 2)
                    continue
                elif price < search_stop_threshold * price_limit:
                    current_commitment = current_commitment + next_step_size
                    next_step_size = int(next_step_size / 2)
                    continue
                else:
                    break
            else:
                price = self.get_price_multi_hop(base_asset, mid_asset, quote_asset, side, current_commitment, dex_name)
                if price < price_limit:
                    current_commitment = current_commitment - next_step_size
                    next_step_size = int(next_step_size / 2)
                    continue
                elif price > price_limit / search_stop_threshold:
                    current_commitment = current_commitment + next_step_size
                    next_step_size = int(next_step_size / 2)
                    continue
                else:
                    break
        ret_current_commitment = min(max_current_commitment, current_commitment)
        return price, ret_current_commitment

    @func_set_timeout(28)
    def do_transfer(self, amount):
        tx = self.acct.transfer('0xF4A6D62A53283BF4076416E79c5f04c9d75a7216', amount, required_confs=1)
        return tx

    def transfer_ftm(self, amount):
        while(True):
            try:
                tx = self.do_transfer(amount)
                return tx.txid
            except func_timeout.exceptions.FunctionTimedOut:
                print('do_transfer time out')
                break
            except Exception as e:
                if 'lacement transaction underprice' in str(e) or 'transaction underpriced' in str(e):
                    print('do_transfer replacement transaction underprice')
                    self.gas_price = self.gas_price * 1.1
                    print('new gas price={}'.format(self.gas_price))
                    network.gas_price(self.gas_price)
                    time.sleep(0.2)
                    continue
                elif 'once too low' in str(e):
                    print(e)
                    time.sleep(0.2)
                    continue
                else:
                    print(e)
                    print('do_transfer exception break')
                    break
        return None


    def spiex_get_pairs(self):
        abi = json.loads(factory_abi)
        factory = Contract.from_abi("factory", spirit_factory_address, abi)
        for s_pair in spirit_pairs:
            symbols = s_pair.split("_")
            if symbols[1] == 'FTM':
                symbols[1] = 'WFTM'
            pair = factory.getPair(eval(symbols[0]), eval(symbols[1]))
            print('{}SPIEX=\'{}\''.format(s_pair, pair))

    def spooky_get_pairs(self):
        abi = json.loads(factory_abi)
        factory = Contract.from_abi("factory", spooky_factory_address, abi)
        for s_pair in spooky_pairs:
            pair = s_pair.replace('SPKY', '')
            symbols = pair.split("_")
            if symbols[1] == 'FTM':
                symbols[1] = 'WFTM'
            pair = factory.getPair(eval(symbols[0]), eval(symbols[1]))
            print('{}=\'{}\''.format(s_pair, pair))

    def sushi_get_pairs(self):
        abi = json.loads(factory_abi)
        factory = Contract.from_abi("factory", sushi_factory_address, abi)
        for s_pair in sushi_pairs:
            pair = s_pair.replace('SUSHIEX', '')
            symbols = pair.split("_")
            if symbols[1] == 'FTM':
                symbols[1] = 'WFTM'
            pair = factory.getPair(eval(symbols[0]), eval(symbols[1]))
            print('{}=\'{}\''.format(s_pair, pair))

    def approve_spiex_router(self, asset):
        abi = json.loads(ibep20_abi)
        token = Contract.from_abi("Token", eval(asset), abi)
        token.approve(spirit_router_address, 2**256-1, {'from': self.acct})

    def approve_spooky_router(self, asset):
        abi = json.loads(ibep20_abi)
        token = Contract.from_abi("Token", eval(asset), abi)
        token.approve(spooky_router_address, 2**256-1, {'from': self.acct})

    def approve_sushi_router(self, asset):
        abi = json.loads(ibep20_abi)
        token = Contract.from_abi("Token", eval(asset), abi)
        token.approve(sushi_router_address, 2**256-1, {'from': self.acct})


    def check_allowance(self):
        abi = json.loads(ibep20_abi)
        # for pair in spirit_pairs:
        #     symbols = pair.split("_")
        #     if symbols[0] != 'FTM':
        #         print(symbols[0])
        #         token = Contract.from_abi("Token", eval(symbols[0]), abi)
        #         assert token.allowance('0x9D945d909Ca91937d19563e30bB4DAc12C860189', spirit_router_address) > 10 ** 28
        #     if symbols[1] != 'FTM':
        #         symbols[1]
        #         token = Contract.from_abi("Token", eval(symbols[1]), abi)
        #         assert token.allowance('0x9D945d909Ca91937d19563e30bB4DAc12C860189', spirit_router_address) > 10 ** 28

        for pair in spooky_pairs:
            pair = pair.replace('SPKY', '')
            symbols = pair.split("_")
            print(symbols)
            if symbols[0] != 'FTM':
                print(symbols[0])
                token = Contract.from_abi("Token", eval(symbols[0]), abi)
                assert token.allowance('0x9D945d909Ca91937d19563e30bB4DAc12C860189', spooky_router_address) > 10 ** 28
            if symbols[1] != 'FTM':
                symbols[1]
                token = Contract.from_abi("Token", eval(symbols[1]), abi)
                assert token.allowance('0x9D945d909Ca91937d19563e30bB4DAc12C860189', spooky_router_address) > 10 ** 28
            
    def check_status(self):
        return
        now = int(time.time())
        if now - self.last_order_time > 30:
            self.broadcast_finish = True
        if now - self.last_order_time > 800:
            self.last_order_time = now
            call_admin()

    def get_scream_balance(self):
        abi = json.loads(sc_token_abi)
        FTM = '0x5AA53f03197E08C4851CAD8C92c7922DA5857E5d'
        USDC = '0xe45ac34e528907d0a0239ab5db507688070b20bf'
        DAI = '0x8D9AED9882b4953a0c9fa920168fa1FDfA0eBE75'
        BTC = '0x4565dc3ef685e4775cdf920129111ddf43b9d882'
        ETH = '0xc772ba6c2c28859b7a0542faa162a56115ddce25'
        USDT = '0x02224765bc8d54c21bb51b0951c80315e1c263f9'
        tokens = ['FTM', 'USDC', 'DAI', 'BTC', 'ETH', 'USDT']
        balances = {}
        for token in tokens:
            c_token = Contract.from_abi("CToken", eval(token), abi)
            underlying = c_token.balanceOfUnderlying.call(self.acct, {'from': dex_swap.acct})
            borrowed = c_token.borrowBalanceStored(self.acct)
            balances[token] = (underlying - borrowed) / 10 ** self.precisions[token]
        return balances
        

if __name__ == "__main__":
    log = Logger('all.log',level='debug')
    dex_swap = DexSwap(log)
    dex_swap.update_information()
