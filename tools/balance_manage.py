#encoding=‘utf-8’
from common import *
from arbitrage.binancemarket import BinanceMarket
from arbitrage.dexswap import DexSwap
from arbitrage.logger import Logger
from brownie import *
from Crypto.Cipher import AES
from binascii import b2a_hex, a2b_hex
import getpass
from  Crypto import Random
from waiting import wait as wait_conf
from func_timeout import func_set_timeout
import func_timeout
import time
import func_timeout
from func_timeout import func_set_timeout
import json
from arbitrage.abi import *
from arbitrage.calladmin import *

log = Logger('all.log',level='info')

def get_asset_info(**params):
    return client._request_margin_api('get', 'capital/config/getall', True, data=params)

assets.remove('DEGO')
spot_assets.remove('DEGO')
if 'DEGO' in assets or 'DEGO' in spot_assets:
    raise
cex_wallet = '0xF4A6D62A53283BF4076416E79c5f04c9d75a7216'
admin_wallet = '0x9D945d909Ca91937d19563e30bB4DAc12C860189'

last_order_time = int(time.time())
last_transfer2dex_time = 0

def withdrow(asset, wallet, amount):
    print('{} withdraw amount is {}'.format(asset, amount))
    global last_order_time
    try:
        last_order_time = int(time.time())
        return client.withdraw(
            asset=asset,
            address=wallet,
            amount=amount,
            network='BSC')
    except Exception as e:
        print(e)
        return {'success': False}

def repay_cex_borrow():
    response = client.get_margin_account()
    margin_acc_assets = response['userAssets']
    for asset in assets:
        for asset_info in margin_acc_assets:
            if asset_info['asset'] == asset:
                free = float(asset_info['free'])
                borrowed = float(asset_info['borrowed'])
                if borrowed > 0:
                    repay_amount = 0
                    if free >= borrowed:
                        repay_amount = borrowed
                    elif free > 0:
                        repay_amount = free
                    if repay_amount > 0:
                        print('{} repay amount: {}'.format(asset, repay_amount))
                        client.repay_margin_loan(
                            asset=asset,
                            amount=repay_amount)

dex_swap = DexSwap(None)
dex_swap.set_low_gas_price()

current_bsc_balances = {}
current_binance_balances = {}
tickers = {}

def get_price1(asset):
    global tickers
    if asset == 'USDT' or asset == 'DAI' or asset == 'BUSD':   
        return 1
    if asset == 'WBTC':
        asset = 'BTC'
    for ticker in tickers:
        if asset in busd_pair_assets:
            if ticker['symbol'] == (asset + 'BUSD'):
                return float(ticker['price'])
        if ticker['symbol'] == 'ETHUSDT':
            p_eth = float(ticker['price'])
        if asset == 'BETH' and ticker['symbol'] == 'BETHETH':
            p_beth = float(ticker['price'])
            return p_eth * p_beth
        elif ticker['symbol'] == (asset + 'USDT'):
            return float(ticker['price'])

def check_balance_change(start_total_balance, end_total_balance):
    diff_balances = {}
    for asset in assets:
        diff_balances[asset] = start_total_balance[asset] - end_total_balance[asset]

    diff_bal = 0
    for asset in assets:
        if asset in diff_balances:
            price = get_price1(asset)
            diff_bal += diff_balances[asset] * price
    assert abs(diff_bal) < 300

@func_set_timeout(28)
def do_transfer(asset, amount, _nonce):
    if asset == 'BNB':
        tx = dex_swap.acct.transfer(cex_wallet, amount, required_confs=1, nonce=_nonce)
        return tx
    else:
        abi = json.loads(ibep20_abi)
        mtoken = Contract.from_abi("BscToken", eval(asset), abi)
        tx = mtoken.transfer(cex_wallet, amount, {'from': dex_swap.acct})
        return tx

def transfer_token(asset, amount):
    while(True):
        try:
            _nonce = dex_swap.acct.nonce + 1
            tx = do_transfer(asset, amount, _nonce)
            return tx.txid
        except func_timeout.exceptions.FunctionTimedOut:
            print('do_transfer time out')
            break
        except Exception as e:
            if 'lacement transaction underprice' in str(e) or 'once too low' in str(e):
                print('do_transfer replacement transaction underprice or nonce {} too low'.format(_nonce))
                time.sleep(0.2)
                continue
            else:
                print(e)
                print('do_transfer exception break')
                break
            # traceback.print_exc()
    return None

def get_spot_asset_balance(asset, spot_balances):
    for balance in spot_balances:
        if balance['asset'] == asset:
            free = float(balance['free'])
            return free
    return 0

withdrow_dex_ids = []

spot_assets_max_limit = {'BNB': 150, 'USDT': 100000, 'BUSD': 30000, 'BTC': 1, 'ETH': 20, 'DOT': 200, 'LINK': 200, 'UNI': 300, 'COMP': 10, 'SUSHI': 500}
wallet_assets_max_limit = {'BNB': 800, 'USDT': 70000, 'BUSD': 60000, 'BTC': 1.5, 'ETH': 30, 'DOT': 900, 'LINK': 600, 'FIL': 200,
    'UNI': 650, 'COMP': 45, 'SUSHI': 1100}
#############################

def transfer2cex():
    withdrow_dex_ids.clear()
    margin_balances = get_margin_balance()
    spot_balances = get_spot_balances()
    dex_swap.update_information()
    i = 0
    while(i < len(assets)):
        asset = assets[i]
        i += 1
        margin_balances = get_margin_balance()
        dex_swap.update_information()
        for asset_info in margin_balances:
            if asset_info['asset'] == asset:
                free = float(asset_info['free'])
                dex_free = dex_swap.balances[asset]
                borrowed = float(asset_info['borrowed']) - free
                price = get_price1(asset)
                if price * dex_free / 10 ** 18 < 22000:
                    continue
                amount = min(int(borrowed * 10 ** 18), dex_free)
                if amount * price / 10 ** 18 < 5000 and asset not in wallet_assets_max_limit:
                    continue
                if asset in wallet_assets_max_limit:
                    diff_amount = dex_free / 10 ** 18 - wallet_assets_max_limit[asset]
                    if diff_amount * price > 8000:
                        amount = int(dex_free - wallet_assets_max_limit[asset] * 10 ** 18)
                    else:
                        continue
                print('margin {} transfer to cex, amount is {:.4f}'.format(asset, amount / 10 ** 18))
                try:
                    amount = amount // 10 ** (18 - dex_swap.precisions[asset])
                    txid = transfer_token(asset, int(amount))
                    if txid is None:
                        continue
                    withdrow_dex_ids.append(txid)
                    print(txid)
                    transfer_asset_2_spot()
                    # spot2margin()
                except func_timeout.exceptions.FunctionTimedOut:
                    print('transfer_token time out')
                    i -= 1
                except Exception as e:
                    print('transfer_token exception')
                    # traceback.print_exc()
                    if 'lacement transaction underprice' in str(e) or 'once too low' in str(e):
                        i -= 1
                        time.sleep(1)
                    else:
                        time.sleep(0.5)
                        dex_swap.update_information()

    spot_balances = get_spot_balances()
    i = 0
    while(i < len(spot_assets)):
        asset = spot_assets[i]
        i += 1
        for asset_info in spot_balances:
            if asset_info['asset'] == asset:
                price = get_price1(asset)
                cex_free = float(asset_info['free'])
                if cex_free * price > 5000:
                    continue
                dex_free = dex_swap.balances[asset]
                if (dex_free / 10 ** 18 - cex_free) * price / 2 > 3000:
                    amount = int((dex_free - cex_free * 10 ** 18) / 2)
                    print('{} transfer to cex, amount is {:.4f}'.format(asset, amount / 10 ** 18))
                    amount = amount // 10 ** (18 - dex_swap.precisions[asset])
                    try:
                        txid = transfer_token(asset, amount)
                        if txid is None:
                            continue
                        withdrow_dex_ids.append(txid)
                        print(txid)
                        transfer_asset_2_spot()
                        # spot2margin()
                    except func_timeout.exceptions.FunctionTimedOut:
                        print('transfer_token time out')
                        i -= 1
                    except Exception as e:
                        print('transfer_token exception')
                        # traceback.print_exc()
                        if 'lacement transaction underprice' in str(e) or 'once too low' in str(e):
                            i -= 1
                        time.sleep(1)
def spot2margin():
    response = client.get_account()
    balances = response['balances']
    for asset in assets:
        if asset in spot_assets:
            continue
        for balance in balances:
            if balance['asset'] == asset:
                free = float(balance['free'])
                amount = free
                if asset in spot_assets_max_limit:
                    if free > spot_assets_max_limit[asset] * 1.01:
                        amount = free - spot_assets_max_limit[asset]
                    else:
                        amount = 0

                price = get_price1(asset)
                if amount * price > 100:
                    print('{} transfer to margin amount is {}'.format(asset, amount))
                    client.transfer_spot_to_margin(
                        asset=asset,
                        amount=amount)

withdrow_cex_ids = []


def is_arbitrager_working():
    return False
    with open('flag') as f:
        if f.read() == 'true':
            return True
    return False

def repay_bnb_borrow(margin_balances):
    assets = ['BNB', 'BUSD', 'USDT']
    for asset_info in margin_balances:
        if asset_info['asset'] in assets:
            asset = asset_info['asset']
            free = float(asset_info['free'])
            borrowed = float(asset_info['borrowed'])
            price = get_price1(asset)
            if borrowed * price > 100:
                repay_amount = 0
                if free >= borrowed + 2:
                    repay_amount = borrowed
                    print('{} repay amount: {}'.format(asset, repay_amount))
                    client.repay_margin_loan(
                        asset=asset,
                        amount=repay_amount)


def transfer2dex():
    global tickers
    if not tickers:
        tickers = client.get_all_tickers()
    if is_arbitrager_working():
        print('arbitrager is working')
        return True
    global last_transfer2dex_time
    withdrow_cex_ids.clear()
    current_transfer2dex_time = time.time()
    if current_transfer2dex_time - last_transfer2dex_time < 120:
        print('transfer2dex time interval too small')
        return True
    last_transfer2dex_time = current_transfer2dex_time
    margin_balances = get_margin_balance()
    repay_bnb_borrow(margin_balances)
    spot_balances = get_spot_balances()
    dex_swap.update_information()
    assets_info = get_asset_info(timestamp = int(time.time() * 1000))
    try:
        for asset in assets:
            if not is_withdraw_enable(assets_info, asset):
                continue
            for asset_info in margin_balances:
                if asset_info['asset'] == asset:
                    free = float(asset_info['free'])
                    borrowed = float(asset_info['borrowed'])
                    amount = free - borrowed
                    dex_free = dex_swap.balances[asset] / 10 ** 18
                    price = get_price1(asset)
                    if asset in wallet_assets_max_limit:
                        if dex_free > wallet_assets_max_limit[asset] / 2:
                            continue
                    elif dex_free * price > 10000:
                        continue
                    if asset in wallet_assets_max_limit:
                        spot_bal = get_spot_asset_balance(asset, spot_balances)
                        if dex_free < wallet_assets_max_limit[asset]:
                            keep_spot_balance = 0 if asset not in spot_assets_max_limit else spot_assets_max_limit[asset]
                            needed_bal = (wallet_assets_max_limit[asset] - dex_free) - (spot_bal - keep_spot_balance)
                        else:
                            continue
                        if needed_bal * price > 8000 and free > needed_bal * 1.01:
                            amount = round(needed_bal, 2) - 0.01
                        else:
                            continue
                    if amount * price < 5000:
                        continue
                    print('{} transfer margin to spot amount: {}'.format(asset, amount))
                    client.transfer_margin_to_spot(
                        asset=asset,
                        amount=amount)
    except Exception as e:
        print(str(e))
        repay_cex_borrow()
    finally:
        print('transfer out finish')

    spot_balances = get_spot_balances()
    margin_balances = get_margin_balance()
    asset = 'BNB'
    for balance in spot_balances:
        if balance['asset'] == asset:
            free = float(balance['free'])
            price = get_price1(asset)
            dex_free = dex_swap.balances[asset] / 10 ** 18
            if dex_free > 400:
                continue
            amount = wallet_assets_max_limit[asset] - dex_free
            if free > amount and amount * price >= 8000:
                print('withdraw {}, amount is {}'.format(asset, amount))
                reponse = {}
                amount = round(amount, 2) - 0.01
                if asset.upper() == 'BNB':
                    reponse = withdrow('BNB', admin_wallet, amount)
                if 'success' in reponse and reponse['success']:
                    withdrow_cex_ids.append(reponse['id'])
            break

    spot_balances = get_spot_balances()
    try:
        for asset in spot_assets:
            if not is_withdraw_enable(assets_info, asset):
                continue
            for balance in spot_balances:
                if balance['asset'] == asset:
                    price = get_price1(asset)
                    if asset.upper() == 'BNB':
                        continue
                    cex_free = float(balance['free'])
                    dex_free = dex_swap.balances[asset] / 10 ** 18
                    if dex_free * price > 7000:
                        continue
                    amount = (cex_free - dex_free) / 2
                    amount = min(round(amount, 3) - 0.001, cex_free)
                    if asset.upper() in fans_tokens or asset == 'DODO' or asset == 'TLM':
                        amount = round(amount, 1) - 0.1
                    if amount * price > 5000:
                        print('spot asset {} withdraw to dex, amount is {:.4f}'.format(asset, amount))
                        reponse = withdrow(asset, admin_wallet, amount)
                        if 'success' in reponse and reponse['success']:
                            withdrow_cex_ids.append(reponse['id'])
    except Exception as e:
        print(str(e))
    except:
        print('Unexpected error')

    time.sleep(1)
    spot_balances = get_spot_balances()
    margin_balances = get_margin_balance()
    for asset in assets:
        if asset in spot_assets or asset == 'BNB':
            continue
        if not is_withdraw_enable(assets_info, asset):
            continue
        for balance in spot_balances:
            if balance['asset'] == asset:
                free = float(balance['free'])
                price = get_price1(asset)
                amount = free
                if asset in wallet_assets_max_limit:
                    dex_free = dex_swap.balances[asset] / 10 ** 18
                    amount = wallet_assets_max_limit[asset] - dex_free
                    if amount * price < 8000 or free < amount:
                        print('{} balance is {}, withdraw amount is {}, too small'.format(asset, free, amount))
                        continue
                if amount * price >= 5000:
                    print('margin asset withdraw {}, amount is {}'.format(asset, free))
                    reponse = {}
                    amount = round(amount, 3) - 0.001    
                    reponse = withdrow(asset, admin_wallet, amount)
                    if 'success' in reponse and reponse['success']:
                        withdrow_cex_ids.append(reponse['id'])
    return True


last_transfer_time = 0
def transfer_asset_2_spot():
    global last_transfer_time
    current_transfer_time = time.time()
    if current_transfer_time - last_transfer_time < 5:
        return
    last_transfer_time = current_transfer_time
    margin_balances = get_margin_balance()
    spot_balances = get_spot_balances()
    try:
        for asset_info in margin_balances:
            asset = asset_info['asset']
            if asset in spot_assets_max_limit:
                free = float(asset_info['free'])
                spot_asset_balance = get_spot_asset_balance(asset, spot_balances)
                if spot_asset_balance < spot_assets_max_limit[asset] * 0.9:
                    free = round(min(spot_assets_max_limit[asset] - spot_asset_balance, free), 3) - 0.001
                    price = get_price1(asset)
                    if free * price < 1000:
                        continue
                    print('{} transfer margin to spot amount: {}'.format(asset, free))
                    client.transfer_margin_to_spot(
                        asset=asset,
                        amount=free)
    except Exception as e:
        print(str(e))
    finally:
        return

def wait_for_deposit_complete():
    l = len(withdrow_dex_ids)
    while(l > 0):
        try:
            print('\nwait for deposit to cex complete, pending txids:\n')
            print(withdrow_dex_ids)
            response = client.get_deposit_history(status=1)
            deposit_list = response['depositList']
            for item in deposit_list:
                for txid in withdrow_dex_ids:
                    if item['txId'] == txid:
                        withdrow_dex_ids.remove(txid)
                        break
        finally:
            l = len(withdrow_dex_ids)
            check_status()
        time.sleep(3)
        transfer_asset_2_spot()
        spot2margin()

def wait_for_withdraw_complete():
    l = len(withdrow_cex_ids)
    if l==0:
        transfer2cex()
        wait_for_deposit_complete()
        spot2margin()
    while(l > 0):
        try:
            print('\nwait for withdraw to dex complete, pending ids:\n')
            print(withdrow_cex_ids)
            response = client.get_withdraw_history(status=2)
            withdraw_list = response['withdrawList']
            response = client.get_withdraw_history(status=4)
            withdraw_list += response['withdrawList']
            response = client.get_withdraw_history(status=6)
            withdraw_list += response['withdrawList']

            for item in withdraw_list:
                for _id in withdrow_cex_ids:
                    if item['id'] == _id and 'txId' in item:
                        txid = item['txId']
                        tx = chain.get_transaction(txid)
                        while(tx==None or tx.status==None or tx.status < 0):
                            print('withdraw tx status is {}'.format(tx.status))
                            time.sleep(0.3)
                        if tx.status == 1:
                            print('txid={} succeed'.format(txid))
                            withdrow_cex_ids.remove(_id)
                            break
                        else:
                            print('txid={} status={}'.format(txid, tx.status))
            l = len(withdrow_cex_ids)
            if l > 0:            
                response = client.get_withdraw_history(status=3)
                withdraw_list = response['withdrawList']
                response = client.get_withdraw_history(status=5)
                withdraw_list += response['withdrawList']
                print('get completed withdraw history')
                for item in withdraw_list:
                    for _id in withdrow_cex_ids:
                        if item['id'] == _id:
                            withdrow_cex_ids.remove(_id)
                            break
            l = len(withdrow_cex_ids)
            if l > 0:
                transfer2cex()
                wait_for_deposit_complete()
                spot2margin()
        except Exception as e:
            print('wait_for_withdraw_complete exception')
            log.logger.info('wait_for_withdraw_complete exception: {}'.format(str(e)))
        finally:
            l = len(withdrow_cex_ids)
            check_status()
        time.sleep(3)
        transfer_asset_2_spot()
    
def is_withdraw_enable(assets_info, asset):
    for asset_info in assets_info:
        if asset_info['coin'].upper() == asset.upper():
            for network in asset_info['networkList']:
                if network['name'] == 'BEP20 (BSC)':
                    return network['withdrawEnable']
    return False

def check_status():
    global last_order_time
    now = int(time.time())
    if now - last_order_time > 1800:
        last_order_time = now
        call_admin()

if __name__ == "__main__":
    print('start monitor balance...')
    while(True):
        try:
            check_status()
            if not transfer2dex():
                spot2margin()
            time.sleep(1)
            wait_for_withdraw_complete()
        except func_timeout.exceptions.FunctionTimedOut:
            print('transfer_token time out') 
        except Exception as e:
            print(str(e))
        time.sleep(2)