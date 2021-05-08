
from common import *
from arbitrage.dexswap import DexSwap
from arbitrage.binancemarket import BinanceMarket
from arbitrage.logger import Logger
import json
from tronpy import Tron
from tronpy import keys
import math

binance = BinanceMarket(None)

def round_down(n, decimals=0):
    multiplier = 10 ** decimals
    return math.floor(n * multiplier) / multiplier

def margin_borrow_asset(asset, amount):
    try:
        response = client.create_margin_loan(
            asset=asset,
            amount=amount,
            timestamp=int(time.time() * 1000)
        )
        print('Loan tranId='+str(response['tranId'])+',amount='+str(amount))
        return True
    except Exception as e:
        print('margin borrow exception: {}'.format(str(e)))
        return False

def margin_buy(asset, amount, price):
    buy_action = 'buy {}, amount {}, price {}'.format(asset, amount, price)
    order = None
    try:
        precis = binance.precisions[asset + 'USDT']
        price = round_down(price, precis[0])
        amount = round_down(amount, precis[1])
        free_bal = binance.margin_get_balance('USDT')
        usdt_needed = round_down(amount * price * 1.003, 1) + 1
        if free_bal < usdt_needed:
            bamount = usdt_needed - free_bal
            if bamount < 5:
                amount = round_down(free_bal * 0.997 / price, precis[1])
            else:
                if not margin_borrow_asset('USDT', bamount):
                    raise Exception('margin borrow USDT failed')
        order = client.create_margin_order(
            symbol=asset + 'USDT',
            side=client.SIDE_BUY,
            type=client.ORDER_TYPE_MARKET,
            quantity=amount,
            timestamp=int(time.time() * 1000))
    except Exception as e:
        print('xxxxx--cex buy exception: {}'.format(str(e)))
        print('xxxxx--cex buy exception: {}'.format(buy_action))
        return
    binance.update_margin_balances = False
    print('cex {} succeed'.format(buy_action))

def margin_sell(asset, amount, price):
    sell_action = 'sell {}, amount {}, price {}'.format(asset, amount, price)
    order = None
    try:
        precis = binance.precisions[asset + 'USDT']
        price = round_down(price, precis[0])
        amount = round_down(amount, precis[1])
        free_bal = binance.margin_get_balance(asset)
        if free_bal < amount * 1.003:
            bamount = round_down(amount * 1.01 - free_bal, precis[1])
            if bamount * price < 5:
                amount = round_down(free_bal, precis[1])
            else:
                if not margin_borrow_asset(asset, bamount):
                    raise Exception('margin borrow {} failed'.format(asset))
        order = client.create_margin_order(
            symbol=asset + 'USDT',
            side=client.SIDE_SELL,
            type=client.ORDER_TYPE_MARKET,
            quantity=amount,
            timestamp=int(time.time() * 1000))
    except Exception as e:
        print('xxxxx--cex sell exception: {}'.format(str(e)))
        print('xxxxx--cex sell exception: {}'.format(sell_action))
        return
    binance.update_margin_balances = False
    print('cex {} succeed'.format(sell_action))

def spot_buy(asset, amount, price):
    buy_action = 'buy {}, amount {}, price {}'.format(asset, amount, price)
    order = None
    org_amount = amount
    org_price = price
    try:
        symbol=asset + 'USDT'
        if asset in busd_pair_assets:
            symbol=asset + 'BUSD'
        side=client.SIDE_BUY
        precis = binance.precisions[symbol]
        price = round_down(price, precis[0])
        amount = round_down(amount, precis[1])
        order = client.create_order(
            symbol=symbol,
            side=side,
            type=client.ORDER_TYPE_MARKET,
            quantity=amount,
            timestamp=int(time.time() * 1000))
    except Exception as e:
        print('xxxxx--cex buy exception: {}'.format(str(e)))
        print('xxxxx--cex buy exception: {}'.format(buy_action))
        return
    binance.update_spot_balances = False
    print('cex {} succeed'.format(buy_action))

def spot_sell(asset, amount, price):
    sell_action = 'sell {}, amount {}, price {}'.format(asset, amount, price)
    order = None
    org_price = price
    try:
        symbol=asset + 'USDT'
        if asset in busd_pair_assets:
            symbol=asset + 'BUSD'
        side=client.SIDE_SELL
        precis = binance.precisions[symbol]
        price = round_down(price, precis[0])
        amount = round_down(amount, precis[1])
        order = client.create_order(
            symbol=symbol,
            side=side,
            type=client.ORDER_TYPE_MARKET,
            quantity=amount,
            timestamp=int(time.time() * 1000))
    except Exception as e:
        print('xxxxx--cex sell exception: {}'.format(str(e)))
        print('xxxxx--cex sell exception: {}'.format(sell_action))
        return
    binance.update_spot_balances = False
    print('cex {} succeed'.format(sell_action))

cex_assets = ['DAI', 'WBTC', 'CHR']

trx_unique_assets = ['TRX', 'BTT', 'JST', 'WIN', 'SUN']

trx_assets = ['TRX', 'USDT', 'BTT', 'BTC', 'JST', 'WIN', 'SUN', 'ETH', 'WBTC']

USDT='TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t'
BTT='TKfjV9RNKJJCqPvBtK8L7Knykh7DNWvnYt'
BTC='TN3W4H6rK2ce4vX9YnFQHwKENnHjoxb3m9'
JST='TCFLL5dx5ZJdKnWuesXxi1VPwjLVmWZZy9'
WIN='TLa2f6VPqDgRE67v1736s7bJ8Ray5wYjU7'
SUN='TKkeiboTkxXKJpbmVFbv4a8ov5rAfRDMf9'
ETH='THb4CqiFdwNHsWsQCs4JhzwjMWys4aqCbF'
WBTC='TXpw8XeWYeTUd4quDskoUqeQPowRh4jY65'

USDT_TRX='TQn9Y2khEsLJW1ChVWFMSMeRDow5KcbLSE'
BTT_TRX='TH2mEwTKNgtg8psR6Qx2RBUXZ48Lon1ygu'
BTC_TRX='TKAtLoCB529zusLfLVkGvLNis6okwjB7jf'
JST_TRX='TYukBQZ2XXCcRCReAUguyXncCWNY9CEiDQ'
WIN_TRX='TYN6Wh11maRfzgG7n5B6nM5VW1jfGs9chu'
SUN_TRX='TUEYcyPAqc4hTg1fSuBCPc18vGWcJDECVw'
ETH_TRX='TVrZ3PjjFGbnp44p6SGASAKrJWAUjCHmCA'
WBTC_TRX='TT21D6nXBGMJjUqtth4HntD6NULWAmkLib'

trx_client = Tron()

query_abi = '''
[
	{
		"constant": true,
		"inputs": [
			{
				"internalType": "address",
				"name": "admin",
				"type": "address"
			},
			{
				"internalType": "address[]",
				"name": "tokens",
				"type": "address[]"
			},
			{
				"internalType": "address[]",
				"name": "pair_list",
				"type": "address[]"
			}
		],
		"name": "get_all_information",
		"outputs": [
			{
				"components": [
					{
						"internalType": "uint256",
						"name": "reserve0",
						"type": "uint256"
					},
					{
						"internalType": "uint256",
						"name": "reserve1",
						"type": "uint256"
					}
				],
				"internalType": "struct MQuery.Reserve[]",
				"name": "",
				"type": "tuple[]"
			},
			{
				"internalType": "uint256[]",
				"name": "",
				"type": "uint256[]"
			}
		],
		"payable": false,
		"stateMutability": "view",
		"type": "function"
	}
]
'''
trx_assets_precisions = {'TRX': 6, 'USDT': 6, 'BTT': 6, 'BTC': 8, 'JST': 18, 'WIN': 6, 'SUN': 18, 'ETH': 18, 'WBTC': 8}
q_abi = json.loads(query_abi)
trx_query = trx_client.get_contract('TG4qLmPTPYrWs25vGBwm6ogZzhYHAZuhcn')
trx_query.abi = q_abi
def get_tron_chain_balances():
    balances_dic = {}
    asset_addrs = [eval(asset) for asset in trx_assets[1:]]
    pairs = [eval(asset+'_TRX') for asset in trx_assets[1:]]
    (reserves, balances) = trx_query.functions.get_all_information('TZFUe7XD2Di4G4pha8uhQnAXfstfMbRTp6', asset_addrs, pairs)
    for i in range(len(trx_assets)):
        balances_dic[trx_assets[i]] = balances[i] / 10 ** trx_assets_precisions[trx_assets[i]]
    return balances_dic

def print_json(data):
    print(json.dumps(data, sort_keys=True, indent=4, separators=(', ', ': '), ensure_ascii=False))
 
if __name__ == "__main__":
    init_balance = {}
    base_dir = os.path.dirname(os.path.abspath(__file__))
    if base_dir is None:
        base_dir = os.getcwd()
    with open(base_dir+'/startbalance',"r",encoding="utf-8") as f:
        s = f.read().replace('\n','')
        init_balance = eval(s)
    dex_swap = DexSwap(None,False)
    start=time.time()
    dex_swap.update_information()
    end=time.time()
    print('dex_swap.update_information running time: %s Seconds'%(end-start))

    start=time.time()
    tickers = client.get_all_tickers()
    margin_balances = get_margin_balance()
    spot_balances = get_spot_balances()
    end=time.time()
    print('binance get balance running time: %s Seconds'%(end-start))

    assets += cex_assets + trx_unique_assets

    current_binance_balances = {}
    current_balances = {}
    diff_balances = {}
    for asset in assets:
        for asset_info in margin_balances:
            if asset_info['asset'] == asset:
                cex_netasset = float(asset_info['netAsset'])
                current_binance_balances[asset] = {'free': float(asset_info['free']), 'locked': float(asset_info['locked']), 'borrowed': float(asset_info['borrowed'])}
        for asset_info in spot_balances:
            if asset_info['asset'] == asset:
                cex_free = float(asset_info['free'])
                cex_locked = float(asset_info['locked'])
                if asset in current_binance_balances:
                    current_binance_balances[asset]['free'] += cex_free
                    current_binance_balances[asset]['locked'] += cex_locked
                else:
                    current_binance_balances[asset] = {'free': cex_free, 'locked': cex_locked, 'borrowed': 0}

    current_bsc_balances = {}
    for asset in assets:
        if asset in cex_assets:
            current_bsc_balances[asset] = 0
            continue
        if asset in trx_unique_assets:
            current_bsc_balances[asset] = 0
            continue
        dex_free = dex_swap.balances[asset]
        current_bsc_balances[asset] = dex_free / 10 ** 18
    tron_chain_balances = get_tron_chain_balances()
    for asset in assets:
        dex_free = current_bsc_balances[asset]
        cex_netasset = current_binance_balances[asset]['free'] + current_binance_balances[asset]['locked'] - current_binance_balances[asset]['borrowed']
        current_balances[asset] = cex_netasset + dex_free + (tron_chain_balances[asset] if asset in tron_chain_balances else 0)
        diff_balances[asset] = current_balances[asset] - (init_balance[asset] if asset in init_balance else 0)

    binance.margin_get_balances()
    diff_value = 0
    for asset in assets:
        if asset == 'USDT' or asset in trx_unique_assets or asset in cex_assets:
            continue
        price = get_price(tickers, asset)
        diff_value = diff_balances[asset] * price
        diff_bal = diff_balances[asset]
        if asset in diff_balances:
            if abs(diff_value) > 20:
                if asset == 'BETH':
                    print('XXXXXXXXXX|||||||||||||XXXXXXXXX BETH diff is {}'.format(diff_bal))
                    continue
                if asset in spot_assets:
                    if diff_bal > 0 and current_binance_balances[asset]['free'] > diff_bal:
                        spot_sell(asset, diff_bal, price)
                    elif diff_bal < 0:
                        spot_buy(asset, abs(diff_bal), price)
                else:
                    if diff_bal > 0:
                        margin_sell(asset, diff_bal, price)
                    elif diff_bal < 0:
                        margin_buy(asset, abs(diff_bal), price)
            
                    
