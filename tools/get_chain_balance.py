import sys
import os
from os import path
parent_path = os.path.dirname(os.getcwd())
sys.path.append(parent_path)
from arbitrage.config import *
from binance.client import Client
import time
import json
from brownie import *
from web3 import Web3

ftm_unique_assets = ['FTM', 'USDC', 'CRV', 'SNX', 'AAVE']
ftm_assets = ['FTM', 'BTC', 'ETH', 'USDC', 'SUSHI', 'CRV', 'LINK', 'YFI', 'USDT', 'DAI', 'SNX', 'AAVE']


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
trx_assets_precisions = {'TRX': 6, 'USDT': 6, 'BTT': 6, 'BTC': 8, 'JST': 18, 'WIN': 6, 'SUN': 18, 'ETH': 18, 'WBTC': 8, 'TUSD': 18}
ftm_assets_precisions = {'FTM': 18, 'BTC': 8, 'ETH': 18, 'USDC': 6, 'SUSHI': 18, 'CRV': 18, 'LINK': 18, 'YFI': 18, 'USDT': 6, 
    'DAI': 18, 'SNX': 18, 'AAVE': 18}

from brownie import *

FBTC='0x321162Cd933E2Be498Cd2267a90534A804051b11'
FETH='0x74b23882a30290451A17c44f4F05243b6b58C76d'
FUSDC='0x04068DA6C83AFCFA0e13ba15A6696662335D5B75'
FSUSHI='0xae75A438b2E0cB8Bb01Ec1E1e376De11D44477CC'
FCRV='0x1E4F97b9f9F913c46F1632781732927B9019C68b'
FLINK='0xb3654dc3D10Ea7645f8319668E8F54d2574FBdC8'
FYFI='0x29b0Da86e484E1C0029B56e817912d778aC0EC69'
FUSDT='0x049d68029688eAbF473097a2fC38ef61633A3C7A'
FDAI='0x8D11eC38a3EB5E956B052f67Da8Bdc9bef8Abf3E'
FAAVE='0x6a07A792ab2965C72a5B8088d3a069A7aC3a993B'
FSNX='0x56ee926bD8c72B2d5fa1aF4d9E4Cbb515a1E3Adc'

BTC_FTM='0x279b2c897737a50405ED2091694F225D83F2D3bA'
ETH_FTM='0x613BF4E46b4817015c01c6Bb31C7ae9edAadc26e'
USDC_FTM='0xe7E90f5a767406efF87Fdad7EB07ef407922EC1D'
SUSHI_FTM='0x9Fe4c0CE5F533e96C2b72d852f190961AD5a7bB3'
CRV_FTM='0x374C8ACb146407Ef0AE8F82BaAFcF8f4EC1708CF'
LINK_FTM='0xd061c6586670792331E14a80f3b3Bb267189C681'
YFI_FTM='0x4fc38a2735C7da1d71ccAbf6DeC235a7DA4Ec52C'
DAI_FTM='0xe120ffBDA0d14f3Bb6d6053E90E63c572A66a428'
USDT_FTM='0x5965E53aa80a0bcF1CD6dbDd72e6A9b2AA047410'
SNX_FTM='0x06d173628bE105fE81F1C82c9979bA79eBCAfCB7'
AAVE_FTM='0xeBF374bB21D83Cf010cC7363918776aDF6FF2BF6'
CRV_FTM='0xB471Ac6eF617e952b84C6a9fF5de65A9da96C93B'

# def get_ftm_chain_balances():
# 	abi = json.loads(query_abi)
# 	# w3 = Web3(Web3.HTTPProvider('https://rpcapi.fantom.network/'))
# 	while(True):
# 		try:
# 			w3 = Web3(Web3.WebsocketProvider('wss://wsapi.fantom.network'))
# 			query = w3.eth.contract('0x7CAE4F73eAFc482efA0d02205C6e6E71c3cdcEEd', abi=abi)
# 			balances_dic = {}
# 			asset_addrs = [eval('F'+asset) for asset in ftm_assets[1:]]
# 			pairs = [eval(asset+'_FTM') for asset in ftm_assets[1:]]
# 			(reserves, balances) = query.functions.get_all_information('0x9D945d909Ca91937d19563e30bB4DAc12C860189', asset_addrs, pairs).call()
# 			for i in range(len(ftm_assets)):
# 				balances_dic[ftm_assets[i]] = balances[i] / 10 ** ftm_assets_precisions[ftm_assets[i]]
# 			return balances_dic
# 			break
# 		except Exception as e:
# 			print(e)
# 			time.sleep(1)

def get_ftm_chain_balances():
	abi = json.loads(query_abi)
	# w3 = Web3(Web3.HTTPProvider('https://rpcapi.fantom.network/'))
	w3 = Web3(Web3.WebsocketProvider('wss://wsapi.fantom.network'))
	query = w3.eth.contract('0x7CAE4F73eAFc482efA0d02205C6e6E71c3cdcEEd', abi=abi)
	balances_dic = {}
	asset_addrs = [eval('F'+asset) for asset in ftm_assets[1:]]
	pairs = [eval(asset+'_FTM') for asset in ftm_assets[1:]]
	loop = 0
	while(True):
		loop += 1
		(reserves, balances) = query.functions.get_all_information('0x9D945d909Ca91937d19563e30bB4DAc12C860189', asset_addrs, pairs).call()
		for i in range(len(ftm_assets)):
			balances_dic[ftm_assets[i]] = balances[i] / 10 ** ftm_assets_precisions[ftm_assets[i]]
		if balances_dic['FTM'] > 50000:
			print('tick {} balance={}'.format(loop, balances_dic['FTM']))
		time.sleep(0.3)
		print(balances_dic['FTM'])

get_ftm_chain_balances()