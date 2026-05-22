#!/usr/bin/env python3
"""
One-time setup for EOA trading on Polymarket V2.
Run after depositing new USDC to EOA.
1. Approve V2 exchange contracts
2. Create/derive new API credentials
"""
from pathlib import Path
from dotenv import load_dotenv
import os, json

load_dotenv(Path(__file__).parent.parent / '.env')

PK = os.environ['POLYMARKET_PRIVATE_KEY']
V2_EXCHANGE     = '0xE111180000d2663C0091e4f400237545B87B996B'
V2_NEG_RISK     = '0xe2222d279d744050d28e00520010520000310F59'
V2_NEG_RISK_ADJ = '0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296'
USDC_NEW        = '0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB'
MAX_UINT256     = 2**256 - 1
CHAIN_ID        = 137
RPCS = [
    'https://polygon-bor-rpc.publicnode.com',
    'https://1rpc.io/matic',
]

from web3 import Web3
from eth_account import Account

acct = Account.from_key('0x' + PK)
print(f'EOA: {acct.address}')

# Connect RPC
w3 = None
for rpc in RPCS:
    _w3 = Web3(Web3.HTTPProvider(rpc))
    if _w3.is_connected():
        w3 = _w3
        print(f'Connected: {rpc}')
        break

if not w3:
    raise RuntimeError('No RPC available')

usdc = w3.eth.contract(
    address=Web3.to_checksum_address(USDC_NEW),
    abi=[{
        'name': 'approve', 'type': 'function', 'stateMutability': 'nonpayable',
        'inputs': [{'name': 'spender','type':'address'},{'name':'amount','type':'uint256'}],
        'outputs': [{'name':'','type':'bool'}],
    }, {
        'name': 'balanceOf', 'type': 'function', 'stateMutability': 'view',
        'inputs': [{'name':'account','type':'address'}],
        'outputs': [{'name':'','type':'uint256'}],
    }, {
        'name': 'allowance', 'type': 'function', 'stateMutability': 'view',
        'inputs': [{'name':'owner','type':'address'},{'name':'spender','type':'address'}],
        'outputs': [{'name':'','type':'uint256'}],
    }]
)

balance = usdc.functions.balanceOf(acct.address).call()
print(f'USDC balance: {balance/1e6:.6f}')
if balance == 0:
    print('ERROR: No new USDC found. Please deposit USDC to your EOA first.')
    exit(1)

nonce = w3.eth.get_transaction_count(acct.address)
gas_price = w3.eth.gas_price

for name, spender in [('V2 Exchange', V2_EXCHANGE), ('V2 Neg Risk Adj', V2_NEG_RISK_ADJ), ('V2 Neg Risk', V2_NEG_RISK)]:
    spender_cs = Web3.to_checksum_address(spender)
    current = usdc.functions.allowance(acct.address, spender_cs).call()
    if current >= MAX_UINT256 // 2:
        print(f'SKIP {name}: already approved (allowance={current//10**6})')
        continue

    tx = usdc.functions.approve(spender_cs, MAX_UINT256).build_transaction({
        'from': acct.address, 'nonce': nonce,
        'gas': 80000, 'gasPrice': gas_price, 'chainId': CHAIN_ID,
    })
    signed = acct.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    print(f'Approving {name}... tx={tx_hash.hex()}')
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
    status = 'OK' if receipt.status == 1 else 'FAILED'
    print(f'  {status} (gas={receipt.gasUsed})')
    nonce += 1

print('\nAll approvals done.')

# Create API key for EOA
print('\nCreating API key...')
from py_clob_client_v2.client import ClobClient

client = ClobClient(
    host='https://clob.polymarket.com',
    key=PK, chain_id=137,
    signature_type=0,
)
try:
    creds = client.create_or_derive_api_key()
    print(f'API Key:    {creds.api_key}')
    print(f'Secret:     {creds.api_secret}')
    print(f'Passphrase: {creds.api_passphrase}')

    env_path = Path(__file__).parent.parent / '.env'
    lines = env_path.read_text().splitlines()
    new_lines = []
    updated = set()
    for line in lines:
        key_name = line.split('=')[0].strip() if '=' in line else ''
        if key_name == 'POLYMARKET_API_KEY':
            new_lines.append(f'POLYMARKET_API_KEY={creds.api_key}')
            updated.add(key_name)
        elif key_name == 'POLYMARKET_SECRET':
            new_lines.append(f'POLYMARKET_SECRET={creds.api_secret}')
            updated.add(key_name)
        elif key_name == 'POLYMARKET_PASSPHRASE':
            new_lines.append(f'POLYMARKET_PASSPHRASE={creds.api_passphrase}')
            updated.add(key_name)
        elif key_name == 'POLYMARKET_SIGNATURE_TYPE':
            new_lines.append('POLYMARKET_SIGNATURE_TYPE=0')
            updated.add(key_name)
        else:
            new_lines.append(line)
    if 'POLYMARKET_SIGNATURE_TYPE' not in updated:
        new_lines.append('POLYMARKET_SIGNATURE_TYPE=0')
    env_path.write_text('\n'.join(new_lines) + '\n')
    print('\n.env updated with new credentials.')
    print('Now set MODE=live in .env and restart expiry-sniper.py')
except Exception as e:
    print(f'API key creation failed: {e}')
    print('Manually update .env with credentials from Polymarket UI.')
