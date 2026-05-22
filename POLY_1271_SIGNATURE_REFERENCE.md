# Polymarket POLY_1271 / ERC-7739 Signature Implementation Analysis

## Overview

Based on the official Polymarket clob-client-v2 (TypeScript) and py-clob-client-v2 (Python) implementations, here's the complete signature architecture for V2 orders.

---

## Signature Types (SignatureTypeV2)

The Polymarket V2 system supports **4 signature types**:

```python
class SignatureTypeV2(IntEnum):
    EOA = 0                   # ECDSA EIP712 signatures (externally-owned accounts)
    POLY_PROXY = 1            # EIP712 signatures from Polymarket Proxy wallets
    POLY_GNOSIS_SAFE = 2      # EIP712 signatures from Polymarket Gnosis safes
    POLY_1271 = 3             # EIP1271 signatures (smart contract wallets/vaults)
```

---

## 1. Standard EIP712 Signature (Types 0, 1, 2)

### TypeScript: buildClobEip712Signature()

Used for **CLOB authentication** (separate from order signing):

```typescript
export const buildClobEip712Signature = async (
    signer: ClobSigner,
    chainId: Chain,
    timestamp: number,
    nonce: number,
    address?: string,
): Promise<string> => {
    const resolvedAddress = address ?? (await getSignerAddress(signer));
    const ts = timestamp.toString();

    const domain = {
        name: "ClobAuthDomain",
        version: "1",
        chainId: chainId,
    };

    const types = {
        ClobAuth: [
            { name: "address", type: "address" },
            { name: "timestamp", type: "string" },
            { name: "nonce", type: "uint256" },
            { name: "message", type: "string" },
        ],
    };
    const value = {
        address: resolvedAddress,
        timestamp: ts,
        nonce,
        message: MSG_TO_SIGN,
    };
    const sig = await signTypedDataWithSigner({
        signer,
        domain,
        types,
        value,
        primaryType: "ClobAuth",
    });
    return sig;
};
```

**Constants:**
```javascript
export const CLOB_DOMAIN_NAME = "ClobAuthDomain";
export const CLOB_VERSION = "1";
export const MSG_TO_SIGN = "This message attests that I control the given wallet";
```

**Used for:** API authentication (not order signing)

---

## 2. Order Signature Signing (EIP712 Standard Orders)

### Python: ExchangeOrderBuilderV2.build_order_signature()

For **EOA, POLY_PROXY, POLY_GNOSIS_SAFE** signature types:

```python
def build_order_signature(self, typed_data: dict) -> str:
    if typed_data["message"]["signatureType"] == int(SignatureTypeV2.POLY_1271):
        return self._build_poly_1271_order_signature(typed_data)

    # Standard EIP712 signing for types 0, 1, 2
    encoded = encode_typed_data(full_message=typed_data)
    signed = Account.sign_message(encoded, private_key=self.signer.private_key)
    return "0x" + signed.signature.hex()
```

**Order Typed Data Structure:**

```python
CTF_EXCHANGE_V2_ORDER_STRUCT = [
    {"name": "salt", "type": "uint256"},
    {"name": "maker", "type": "address"},
    {"name": "signer", "type": "address"},
    {"name": "tokenId", "type": "uint256"},
    {"name": "makerAmount", "type": "uint256"},
    {"name": "takerAmount", "type": "uint256"},
    {"name": "side", "type": "uint8"},
    {"name": "signatureType", "type": "uint8"},
    {"name": "timestamp", "type": "uint256"},
    {"name": "metadata", "type": "bytes32"},
    {"name": "builder", "type": "bytes32"},
]

EIP712_DOMAIN = [
    {"name": "name", "type": "string"},
    {"name": "version", "type": "string"},
    {"name": "chainId", "type": "uint256"},
    {"name": "verifyingContract", "type": "address"},
]
```

**Domain:**
```python
{
    "name": "Polymarket CTF Exchange",
    "version": "2",
    "chainId": 137,  # Polygon mainnet
    "verifyingContract": "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
}
```

---

## 3. POLY_1271 Signature (ERC-7739 Wrapped)

### Python: ExchangeOrderBuilderV2._build_poly_1271_order_signature()

The **POLY_1271 signature type (3)** uses an **ERC-7739-wrapped EIP1271 signature**. This allows smart contract wallets (deposit wallets) to sign orders.

#### Step-by-Step Signature Construction

```python
def _build_poly_1271_order_signature(self, typed_data: dict) -> str:
    message = typed_data["message"]
    
    # ========== STEP 1: Hash the ORDER ==========
    contents_hash = _keccak(
        primitive=abi_encode(
            [
                "bytes32",      # ORDER_TYPE_HASH
                "uint256",      # salt
                "address",      # maker
                "address",      # signer
                "uint256",      # tokenId
                "uint256",      # makerAmount
                "uint256",      # takerAmount
                "uint8",        # side
                "uint8",        # signatureType
                "uint256",      # timestamp
                "bytes32",      # metadata
                "bytes32",      # builder
            ],
            [
                ORDER_TYPE_HASH,
                int(message["salt"]),
                message["maker"],
                message["signer"],
                int(message["tokenId"]),
                int(message["makerAmount"]),
                int(message["takerAmount"]),
                int(message["side"]),
                int(message["signatureType"]),
                int(message["timestamp"]),
                _hex_to_bytes32(message["metadata"]),
                _hex_to_bytes32(message["builder"]),
            ],
        )
    )
    
    # ========== STEP 2: Hash the TypedDataSign Struct ==========
    # This wraps the order in an ERC-7739-style structure
    typed_data_sign_struct_hash = _keccak(
        primitive=abi_encode(
            [
                "bytes32",      # SOLADY_TYPE_HASH (custom type for ERC-7739)
                "bytes32",      # contents_hash (the order hash)
                "bytes32",      # "DepositWallet" name hash
                "bytes32",      # "1" version hash
                "uint256",      # chainId
                "address",      # signer (deposit wallet address)
                "bytes32",      # salt
            ],
            [
                SOLADY_TYPE_HASH,
                contents_hash,
                DEPOSIT_WALLET_NAME_HASH,
                DEPOSIT_WALLET_VERSION_HASH,
                self.chain_id,
                message["signer"],           # This is the deposit wallet address
                DEPOSIT_WALLET_DOMAIN_SALT,
            ],
        )
    )
    
    # ========== STEP 3: Compute EIP712 Digest ==========
    digest = _keccak(
        primitive=(
            b"\x19\x01" + self.app_domain_separator + typed_data_sign_struct_hash
        )
    )
    
    # ========== STEP 4: Sign the Digest ==========
    signed = Account._sign_hash(digest, private_key=self.signer.private_key)
    inner_signature = signed.signature.hex()
    if inner_signature.startswith("0x"):
        inner_signature = inner_signature[2:]
    
    # ========== STEP 5: Assemble the Final Signature ==========
    # POLY_1271 wraps the signature with metadata for on-chain verification
    contents_type = ORDER_TYPE_STRING.encode("utf-8").hex()
    contents_type_len = len(ORDER_TYPE_STRING).to_bytes(2, "big").hex()

    return (
        "0x"
        + inner_signature               # 65 bytes (ECDSA signature)
        + self.app_domain_separator.hex()   # 32 bytes (CTF domain separator)
        + contents_hash.hex()               # 32 bytes (order hash)
        + contents_type                     # Variable (ORDER_TYPE_STRING in hex)
        + contents_type_len                 # 2 bytes (length of ORDER_TYPE_STRING)
    )
```

#### Type Hashes (Constants)

```python
ORDER_TYPE_STRING = (
    "Order(uint256 salt,address maker,address signer,uint256 tokenId,"
    "uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,"
    "uint256 timestamp,bytes32 metadata,bytes32 builder)"
)

SOLADY_TYPE_STRING = (
    "TypedDataSign(Order contents,string name,string version,uint256 chainId,"
    "address verifyingContract,bytes32 salt)"
    f"{ORDER_TYPE_STRING}"
)

DOMAIN_TYPE_STRING = (
    "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
)

ORDER_TYPE_HASH = _keccak(text=ORDER_TYPE_STRING)
DOMAIN_TYPE_HASH = _keccak(text=DOMAIN_TYPE_STRING)
SOLADY_TYPE_HASH = _keccak(text=SOLADY_TYPE_STRING)
DEPOSIT_WALLET_NAME_HASH = _keccak(text="DepositWallet")
DEPOSIT_WALLET_VERSION_HASH = _keccak(text="1")
CTF_EXCHANGE_NAME_HASH = _keccak(text="Polymarket CTF Exchange")
CTF_EXCHANGE_VERSION_HASH = _keccak(text="2")
DEPOSIT_WALLET_DOMAIN_SALT = bytes.fromhex(BYTES32_ZERO.replace("0x", "").zfill(64))
```

#### Final POLY_1271 Signature Format

```
0x
  + {65 bytes: ECDSA signature (r, s, v)}
  + {32 bytes: CTF Exchange domain separator}
  + {32 bytes: Order contents hash}
  + {N bytes: ORDER_TYPE_STRING in hex (variable length)}
  + {2 bytes: Length of ORDER_TYPE_STRING (big-endian)}
```

**Total signature length:** 65 + 32 + 32 + N + 2 = 131 + N bytes

---

## 4. Order Building Flow

```python
def build_signed_order(self, order_data: OrderDataV2) -> SignedOrderV2:
    # Step 1: Build the order structure
    order = self.build_order(order_data)
    
    # Step 2: Build EIP712 typed data
    typed_data = self.build_order_typed_data(order)
    
    # Step 3: Sign (choose based on signature type)
    signature = self.build_order_signature(typed_data)
    
    # Step 4: Return signed order
    return SignedOrderV2(**{**dataclasses.asdict(order), "signature": signature})
```

### Order Structure (OrderV2)

```python
@dataclass
class OrderV2:
    salt: str                          # Random unique value
    maker: str                         # Liquidity provider address
    signer: str                        # EOA or deposit wallet address
    tokenId: str                       # Market outcome token ID
    makerAmount: str                   # Amount maker is offering
    takerAmount: str                   # Amount maker expects back
    side: Side                         # BUY or SELL
    signatureType: SignatureTypeV2     # 0=EOA, 1=PROXY, 2=GNOSIS, 3=POLY_1271
    timestamp: str                     # Unix timestamp (milliseconds)
    metadata: str                      # Additional metadata (hex, optional)
    builder: str                       # Builder address (optional)
    expiration: str                    # Order expiration (optional)
```

---

## 5. Key Differences: Standard EIP712 vs POLY_1271

| Aspect | Standard EIP712 | POLY_1271 |
|--------|-----------------|-----------|
| **Signature Type** | 0, 1, or 2 | 3 |
| **Signer** | EOA private key | EOA private key (on behalf of deposit wallet) |
| **Signer Field** | Equals signer address | Equals deposit wallet address |
| **Signature Format** | 65 bytes (r, s, v) | 131+ bytes (wrapped with metadata) |
| **On-Chain Verification** | Direct ECDSA recovery | ERC-7739 (Solady) signature format |
| **Use Case** | Individual traders | Smart contract wallets, vaults, custody |
| **Domain** | CTF Exchange Domain | CTF Exchange Domain (via SOLADY wrapper) |

---

## 6. Signature Validation Details

### Standard EIP712 (Types 0, 1, 2)
- Standard ECDSA signature: 65 bytes
- Direct recovery: `ecrecover(digest, v, r, s)`
- Signer address recovered directly from signature

### POLY_1271 (Type 3)
- Wrapped signature format with metadata
- On-chain validation uses ERC-7739 standard
- Allows smart contracts to verify orders
- Deposit wallet address stored in `signer` field
- Signature contains:
  1. Inner ECDSA signature (65 bytes)
  2. CTF domain separator hash (32 bytes)
  3. Order contents hash (32 bytes)
  4. ORDER_TYPE_STRING in hex (variable)
  5. Length of ORDER_TYPE_STRING (2 bytes)

---

## 7. Chain Parameters

```
Chain: Polygon Mainnet (ID: 137)
CTF Exchange: 0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E
USDC: 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174
```

---

## Sources

- [Polymarket py-clob-client-v2 Repository](https://github.com/Polymarket/py-clob-client-v2)
- [Polymarket clob-client-v2 Repository](https://github.com/Polymarket/clob-client-v2)
- [Polymarket API Documentation](https://docs.polymarket.com/api-reference/authentication)

