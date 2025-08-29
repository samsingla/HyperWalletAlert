import os
import json
import asyncio
import websockets
import requests
from typing import Any, Dict, List, Set

# e.g. wss://api.hyperliquid.xyz/ws
WS_URL = os.environ.get("HL_WS_URL", "").strip()
WALLETS = [w.strip().lower() for w in os.environ.get(
    "WALLET_ADDRESSES", "").split(",") if w.strip()]
TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

PING_INTERVAL = float(os.environ.get("PING_INTERVAL", "20"))   # seconds
RECONNECT_BASE = float(os.environ.get("RECONNECT_BASE", "1.0"))
RECONNECT_MAX = float(os.environ.get("RECONNECT_MAX", "20.0"))

if not WS_URL:
    raise RuntimeError("Set HL_WS_URL (e.g., wss://api.hyperliquid.xyz/ws).")
if not WALLETS:
    raise RuntimeError(
        "Set WALLET_ADDRESSES (comma-separated 0x... addresses).")


def tg_send(text: str):
    if not (TG_TOKEN and TG_CHAT):
        print("TG:", text)
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        r = requests.post(
            url, json={"chat_id": TG_CHAT, "text": text}, timeout=10)
        print("Telegram:", r.status_code, r.text[:160])
        r.raise_for_status()
    except Exception as e:
        print("Telegram error:", e)


def fmt_fill(f: Dict[str, Any]) -> str:
    coin = f.get("coin")
    side = f.get("side")
    px = f.get("px")
    sz = f.get("sz")
    t = f.get("time")
    fee = f.get("fee")
    crossed = f.get("crossed")
    parts = []
    if t is not None:
        parts.append(f"[{t}]")
    parts += ["fill", str(coin) if coin else "", str(side) if side else ""]
    if sz is not None:
        parts.append(f"sz={sz}")
    if px is not None:
        parts.append(f"px={px}")
    if fee is not None:
        parts.append(f"fee={fee}")
    if crossed is not None:
        parts.append("taker" if crossed else "maker")
    return " ".join([p for p in parts if p])


async def subscribe_user(ws, addr: str):
    # Per docs: { "method": "subscribe", "subscription": { "type": "userFills", "user": "<address>" } }
    msg = {"method": "subscribe", "subscription": {
        "type": "userFills", "user": addr}}
    await ws.send(json.dumps(msg))


async def ws_loop():
    backoff = RECONNECT_BASE
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=PING_INTERVAL, ping_timeout=PING_INTERVAL*2) as ws:
                print("Connected to", WS_URL)
                lower_wallets = WALLETS
                for addr in lower_wallets:
                    await subscribe_user(ws, addr)
                tg_send(
                    f"HL WS connected. Subscribed to userFills for {len(lower_wallets)} wallet(s).")

                # per-connection dedup, keys like user:tid/hash:time
                seen_ids: Set[str] = set()

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    channel = msg.get("channel")
                    data = msg.get("data")

                    # subscription ack
                    if channel == "subscriptionResponse":
                        continue

                    if channel == "userFills" and isinstance(data, dict):
                        user = (data.get("user") or "").lower()
                        fills = data.get("fills", [])
                        is_snapshot = bool(data.get("isSnapshot", False))

                        if is_snapshot:
                            # Send only last fill as baseline to avoid spam
                            if fills:
                                last = fills[-1]
                                key = f"{user}:{last.get('tid') or last.get('hash') or last.get('time')}"
                                if key not in seen_ids:
                                    seen_ids.add(key)
                                    tg_send(
                                        f"HL {user} baseline -> {fmt_fill(last)}")
                            continue

                        # Streaming updates
                        for f in fills:
                            tid = f.get("tid") or f.get(
                                "hash") or f.get("time")
                            key = f"{user}:{tid}"
                            if key in seen_ids:
                                continue
                            seen_ids.add(key)
                            tg_send(f"HL {user} -> {fmt_fill(f)}")

        except asyncio.CancelledError:
            raise
        except Exception as e:
            print("WS loop error:", e)
            tg_send("HL WS disconnected; retrying soon…")
            # exponential backoff
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, RECONNECT_MAX)


def main():
    asyncio.run(ws_loop())


if __name__ == "__main__":
    main()
