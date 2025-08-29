import os, json, asyncio, collections
import websockets, requests
from typing import Any, Dict, List, Set, Tuple

WS_URL = os.environ.get("HL_WS_URL", "").strip()          # e.g. wss://api.hyperliquid.xyz/ws
WALLETS = [w.strip().lower() for w in os.environ.get("WALLET_ADDRESSES", "").split(",") if w.strip()]
TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT  = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

PING_INTERVAL = float(os.environ.get("PING_INTERVAL", "20"))   # seconds
RECONNECT_BASE = float(os.environ.get("RECONNECT_BASE", "1.0"))
RECONNECT_MAX  = float(os.environ.get("RECONNECT_MAX", "20.0"))

# Behavior toggles
SEND_BASELINE_ON_SNAPSHOT = True  # set False if you want to suppress baseline entirely

if not WS_URL:
    raise RuntimeError("Set HL_WS_URL (e.g., wss://api.hyperliquid.xyz/ws).")
if not WALLETS:
    raise RuntimeError("Set WALLET_ADDRESSES (comma-separated 0x... addresses).")

def tg_send(text: str):
    if not (TG_TOKEN and TG_CHAT):
        print("TG:", text)
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={"chat_id": TG_CHAT, "text": text}, timeout=10)
        print("Telegram:", r.status_code, r.text[:160])
        r.raise_for_status()
    except Exception as e:
        print("Telegram error:", e)

def fmt_fill(f: Dict[str, Any]) -> str:
    coin = f.get("coin")
    side = f.get("side")
    px   = f.get("px")
    sz   = f.get("sz")
    t    = f.get("time")
    fee  = f.get("fee")
    crossed = f.get("crossed")
    parts = []
    if t is not None: parts.append(f"[{t}]")
    parts += ["fill", str(coin) if coin else "", str(side) if side else ""]
    if sz is not None: parts.append(f"sz={sz}")
    if px is not None: parts.append(f"px={px}")
    if fee is not None: parts.append(f"fee={fee}")
    if crossed is not None: parts.append("taker" if crossed else "maker")
    return " ".join([p for p in parts if p])

def fill_key(user: str, f: Dict[str, Any]) -> str:
    """
    Build a stable unique key for a fill. Prefer tid; then hash; then a tuple of fields.
    """
    tid = f.get("tid")
    hsh = f.get("hash")
    if tid is not None:
        return f"{user}:tid:{tid}"
    if hsh is not None:
        return f"{user}:hash:{hsh}"
    # fallback composite: time+coin+side+px+sz+oid
    coin = f.get("coin"); side = f.get("side"); px = f.get("px"); sz = f.get("sz"); oid = f.get("oid")
    t = f.get("time")
    return f"{user}:t:{t}|{coin}|{side}|{px}|{sz}|{oid}"

class SeenCache:
    """
    Per-wallet bounded cache to avoid duplicate alerts.
    Uses OrderedDict as an LRU set with max size.
    """
    def __init__(self, max_size: int = 2000):
        self.max_size = max_size
        self._maps: Dict[str, collections.OrderedDict] = {}  # user -> LRU keys

    def add_and_check(self, user: str, key: str) -> bool:
        """
        Returns True if key was already seen (i.e., duplicate).
        """
        lru = self._maps.setdefault(user, collections.OrderedDict())
        if key in lru:
            # move to end (recent)
            lru.move_to_end(key)
            return True
        # new key
        lru[key] = None
        if len(lru) > self.max_size:
            lru.popitem(last=False)
        return False

seen_cache = SeenCache(max_size=4000)
baseline_sent: Set[str] = set()  # track wallets we already baseline-notified for this process

async def subscribe_user(ws, addr: str):
    msg = {"method": "subscribe", "subscription": {"type": "userFills", "user": addr}}
    await ws.send(json.dumps(msg))

async def ws_loop():
    backoff = RECONNECT_BASE
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=PING_INTERVAL, ping_timeout=PING_INTERVAL*2) as ws:
                print("Connected to", WS_URL)
                for addr in WALLETS:
                    await subscribe_user(ws, addr)
                tg_send(f"HL WS connected. Subscribed to userFills for {len(WALLETS)} wallet(s).")

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    channel = msg.get("channel")
                    data = msg.get("data")

                    if channel == "subscriptionResponse":
                        # ack, ignore
                        continue

                    if channel == "userFills" and isinstance(data, dict):
                        user = (data.get("user") or "").lower()
                        fills = data.get("fills", [])
                        is_snapshot = bool(data.get("isSnapshot", False))

                        # Skip unrelated user (shouldn't happen)
                        if user not in WALLETS:
                            continue

                        if is_snapshot:
                            # Only send baseline once per process per wallet
                            if SEND_BASELINE_ON_SNAPSHOT and user not in baseline_sent:
                                if fills:
                                    last = fills[-1]
                                    key = fill_key(user, last)
                                    # Mark baseline as seen so we won't alert it again later
                                    seen_cache.add_and_check(user, key)
                                    tg_send(f"HL {user} baseline -> {fmt_fill(last)}")
                                baseline_sent.add(user)
                            else:
                                # still mark the snapshot fills as seen to avoid repeats
                                for f in fills[-5:]:  # mark last few as seen
                                    seen_cache.add_and_check(user, fill_key(user, f))
                            continue

                        # Streaming updates (isSnapshot == false)
                        for f in fills:
                            key = fill_key(user, f)
                            if seen_cache.add_and_check(user, key):
                                continue  # duplicate, skip
                            tg_send(f"HL {user} -> {fmt_fill(f)}")

        except asyncio.CancelledError:
            raise
        except Exception as e:
            print("WS loop error:", e)
            tg_send("HL WS disconnected; retrying soon…")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, RECONNECT_MAX)

def main():
    asyncio.run(ws_loop())

if __name__ == "__main__":
    main()