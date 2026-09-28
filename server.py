#!/usr/bin/env python3
"""
Nova epoch / allowed-reaction dashboard.

Standalone: talks to the Bittensor chain over raw Substrate JSON-RPC (websocket)
and does NOT import anything from the nova project.

The allowed reaction for an epoch is derived exactly like nova's
utils.challenge.get_challenge_params_from_blockhash():

    start_block      = epoch * EPOCH_LENGTH
    seed             = int(block_hash(start_block), 16)
    allowed_reaction = f"rxn:{seed % TOTAL_REACTIONS + 1}"

so it is known for past and current epochs, never for future ones.

Run:
    pip install -r requirements.txt
    python3 server.py --port 8080
"""

import argparse
import asyncio
import itertools
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional

import aiohttp
from aiohttp import web

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
DATA_DIR = BASE_DIR / "data"

NETWORKS = {
    "finney": "wss://entrypoint-finney.opentensor.ai:443",
    "test": "wss://test.finney.opentensor.ai:443",
    "local": "ws://127.0.0.1:9944",
}

# Names from nova's combinatorial_db `reactions` table.
REACTION_NAMES = {
    1: "triazole",
    2: "reductive_amination",
    3: "click_amide_cascade",
    4: "suzuki_bromide",
    5: "suzuki_bromide_then_chloride",
}

# TAO/USD sources, tried in order.
TAO_USD_SOURCES = (
    ("https://api.coingecko.com/api/v3/simple/price?ids=bittensor&vs_currencies=usd",
     lambda d: float(d["bittensor"]["usd"])),
    ("https://api.binance.com/api/v3/ticker/price?symbol=TAOUSDT",
     lambda d: float(d["price"])),
)

# A block hash is only cached once it is this far behind the head, so a
# short reorg can never leave a wrong hash stuck in the cache.
CACHE_SAFETY_BLOCKS = 20

log = logging.getLogger("rxn-dashboard")


# ============================================================================
# Substrate JSON-RPC client (one persistent websocket, auto-reconnect)
# ============================================================================

class RPCError(Exception):
    pass


class SubstrateRPC:
    def __init__(self, url: str):
        self.url = url
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._pending: Dict[int, asyncio.Future] = {}
        self._ids = itertools.count(1)
        self._lock = asyncio.Lock()

    async def _ensure(self) -> None:
        async with self._lock:
            if self._ws is not None and not self._ws.closed:
                return
            if self._session is None:
                self._session = aiohttp.ClientSession()
            ws = await self._session.ws_connect(
                self.url, heartbeat=20, max_msg_size=0,
                timeout=aiohttp.ClientWSTimeout(ws_close=10),
            )
            pending: Dict[int, asyncio.Future] = {}
            self._ws, self._pending = ws, pending
            asyncio.create_task(self._read_loop(ws, pending))
            log.info("connected to %s", self.url)

    async def _read_loop(self, ws, pending: Dict[int, asyncio.Future]) -> None:
        try:
            async for msg in ws:
                if msg.type != aiohttp.WSMsgType.TEXT:
                    continue
                data = json.loads(msg.data)
                fut = pending.pop(data.get("id"), None)
                if fut is None or fut.done():
                    continue
                if "error" in data:
                    fut.set_exception(RPCError(str(data["error"])))
                else:
                    fut.set_result(data.get("result"))
        except Exception as e:  # pragma: no cover
            log.warning("websocket reader stopped: %s", e)
        finally:
            for fut in pending.values():
                if not fut.done():
                    fut.set_exception(ConnectionError("websocket closed"))
            pending.clear()

    async def _reset(self) -> None:
        ws, self._ws = self._ws, None
        if ws is not None and not ws.closed:
            try:
                await ws.close()
            except Exception:
                pass

    async def call(self, method: str, params: Optional[list] = None, timeout: float = 15) -> Any:
        last_exc: Optional[Exception] = None
        for _ in range(2):
            req_id = None
            try:
                await self._ensure()
                req_id = next(self._ids)
                fut = asyncio.get_running_loop().create_future()
                self._pending[req_id] = fut
                await self._ws.send_json(
                    {"jsonrpc": "2.0", "id": req_id, "method": method, "params": params or []}
                )
                return await asyncio.wait_for(fut, timeout)
            except RPCError:
                raise
            except (aiohttp.ClientError, ConnectionError, asyncio.TimeoutError, OSError) as e:
                last_exc = e
                if req_id is not None:
                    self._pending.pop(req_id, None)
                await self._reset()
        raise ConnectionError(f"RPC {method} failed: {last_exc}")

    async def close(self) -> None:
        await self._reset()
        if self._session is not None:
            await self._session.close()


# ============================================================================
# Chain state + epoch hash cache
# ============================================================================

class Monitor:
    def __init__(self, args: argparse.Namespace):
        self.network = args.network
        self.url = NETWORKS.get(args.network, args.network)
        self.epoch_length = args.epoch_length
        self.total_reactions = args.total_reactions
        self.block_time = args.block_time
        self.late_submit_remaining = args.late_submit_remaining
        self.scoring_finished_block = args.scoring_finished_block
        self.netuid = args.netuid
        self.price_interval = args.price_interval
        self.poll_interval = args.poll_interval

        self.rpc = SubstrateRPC(self.url)
        self.head: Optional[int] = None
        self.head_seen_at: float = 0.0
        self.last_ok: float = 0.0
        self.error: Optional[str] = None

        DATA_DIR.mkdir(exist_ok=True)
        safe_name = "".join(c if c.isalnum() else "_" for c in self.network)
        self.cache_path = DATA_DIR / f"epoch_hashes_{safe_name}_{self.epoch_length}.json"
        self.cache: Dict[int, str] = self._load_cache()
        self._cache_dirty = False
        self._fetch_sem = asyncio.Semaphore(16)

        self.alpha_tao: Optional[float] = None
        self.tao_usd: Optional[float] = None
        self.prices_at: float = 0.0

    # ---- cache ------------------------------------------------------------
    def _load_cache(self) -> Dict[int, str]:
        try:
            with open(self.cache_path, "r", encoding="utf-8") as f:
                return {int(k): v for k, v in json.load(f).items()}
        except FileNotFoundError:
            return {}
        except Exception as e:
            log.warning("ignoring unreadable cache %s: %s", self.cache_path, e)
            return {}

    def _save_cache(self) -> None:
        if not self._cache_dirty:
            return
        tmp = self.cache_path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({str(k): v for k, v in sorted(self.cache.items())}, f)
        os.replace(tmp, self.cache_path)
        self._cache_dirty = False

    # ---- chain reads ------------------------------------------------------
    async def refresh_head(self) -> int:
        header = await self.rpc.call("chain_getHeader")
        number = int(header["number"], 16)
        if number != self.head:
            self.head = number
            self.head_seen_at = time.time()
        self.last_ok = time.time()
        self.error = None
        return number

    async def current_head(self) -> int:
        if self.head is None or time.time() - self.last_ok > 30:
            return await self.refresh_head()
        return self.head

    async def epoch_hash(self, epoch: int) -> Optional[str]:
        if epoch in self.cache:
            return self.cache[epoch]
        start = epoch * self.epoch_length
        async with self._fetch_sem:
            block_hash = await self.rpc.call("chain_getBlockHash", [start])
        if not block_hash:
            return None
        if self.head is not None and start <= self.head - CACHE_SAFETY_BLOCKS:
            self.cache[epoch] = block_hash
            self._cache_dirty = True
        return block_hash

    # ---- derived ----------------------------------------------------------
    def reaction(self, block_hash: str) -> Dict[str, Any]:
        rid = int(block_hash[2:], 16) % self.total_reactions + 1
        return {"id": rid, "label": f"rxn:{rid}", "name": REACTION_NAMES.get(rid, f"rxn:{rid}")}

    def epoch_record(self, epoch: int, block_hash: str, head: int) -> Dict[str, Any]:
        start = epoch * self.epoch_length
        return {
            "epoch": epoch,
            "start_block": start,
            "end_block": start + self.epoch_length - 1,
            "start_block_hash": block_hash,
            "reaction": self.reaction(block_hash),
            # Estimated wall-clock start (no per-block timestamp lookups).
            "approx_start_ts": self.head_seen_at - (head - start) * self.block_time,
        }

    async def status(self) -> Dict[str, Any]:
        head = await self.current_head()
        L = self.epoch_length
        epoch = head // L
        start = epoch * L
        into = head - start
        remaining = L - into
        block_hash = await self.epoch_hash(epoch)
        trigger = L - self.late_submit_remaining if self.late_submit_remaining > 0 else None
        scoring = self.scoring_finished_block if 0 < self.scoring_finished_block < L else None
        return {
            "network": self.network,
            "endpoint": self.url,
            "connected": time.time() - self.last_ok < 30,
            "error": self.error,
            "server_time": time.time(),
            "head_seen_at": self.head_seen_at,
            "epoch_length": L,
            "block_time": self.block_time,
            "total_reactions": self.total_reactions,
            "reaction_names": REACTION_NAMES,
            "current_block": head,
            "current_epoch": epoch,
            "epoch_start_block": start,
            "epoch_end_block": start + L - 1,
            "next_epoch_start_block": start + L,
            "blocks_into_epoch": into,
            "blocks_remaining": remaining,
            "late_submit_trigger_block": trigger,
            "scoring_finished_block": scoring,
            "prices": self.prices(),
            "start_block_hash": block_hash,
            "reaction": self.reaction(block_hash) if block_hash else None,
        }

    # ---- prices -----------------------------------------------------------
    async def fetch_alpha_price(self) -> float:
        """Subnet alpha price in TAO, via the Swap runtime API (u16 netuid -> u64 rao)."""
        result = await self.rpc.call(
            "state_call", ["SwapRuntimeApi_current_alpha_price", "0x" + self.netuid.to_bytes(2, "little").hex()]
        )
        return int.from_bytes(bytes.fromhex(result[2:]), "little") / 1e9

    async def fetch_tao_usd(self) -> float:
        last_exc: Optional[Exception] = None
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            for url, parse in TAO_USD_SOURCES:
                try:
                    async with session.get(url) as r:
                        r.raise_for_status()
                        return parse(await r.json(content_type=None))
                except Exception as e:
                    last_exc = e
        raise ConnectionError(f"TAO/USD unavailable: {last_exc}")

    async def price_loop(self) -> None:
        while True:
            alpha, usd = await asyncio.gather(
                self.fetch_alpha_price(), self.fetch_tao_usd(), return_exceptions=True
            )
            for name, value in (("alpha price", alpha), ("TAO/USD", usd)):
                if isinstance(value, BaseException):
                    log.warning("%s fetch failed: %s", name, value)
            if not isinstance(alpha, BaseException):
                self.alpha_tao = alpha
            if not isinstance(usd, BaseException):
                self.tao_usd = usd
            if not isinstance(alpha, BaseException) or not isinstance(usd, BaseException):
                self.prices_at = time.time()
            await asyncio.sleep(self.price_interval)

    def prices(self) -> Dict[str, Any]:
        return {
            "netuid": self.netuid,
            "alpha_tao": self.alpha_tao,
            "tao_usd": self.tao_usd,
            "alpha_usd": self.alpha_tao * self.tao_usd
            if self.alpha_tao is not None and self.tao_usd is not None else None,
            "updated_at": self.prices_at or None,
        }

    async def poll_loop(self) -> None:
        while True:
            try:
                head = await self.refresh_head()
                await self.epoch_hash(head // self.epoch_length)
                self._save_cache()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.error = str(e)
                log.warning("poll failed: %s", e)
            await asyncio.sleep(self.poll_interval)


# ============================================================================
# HTTP handlers
# ============================================================================

def json_error(status: int, message: str, **extra) -> web.Response:
    return web.json_response({"error": message, **extra}, status=status)


async def handle_index(request: web.Request) -> web.FileResponse:
    return web.FileResponse(STATIC_DIR / "index.html", headers={"Cache-Control": "no-cache"})


async def handle_status(request: web.Request) -> web.Response:
    mon: Monitor = request.app["monitor"]
    try:
        return web.json_response(await mon.status())
    except Exception as e:
        return json_error(503, f"chain unavailable: {e}")


async def handle_epochs(request: web.Request) -> web.Response:
    """Past epochs, newest first: epochs [before-limit, before)."""
    mon: Monitor = request.app["monitor"]
    try:
        head = await mon.current_head()
    except Exception as e:
        return json_error(503, f"chain unavailable: {e}")
    current = head // mon.epoch_length
    try:
        before = int(request.query.get("before", current))
        limit = max(1, min(int(request.query.get("limit", 30)), 200))
    except ValueError:
        return json_error(400, "before/limit must be integers")
    before = min(before, current)  # past epochs only
    epochs = list(range(before - 1, max(-1, before - 1 - limit), -1))
    try:
        hashes = await asyncio.gather(*(mon.epoch_hash(e) for e in epochs))
    except Exception as e:
        return json_error(503, f"chain unavailable: {e}")
    mon._save_cache()
    items = [mon.epoch_record(e, h, head) for e, h in zip(epochs, hashes) if h]
    return web.json_response({
        "current_epoch": current,
        "items": items,
        "next_before": epochs[-1] if epochs and epochs[-1] > 0 else None,
    })


async def handle_epoch(request: web.Request) -> web.Response:
    mon: Monitor = request.app["monitor"]
    try:
        epoch = int(request.match_info["epoch"])
    except ValueError:
        return json_error(400, "epoch must be an integer")
    if epoch < 0:
        return json_error(400, "epoch must be >= 0")
    try:
        head = await mon.current_head()
    except Exception as e:
        return json_error(503, f"chain unavailable: {e}")
    current = head // mon.epoch_length
    if epoch > current:
        start = epoch * mon.epoch_length
        return json_error(
            404, "future epoch",
            epoch=epoch, current_epoch=current, start_block=start,
            blocks_until_start=start - head,
            eta_seconds=(start - head) * mon.block_time - (time.time() - mon.head_seen_at),
        )
    try:
        block_hash = await mon.epoch_hash(epoch)
    except Exception as e:
        return json_error(503, f"chain unavailable: {e}")
    if not block_hash:
        return json_error(404, "block hash not available")
    mon._save_cache()
    rec = mon.epoch_record(epoch, block_hash, head)
    rec["is_current"] = epoch == current
    return web.json_response(rec)


# ============================================================================
# App
# ============================================================================

def parse_args() -> argparse.Namespace:
    env = os.environ.get
    p = argparse.ArgumentParser(description="Nova epoch / allowed-reaction dashboard")
    p.add_argument("--host", default=env("HOST", "0.0.0.0"))
    p.add_argument("--port", type=int, default=int(env("PORT", "8080")))
    p.add_argument("--network", default=env("NETWORK", "finney"),
                   help="finney | test | local | ws(s)://custom-endpoint")
    p.add_argument("--epoch-length", type=int, default=int(env("EPOCH_LENGTH", "361")))
    p.add_argument("--total-reactions", type=int, default=int(env("TOTAL_REACTIONS", "5")))
    p.add_argument("--block-time", type=float, default=float(env("BLOCK_TIME", "12")))
    p.add_argument("--late-submit-remaining", type=int,
                   default=int(env("LATE_SUBMIT_REMAINING", "41")),
                   help="Blocks-remaining mark shown on the progress bar (0 = hide)")
    p.add_argument("--scoring-finished-block", type=int,
                   default=int(env("SCORING_FINISHED_BLOCK", "261")),
                   help="Blocks-into-epoch mark where scoring finishes (0 = hide)")
    p.add_argument("--netuid", type=int, default=int(env("NETUID", "68")),
                   help="Subnet whose alpha price is shown")
    p.add_argument("--price-interval", type=float, default=float(env("PRICE_INTERVAL", "30")),
                   help="Seconds between price refreshes")
    p.add_argument("--poll-interval", type=float, default=float(env("POLL_INTERVAL", "12")))
    return p.parse_args()


def build_app(args: argparse.Namespace) -> web.Application:
    app = web.Application()

    async def on_startup(app: web.Application) -> None:
        mon = Monitor(args)
        app["monitor"] = mon
        app["poller"] = asyncio.create_task(mon.poll_loop())
        app["pricer"] = asyncio.create_task(mon.price_loop())

    async def on_cleanup(app: web.Application) -> None:
        app["poller"].cancel()
        app["pricer"].cancel()
        mon: Monitor = app["monitor"]
        mon._save_cache()
        await mon.rpc.close()

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    app.router.add_get("/", handle_index)
    app.router.add_get("/api/status", handle_status)
    app.router.add_get("/api/epochs", handle_epochs)
    app.router.add_get("/api/epoch/{epoch}", handle_epoch)
    app.router.add_static("/static/", STATIC_DIR)
    return app


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    log.info("network=%s epoch_length=%d reactions=%d", args.network, args.epoch_length, args.total_reactions)
    web.run_app(build_app(args), host=args.host, port=args.port, access_log=None)


if __name__ == "__main__":
    main()
