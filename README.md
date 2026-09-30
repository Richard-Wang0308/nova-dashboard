# Reaction Monitor

A small web dashboard that shows the Nova **allowed reaction** for every epoch and
the **live progress of the current epoch** (blocks passed / left, time left,
scoring-finished and submit marks), plus the SN68 alpha price and TAO/USD
price in the header.

It's fully standalone. It doesn't import the nova project or bittensor, and it
reads the chain over plain Substrate JSON-RPC. The alpha price comes from the
chain (`SwapRuntimeApi_current_alpha_price`); the TAO/USD price comes from
CoinGecko, with Binance as a fallback.

## How the reaction is derived

It's the same as nova's `get_challenge_params_from_blockhash`:

```
start_block = epoch * 361
allowed     = rxn:( int(block_hash(start_block), 16) % 5 + 1 )
```

Past and current epochs are known. Future epochs are not, because their start
block doesn't exist yet.

## Setup on a fresh server (Ubuntu / Debian)

```bash
sudo apt update && sudo apt install -y python3 python3-venv
# copy this folder to the server, e.g. /opt/rxn-dashboard
cd /opt/rxn-dashboard
python3 -m venv venv
./venv/bin/pip install -r requirements.txt
./venv/bin/python server.py --port 8080
```

Open `http://<server-ip>:8080`. If you have a firewall, allow the port:
`sudo ufw allow 8080/tcp`.

### Run in the background (systemd)

```bash
sudo cp rxn-dashboard.service /etc/systemd/system/   # edit paths if not /opt/rxn-dashboard
sudo systemctl daemon-reload
sudo systemctl enable --now rxn-dashboard
journalctl -u rxn-dashboard -f                      # logs
```

## Options

Each option can be set as a command-line flag or as an environment variable.

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `--host` | `HOST` | `0.0.0.0` | bind address |
| `--port` | `PORT` | `8080` | HTTP port |
| `--network` | `NETWORK` | `finney` | `finney`, `test`, `local`, or a `ws(s)://` URL |
| `--epoch-length` | `EPOCH_LENGTH` | `361` | blocks per epoch |
| `--total-reactions` | `TOTAL_REACTIONS` | `5` | reaction count (`rxn:1..N`) |
| `--block-time` | `BLOCK_TIME` | `12` | seconds per block (for time estimates) |
| `--late-submit-remaining` | `LATE_SUBMIT_REMAINING` | `41` | marks where the submit window opens on the bar (`0` = hide) |
| `--scoring-finished-block` | `SCORING_FINISHED_BLOCK` | `261` | marks where scoring finishes on the bar, in blocks into the epoch (`0` = hide) |
| `--netuid` | `NETUID` | `68` | subnet whose alpha price is shown in the header |
| `--price-interval` | `PRICE_INTERVAL` | `30` | seconds between price refreshes |
| `--poll-interval` | `POLL_INTERVAL` | `3` | seconds between chain polls |

Hashes of past epochs are cached in `data/` so history loads instantly after the
first time.

## API

- `GET /api/status`: the current block, epoch progress, and allowed reaction
- `GET /api/epochs?before=<epoch>&limit=<n>`: past epochs, newest first
- `GET /api/epoch/<n>`: one epoch. A future epoch returns 404 with its start block and ETA.
