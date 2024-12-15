# Monitor_BTC_Balances.py
# Copyright (C) 2024 Adam P Baguley
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License Version 3 as published by
# the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License Version 3 for more details.
#https://www.gnu.org/licenses/.

import csv
import subprocess
import json
import time
import threading
import requests
from datetime import datetime, timezone
import socket
import threading

# Global dictionary to store BTC price/volume data
latest_data = {
    "Binance": {"price": None, "volume": None},
    "KuCoin": {"price": None, "volume": None},
    "Kraken": {"price": None, "volume": None}
}

# Flag to stop threads
stop_flag = False
lock = threading.Lock()  # For thread-safe access to latest_data

# Convert satoshis to BTC
def satoshis_to_btc(satoshis):
    """Convert satoshis to BTC."""
    return round(satoshis / 1e8, 8)

# Fetch wallet balances
def get_wallet_balances(wallets, server, retries=3, retry_delay=2):
    """Fetch balances for multiple wallets from the ElectrumX server."""
    results = {}
    for attempt in range(retries):
        try:
            with socket.create_connection((server["host"], server["port"]), timeout=10) as sock:
                for wallet in wallets:
                    scripthash = wallet["scripthash"]
                    request = json.dumps({
                        "id": f"{scripthash}",
                        "method": "blockchain.scripthash.get_balance",
                        "params": [scripthash]
                    }) + "\n"
                    sock.sendall(request.encode("utf-8"))
                    response = sock.recv(4096).decode("utf-8")
                    data = json.loads(response)
                    if "result" in data:
                        results[scripthash] = data["result"]
        except Exception as e:
            print(f"Error fetching wallet balances: {e}")
            time.sleep(retry_delay ** attempt)
        if results:
            break
    return results

# Fetch mempool data using Bitcoin Core
def get_mempool_info():
    """Fetch mempool information from Bitcoin Core."""
    try:
        result = subprocess.run(
            ["snap", "run", "bitcoin-core.cli", "-rpcport=28322", "getmempoolinfo"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        return json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error fetching mempool info: {e.stderr}")
        return {}

# Fetch block height
def get_block_height():
    """Fetch block height from Bitcoin Core."""
    try:
        result = subprocess.run(
            ["snap", "run", "bitcoin-core.cli", "-rpcport=28322", "getblockcount"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        return int(result.stdout.strip())
    except subprocess.CalledProcessError as e:
        print(f"Error fetching block height: {e.stderr}")
        return None

# Fetch price and volume data from exchanges
def fetch_binance_data():
    while not stop_flag:
        try:
            response = requests.get("https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT", timeout=5)
            data = response.json()
            with lock:
                latest_data["Binance"]["price"] = float(data['lastPrice'])
                latest_data["Binance"]["volume"] = float(data['volume'])
        except Exception as e:
            print(f"[ERROR] Binance fetch failed: {e}")
        time.sleep(1)

def fetch_kucoin_data():
    while not stop_flag:
        try:
            response = requests.get("https://api.kucoin.com/api/v1/market/stats?symbol=BTC-USDT", timeout=5)
            data = response.json()
            if "data" in data:
                with lock:
                    latest_data["KuCoin"]["price"] = float(data['data']['last'])
                    latest_data["KuCoin"]["volume"] = float(data['data']['volValue'])
        except Exception as e:
            print(f"[ERROR] KuCoin fetch failed: {e}")
        time.sleep(1)

def fetch_kraken_data():
    while not stop_flag:
        try:
            response = requests.get("https://api.kraken.com/0/public/Ticker?pair=XXBTZUSD", timeout=5)
            data = response.json()
            if "result" in data and "XXBTZUSD" in data["result"]:
                with lock:
                    latest_data["Kraken"]["price"] = float(data["result"]["XXBTZUSD"]["c"][0])
                    latest_data["Kraken"]["volume"] = float(data["result"]["XXBTZUSD"]["v"][1])
        except Exception as e:
            print(f"[ERROR] Kraken fetch failed: {e}")
        time.sleep(1)

# Update and write CSV
def update_csv(wallets, csv_file, mempool_info, btc_price, btc_volume, blockheight):
    """Update the CSV file with the latest data."""
    timestamp = datetime.now(timezone.utc).isoformat()
    with open(csv_file, "w", newline="") as csvfile:
        fieldnames = [
            "timestamp", "address", "confirmed", "unconfirmed", "mempool_size",
            "mempool_bytes", "btc_price", "btc_volume", "blockheight",
            "activity_count", "activity_magnitude"
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for wallet in wallets:
            writer.writerow({
                "timestamp": timestamp,
                "address": wallet["address"],
                "confirmed": satoshis_to_btc(wallet["confirmed"]),
                "unconfirmed": satoshis_to_btc(wallet["unconfirmed"]),
                "mempool_size": mempool_info.get("size", 0),
                "mempool_bytes": mempool_info.get("bytes", 0),
                "btc_price": btc_price,
                "btc_volume": btc_volume,
                "blockheight": blockheight,
                "activity_count": wallet["activity_count"],
                "activity_magnitude": satoshis_to_btc(wallet["activity_magnitude"])
            })

# Monitor balances and update CSV
def monitor_wallets(input_file, csv_file, server):
    """Main function to monitor wallet balances."""
    wallets = []

    # Load wallets from input file
    with open(input_file, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        wallets = [
            {
                "address": row["address"],
                "scripthash": row["scripthash"],
                "confirmed": 0,
                "unconfirmed": 0,
                "activity_count": 0,
                "activity_magnitude": 0
            }
            for row in reader
        ]

    # Start price threads
    threading.Thread(target=fetch_binance_data, daemon=True).start()
    threading.Thread(target=fetch_kucoin_data, daemon=True).start()
    threading.Thread(target=fetch_kraken_data, daemon=True).start()

    while True:
        print(f"[{datetime.now(timezone.utc)}] Fetching wallet balances...")

        # Fetch wallet balances
        balances = get_wallet_balances(wallets, server)
        mempool_info = get_mempool_info()
        blockheight = get_block_height()

        # Calculate BTC price/volume average
        with lock:
            prices = [exchange["price"] for exchange in latest_data.values() if exchange["price"] is not None]
            volumes = [exchange["volume"] for exchange in latest_data.values() if exchange["volume"] is not None]
        btc_price = round(sum(prices) / len(prices), 2) if prices else 0
        btc_volume = round(sum(volumes), 2) if volumes else 0

        # Update wallets
        changes_detected = 0
        total_confirmed = 0
        total_unconfirmed_inflow = 0
        total_unconfirmed_outflow = 0

        for wallet in wallets:
            scripthash = wallet["scripthash"]
            if scripthash in balances:
                balance = balances[scripthash]
                old_confirmed = wallet["confirmed"]
                old_unconfirmed = wallet["unconfirmed"]
                wallet["confirmed"] = balance.get("confirmed", 0)
                wallet["unconfirmed"] = balance.get("unconfirmed", 0)

                # Update activity count and magnitude
                if old_confirmed != wallet["confirmed"] or old_unconfirmed != wallet["unconfirmed"]:
                    wallet["activity_count"] += 1
                    wallet["activity_magnitude"] += abs(wallet["confirmed"] - old_confirmed) + abs(wallet["unconfirmed"] - old_unconfirmed)
                    changes_detected += 1

                # Update totals
                total_confirmed += wallet["confirmed"]
                if wallet["unconfirmed"] > 0:
                    total_unconfirmed_inflow += wallet["unconfirmed"]
                else:
                    total_unconfirmed_outflow += abs(wallet["unconfirmed"])

        # Print summary
        print(f"Detected balance changes in {changes_detected} wallets.")
        print(f"Total confirmed balance: {satoshis_to_btc(total_confirmed):.8f} BTC")
        print(f"Unconfirmed inflow: {satoshis_to_btc(total_unconfirmed_inflow):.8f} BTC")
        print(f"Unconfirmed outflow: {satoshis_to_btc(total_unconfirmed_outflow):.8f} BTC")
        print(f"Block height: {blockheight}")
        print(f"Mempool size: {mempool_info.get('size', 0)} transactions")
        print(f"BTC price: {btc_price:.2f} USD")
        print(f"BTC volume: {btc_volume:.2f} BTC")

        # Update CSV
        update_csv(wallets, csv_file, mempool_info, btc_price, btc_volume, blockheight)

        time.sleep(1)

# Configuration
INPUT_FILE = "/home/adam/wallet_monitor/input_csv.csv"
CSV_FILE = "/home/adam/wallet_monitor/wallet_data.csv"
SERVER = {"host": "127.0.0.1", "port": 50001}

monitor_wallets(INPUT_FILE, CSV_FILE, SERVER)
