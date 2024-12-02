import csv
import socket
import json
import time
from datetime import datetime


def send_electrumx_request(sock, method, params):
    request = json.dumps({"id": 1, "method": method, "params": params}) + "\n"
    sock.sendall(request.encode("utf-8"))
    response = sock.recv(4096).decode("utf-8")
    return json.loads(response)


def get_balance(scripthash, server="127.0.0.1", port=50001, retries=3):
    for attempt in range(retries):
        try:
            with socket.create_connection((server, port), timeout=10) as sock:
                response = send_electrumx_request(sock, "blockchain.scripthash.get_balance", [scripthash])
                if "result" in response:
                    return response["result"]
                else:
                    print(f"Unexpected response for {scripthash}: {response}")
                    return {"confirmed": 0, "unconfirmed": 0}
        except Exception as e:
            print(f"Attempt {attempt + 1} failed for {scripthash}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                print(f"Failed to fetch balance for {scripthash} after {retries} attempts.")
    return {"confirmed": 0, "unconfirmed": 0}


def monitor_balances(input_file, output_file, server="127.0.0.1", port=50001, batch_size=25, delay=0.1):
    wallets = []
    state_hashes = {}

    # Load wallets from input file
    with open(input_file, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        wallets = [
            {
                "address": row["address"],
                "scripthash": row["scripthash"],
                "confirmed": 0,
                "unconfirmed": 0,
            }
            for row in reader
        ]

    print(f"{datetime.now()} - Subscribing and fetching initial balances...")
    for i in range(0, len(wallets), batch_size):
        batch = wallets[i:i + batch_size]
        with socket.create_connection((server, port), timeout=20) as sock:
            for wallet in batch:
                scripthash = wallet["scripthash"]
                try:
                    print(f"Subscribing to {scripthash}...")
                    response = send_electrumx_request(sock, "blockchain.scripthash.subscribe", [scripthash])
                    state_hash = response.get("result")
                    if state_hash:
                        state_hashes[scripthash] = state_hash
                        print(f"Subscribed to {scripthash}, initial state hash: {state_hash}")

                        # Fetch initial balance
                        balance = get_balance(scripthash, server, port)
                        wallet["confirmed"] = balance.get("confirmed", 0)
                        wallet["unconfirmed"] = balance.get("unconfirmed", 0)

                    else:
                        print(f"Failed to subscribe to {scripthash}: {response}")

                except Exception as e:
                    print(f"Error subscribing or fetching balance for {scripthash}: {e}")

                # Throttle requests slightly
                time.sleep(delay)

    print(f"{datetime.now()} - Initial subscription completed. Monitoring for changes...")

    while True:
        changes_detected = 0
        confirmed_total = 0
        unconfirmed_total = 0
        cycle_start_time = time.time()

        for wallet in wallets:
            scripthash = wallet["scripthash"]
            try:
                # Fetch balance for each wallet
                balance = get_balance(scripthash, server, port)
                old_confirmed = wallet["confirmed"]
                old_unconfirmed = wallet["unconfirmed"]

                wallet["confirmed"] = balance.get("confirmed", 0)
                wallet["unconfirmed"] = balance.get("unconfirmed", 0)

                confirmed_total += wallet["confirmed"]
                unconfirmed_total += wallet["unconfirmed"]

                if old_confirmed != wallet["confirmed"] or old_unconfirmed != wallet["unconfirmed"]:
                    print(f"Balance change detected for {scripthash}. Old: {old_confirmed}/{old_unconfirmed}, New: {wallet['confirmed']}/{wallet['unconfirmed']}")
                    changes_detected += 1

            except Exception as e:
                print(f"Error monitoring {scripthash}: {e}")

            # Throttle requests slightly
            time.sleep(delay)

        # Write changes to output file
        with open(output_file, "w", newline="") as csvfile:
            fieldnames = ["address", "scripthash", "confirmed", "unconfirmed"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(wallets)

        cycle_end_time = time.time()
        cycle_duration = cycle_end_time - cycle_start_time

        print(f"{datetime.now()} - Cycle completed in {cycle_duration:.2f} seconds.")
        print(f"Confirmed total: {confirmed_total} satoshis")
        print(f"Unconfirmed total: {unconfirmed_total} satoshis")
        print(f"Detected balance changes in {changes_detected} wallets.")

        time.sleep(1)  # Delay between cycles


# Example usage
input_file = "input_csv.csv"  # Input file path
output_file = "ScripthashBalances.csv"  # Output file path
monitor_balances(input_file, output_file)

