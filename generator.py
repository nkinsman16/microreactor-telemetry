import argparse
import socket
import json
import time

import pandas as pd
import numpy as np


# Load the CSV once
DF = pd.read_csv("simulator/dataset/Power Plant Data.csv")
# Columns: ['AT', 'V', 'AP', 'RH', 'PE']


def generate_reactor_reading(dataframe: pd.DataFrame) -> dict:
    """Generate one synthetic reading based on column mean/std."""
    reading = {}
    for col in dataframe.columns:
        mean = dataframe[col].mean()
        std = dataframe[col].std()
        reading[col] = float(np.random.normal(mean, std))
    return reading


def main():
    parser = argparse.ArgumentParser(description="Micro Reactor Telemetry Simulator")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind the socket server")
    parser.add_argument("--port", type=int, default=9999, help="Port for Flink (or client) to connect")
    parser.add_argument(
        "--rps", type=float, default=10.0,
        help="Records per second (0 = as fast as possible)"
    )
    args = parser.parse_args()

    interval = 1.0 / args.rps if args.rps > 0 else 0.0

    # Create a TCP server
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((args.host, args.port))
        srv.listen(1)

        print(f"[Simulator] Listening on {args.host}:{args.port} ... waiting for client")
        conn, addr = srv.accept()
        print(f"[Simulator] Connected by {addr}, streaming at {args.rps} records/sec")

        try:
            while True:
                record = generate_reactor_reading(DF)
                record["timestamp"] = time.time()

                line = json.dumps(record) + "\n"
                conn.sendall(line.encode("utf-8"))

                if interval > 0:
                    time.sleep(interval)
        except (BrokenPipeError, ConnectionResetError):
            print("[Simulator] Client disconnected.")
        except KeyboardInterrupt:
            print("\n[Simulator] Simulation stopped by user.")
        finally:
            conn.close()


if __name__ == "__main__":
    main()
