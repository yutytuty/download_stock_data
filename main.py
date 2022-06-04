import datetime
import os

from alpaca_trade_api.rest import REST, TimeFrame
import pandas as pd
import threading
import dotenv

dotenv.load_dotenv()


# .env file looks like this:
# ||=================================||
# ||APCA_API_KEY_ID=<key_id>         ||
# ||APCA_API_SECRET_KEY=<secret_key> ||
# ||=================================||

# YYYY-MM-DD
# CHANGE TO REAL DATES
# -------------------------
START_DATE = "2021-05-08"
END_DATE = "2021-06-08"
# -------------------------


def download_data(rest: REST, symbol, lock: threading.Lock):
    with lock:
        print(f"Downloading {symbol} data...")
    data = rest.get_bars(
        symbol, TimeFrame.Minute, START_DATE, END_DATE,
        adjustment='raw'
    ).df
    if not os.path.exists(f"./data/{START_DATE}->{END_DATE}"):
        os.mkdir(f"./data/{START_DATE}->{END_DATE}")
    data.to_csv(f"./data/{START_DATE}->{END_DATE}/{symbol}.csv")
    with lock:
        print(f"Done downloading {symbol} data")


def main():
    api = REST()

    tickers = pd.read_csv("./tickers.csv")["Symbol"]

    lock = threading.Lock()
    request_num = 0
    thread_num = 10
    running_threads: list[threading.Thread] = []
    pointer = 0
    while len(tickers) != 0:
        for i, thread in enumerate(running_threads):
            if not thread.is_alive():
                thread.join()
                running_threads.pop(i)

        while len(running_threads) != thread_num:
            if request_num < 40:
                t = threading.Thread(
                    target=download_data,
                    args=(api, tickers.pop(pointer), lock)
                )
                pointer += 1
                request_num += 1
                running_threads.append(t)
                t.start()
            else:
                print("Reached 40 requests. Waiting for reset.")
                start_time = datetime.datetime.now()
                while (datetime.datetime.now().minute - start_time.minute) < 1:
                    pass
                print("Request count reset")
                request_num = 0


if __name__ == "__main__":
    main()
