import numpy as np
import time

class OrderBook:
    # 資產的代碼
    # 紀錄買方（bids）和賣方（asks）的訂單的訂單
    def __init__(self, symbol):
        self.symbol = symbol
        self.timestamp = time.time()
        self.start_time = None
        
        self.asks = []  # Stores the ask side (price, size)
        self.asks_weight = None
        self.asks_weight2 = None
        self.asks_mean = None
        self.asks_Sn = None
        self.asks_population_variance = None
        self.asks_sample_variance = None

        self.bids = []  # Stores the bid side (price, size)
        self.bids_weight = None
        self.bids_weight2 = None
        self.bids_mean = None
        self.bids_Sn = None
        self.bids_population_variance = None
        self.bids_sample_variance = None

    
    # 從外部接收新的訂單資料，並更新訂單簿的內容
    def update(self, data):
        """Updates the orderbook with new data."""
        # 避免錯誤更新
        if data["symbol"] != self.symbol:
            raise ValueError("Data symbol does not match orderbook symbol")

        self.asks = data["asks"]
        self.bids = data["bids"]

        asks_mean = 0
        bids_mean = 0
        if self.asks_weight is None:
            self.start_time = time.time()
            # update asks
            money = np.array([ask[0] for ask in self.asks])
            weight = np.array([ask[1] for ask in self.asks])
            self.asks_weight = np.sum(weight)
            self.asks_weight2 = np.sum(weight ** 2)
            self.asks_mean = np.sum(money * weight) / self.asks_weight
            self.asks_Sn = np.sum(weight * (money - self.asks_mean)**2)
            self.asks_population_variance = self.asks_Sn / self.asks_weight
            # refer to "weighted variance": weighted variance is different [Wikipedia]:
            self.asks_sample_variance = self.asks_Sn * (
                self.asks_weight / (self.asks_weight ** 2 - self.asks_weight2)
            )

            # update bids
            money = np.array([bid[0] for bid in self.bids])
            weight = np.array([bid[1] for bid in self.bids])
            self.bids_weight = np.sum(weight)
            self.bids_weight2 = np.sum(weight ** 2)
            self.bids_mean = np.sum(money * weight) / self.bids_weight
            self.bids_Sn = np.sum(weight * (money - self.bids_mean)**2)
            self.bids_population_variance = np.sum(weight * (money - self.bids_mean)**2) / self.bids_weight
            # refer to "weighted variance": weighted variance is different [Wikipedia]:
            self.bids_sample_variance = self.bids_Sn * (
                self.bids_weight / (self.bids_weight ** 2 - self.bids_weight2)
            )
        else:
            # https://fanf2.user.srcf.net/hermes/doc/antiforgery/stats.pdf
            m = np.array([ask[0] for ask in self.asks])
            w = np.array([ask[1] for ask in self.asks])
            asks_weight = np.sum(w)
            asks_mean = np.sum(m * w) / asks_weight

            m = np.array([bid[0] for bid in self.bids])
            w = np.array([bid[1] for bid in self.bids])
            bids_weight = np.sum(w)
            bids_mean = np.sum(m * w) / bids_weight

            for money, weight in self.asks:
                old_mean = self.asks_mean
                self.asks_weight = self.asks_weight + weight
                self.asks_weight2 = self.asks_weight2 + weight ** 2
                self.asks_mean = money - (self.asks_weight - weight) / self.asks_weight * (money - self.asks_mean)
                self.asks_Sn = self.asks_Sn + weight * (money - old_mean) * (money - self.asks_mean)
                self.asks_population_variance = self.asks_Sn / self.asks_weight
                self.asks_sample_variance = self.asks_Sn * (self.asks_weight / (self.asks_weight ** 2 - self.asks_weight2))
            for money, weight in self.bids:
                old_mean = self.bids_mean
                self.bids_weight = self.bids_weight + weight
                self.bids_weight2 = self.bids_weight2 + weight ** 2
                self.bids_mean = money - (self.bids_weight - weight) / self.bids_weight * (money - self.bids_mean)
                self.bids_Sn = self.bids_Sn + weight * (money - old_mean) * (money - self.bids_mean)
                self.bids_population_variance = self.bids_Sn / self.bids_weight
                self.bids_sample_variance = self.bids_Sn * (self.bids_weight / (self.bids_weight ** 2 - self.bids_weight2))
        return asks_mean, bids_mean


    def dump(self, max_level=10):
        """Prints the orderbook in a vertical format."""
        # 限制掛單深度，只顯示前幾筆掛單，聚焦於最相關的訂單
        max_ask_level = min(max_level, len(self.asks))  # Limit ask levels to max_level
        max_bid_level = min(max_level, len(self.bids))  # Limit bid levels to max_level

        print(f"Orderbook for {self.symbol} (Top {max_level} levels):")
        print(f"{'Ask Price':>15} | {'Ask Size':>15}")
        print("-" * 35)

        # 假設訂單已經排序，打印出價格
        # Print Ask orders (sorted from highest to lowest)
        for i in range(max_ask_level):
            ask_price, ask_size = self.asks[i]
            print(f"{ask_price:>15.2f} | {ask_size:>15.6f}")

        print("-" * 35)
        print(f"{'Bid Price':>15} | {'Bid Size':>15}")
        print("-" * 35)

        # Print Bid orders (sorted from highest to lowest)
        for i in range(max_bid_level):
            bid_price, bid_size = self.bids[i]
            print(f"{bid_price:>15.2f} | {bid_size:>15.6f}")

        print("-" * 35)