import polars as pl
import random

class Broker():

    def __init__(self, client_orders: pl.DataFrame):
        self.client_orders = client_orders
        self.hashmap = {}
        for hour in range(14, 22):
            self.hashmap[f"{hour:02d}:00"] = 0

        self.ask_hashmap = {}
        for hour in range(14, 22):
            self.ask_hashmap[f"{hour:02d}:00"] = 0

        self.bid_hashmap = {}
        for hour in range(14, 22):
            self.bid_hashmap[f"{hour:02d}:00"] = 0

        self.eod_hashmap = {}
        for hour in range(14, 22):
            self.eod_hashmap[f"{hour:02d}:00"] = 0
        return
    
    def netting_algorithm(self, event: pl.DataFrame):
        # Extract relevant details from the event
        event_time = event['ts_event'][0]  # Timestamp of the event
        hour_of_trade = event_time.hour  # Extract the hour
        is_ask = event['side'][0] == 'A'  # True if it's an ask (selling), False if it's a bid (buying)
        value_of_trade = event['price'][0] * event['size'][0]  # Total value of the trade

        # Calculate cash flow impact
        cashflow_impact = value_of_trade if is_ask else -value_of_trade

        # Random settlement logic
        if random.random() < 0.2:  # 20% probability
            fixed_hour = random.choice(list(self.hashmap.keys()))  # Choose a random hour
            self.hashmap[fixed_hour] += cashflow_impact
            if is_ask:
                self.ask_hashmap[fixed_hour] += value_of_trade
            else:
                self.bid_hashmap[fixed_hour] += value_of_trade
            return  # Exit early as settlement time is fixed

        # Update hashmap
        current_hour_key = f"{hour_of_trade:02d}:00"
        self.hashmap[current_hour_key] += cashflow_impact
        if is_ask:
            self.ask_hashmap[current_hour_key] += value_of_trade
        else:
            self.bid_hashmap[current_hour_key] += value_of_trade

        # Netting logic
        for hour_key, balance in sorted(self.hashmap.items()):
            if hour_key >= current_hour_key:  # Only adjust from earlier to the current hour
                break

            # If there's an imbalance, net it out
            if balance > 0 and cashflow_impact < 0:  # Bid position
                net_amount = min(balance, abs(cashflow_impact))
                self.hashmap[hour_key] -= net_amount
                cashflow_impact += net_amount

                self.bid_hashmap[hour_key] += net_amount
                self.bid_hashmap[current_hour_key] -= net_amount

            elif balance < 0 and cashflow_impact > 0:  # Ask position
                net_amount = min(abs(balance), cashflow_impact)
                self.hashmap[hour_key] += net_amount
                cashflow_impact -= net_amount

                self.ask_hashmap[hour_key] += net_amount
                self.ask_hashmap[current_hour_key] -= net_amount

            # Break early if fully netted
            if cashflow_impact == 0:
                break

        # Ensure the hashmap reflects final cashflow impact
        self.hashmap[current_hour_key] = cashflow_impact

        return


    def eod_netting(self):

        # Take the whole client_orders, then go through each row, determining the maximum amount of cash outlay they would have to keep
        # basically net all orders within each hour - that's the max cash outlay
        # add that to self.eod_hashmap
        for hour in range(14, 22):
            hour_key = f"{hour:02d}:00"
            before_hour_key = f"{hour-1:02d}:00"
            orders_in_hour = self.client_orders.filter(pl.col('ts_event').dt.hour() == hour)
            ask_total = orders_in_hour.filter(pl.col('side') == 'A')
            bid_total = orders_in_hour.filter(pl.col('side') == 'B')
            net = sum(ask_total['price'] * ask_total['size']) - sum(bid_total['price'] * bid_total['size'])
            if hour != 14:
                self.eod_hashmap[hour_key] = self.eod_hashmap[before_hour_key] + net

            # add to previous actually

        return