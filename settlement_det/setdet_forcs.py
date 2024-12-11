import polars as pl
from broker import Broker
from collections import defaultdict
from tqdm import tqdm
import plotly.graph_objects as go

df = pl.read_csv("/Users/samuelho/databento/DBEQ-20241209-UB4KFCCU7A/dbeq-basic-20241107-20241206.mbo.csv")


df = df.filter(pl.col("ts_recv").str.starts_with("2024-12-06"))



# take each ts_event as a trade that a broker has to execute

# side: specifies [A]sk or [B]id 

# price: what the quoted price is

# size: order quantity

# assume that all of them will go through, for the purposes of simplification

# so what we'll do it we'll split all of this (928,828) data - for one day only - up across 3000 broker dealer (this is the amount that the US has)


# STEP 1: PREPROCESSING

df = df.select(['ts_event', 'side', 'price', 'size', 'action', 'order_id']) # select only relevant volumns

df = df.filter(pl.col("side") != "N") # only filter for bids and asks (idk what N is)


df = df.with_columns((pl.col('price') * 1e-9).alias('price')) # convert from fixed precision integer to decimal

df = df.with_columns(
    pl.col("ts_event").str.to_datetime()
) # convert to datetime, in UTC currently


## only filter for order_ids that are not being majorly modified 



# Group by 'order_id' and count occurrences of each action
action_counts = (
    df.group_by("order_id")
    .agg([
        pl.col("action").filter(pl.col("action") == "A").count().alias("add_count"),
        pl.col("action").filter(pl.col("action") == "C").count().alias("cancel_count"),
        pl.col("action").filter(pl.col("action") == "M").count().alias("modify_count"),
    ])
)


# Add cancel-to-add and modify-to-add ratios
action_counts = action_counts.with_columns([
    (pl.col("cancel_count") / pl.col("add_count")).alias("cancel_to_add_ratio"),
    (pl.col("modify_count") / pl.col("add_count")).alias("modify_to_add_ratio"),
])

# Set thresholds for exclusion (e.g., 80% cancel-to-add or modify-to-add ratio)
market_maker_order_ids = action_counts.filter(
    (pl.col("cancel_to_add_ratio") > 0.8) | (pl.col("modify_to_add_ratio") > 0.8)
).select("order_id")

# Exclude these order_ids from the original dataframe
filtered_df = df.join(
    market_maker_order_ids, on="order_id", how="anti"
)

# remove absurd quoted prices (those above 1e6)
filtered_df = filtered_df.filter(pl.col("price") < 1e6)


# STEP 2: ALLOCATE THESE TRADES INTO 3000 SEPARATE BROKERS
# brokers are instantiated by broker.py > class Broker()

# Number of brokers
num_brokers = 3000

# Split the filtered dataframe into chunks for each broker
shuffled_df = filtered_df.sample(fraction=1,shuffle=True) # shuffle it

# Create a list of Broker instances
brokers = []
chunk_size = len(filtered_df) // num_brokers

for i in range(num_brokers):
    start_idx = i * chunk_size
    end_idx = start_idx + chunk_size
    thisbrokersdf = shuffled_df[start_idx:end_idx]
    broker = Broker(client_orders=thisbrokersdf)
    brokers.append(broker)




# STEP 3: run client_orders one by one through the netting algorithm

# Process trades for each broker
for broker in tqdm(brokers):
    for trade in broker.client_orders.iter_rows(named=True):
        # Convert the trade row to a Polars DataFrame with one row (required for netting_algorithm)
        trade_event = pl.DataFrame([trade])
        broker.netting_algorithm(trade_event)

    broker.eod_netting()

## for each broker, they will have
# broker.ask_hashmap and broker.bid_hashmap, which provides a hour by hour aggregation of when they want their asks/bids to be settled
# (single price is being assumed, as it has been across the entirety of the above)
# need to output this in some way to fit matching.cs Contract class
# simplifying this for now because just a prototype
# in the future, can expand to a truly blow by blow analysis at every time tick
# obvi we don't have enough time for this now


import json

# Collect data for export
contracts = []
for broker_id, broker in enumerate(brokers):
    for hour, bid_volume in broker.bid_hashmap.items():
        contracts.append({
            "Id": f"Broker_{broker_id}_Bid_{hour}",
            "SettlementHour": hour,
            "Price": 170,  # assume single price of 100 for now
            "Quantity": bid_volume,
            "OrderType": "Bid",
        })
    for hour, ask_volume in broker.ask_hashmap.items():
        contracts.append({
            "Id": f"Broker_{broker_id}_Ask_{hour}",
            "SettlementHour": hour,
            "Price": 170,  # assume single price of 100 for now
            "Quantity": ask_volume,
            "OrderType": "Ask",
        })

# Save to JSON
with open("contracts.json", "w") as f:
    json.dump(contracts, f)
