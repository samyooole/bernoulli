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


# STEP 4: across brokers, sum up their bid_hashmap and ask_hashmap by hour, create a visual distribution

# Initialize aggregate hashmaps
aggregate_bid_volume = defaultdict(float)
aggregate_ask_volume = defaultdict(float)

# Sum up the bid and ask hashmaps from all brokers
for broker in brokers:
    for hour, bid_volume in broker.bid_hashmap.items():
        aggregate_bid_volume[hour] += bid_volume
    for hour, ask_volume in broker.ask_hashmap.items():
        aggregate_ask_volume[hour] += ask_volume

# Convert the results to DataFrames for visualization
bid_volume_df = pl.DataFrame({"hour": list(aggregate_bid_volume.keys()), "bid_volume": list(aggregate_bid_volume.values())})
ask_volume_df = pl.DataFrame({"hour": list(aggregate_ask_volume.keys()), "ask_volume": list(aggregate_ask_volume.values())})

# Create two bar plots for the above, aligned to the hour (conver the hour back 5 hours though, we had it in UTC but need to convert to ET for better comprehensibility)


# Convert UTC hours to ET (UTC-5)

# Substring the first two characters of the hour_ET column
bid_volume_df = bid_volume_df.with_columns(
    (pl.col("hour").str.slice(0, 2).cast(pl.Int32) - 5).alias("hour_ET")
)

ask_volume_df = ask_volume_df.with_columns(
    (pl.col("hour").str.slice(0, 2).cast(pl.Int32) - 5).alias("hour_ET")
)

# Plot bid and ask volumes on the same plot
fig = go.Figure()

# Plot bid volumes
fig.add_trace(go.Bar(
    x=bid_volume_df["hour_ET"],
    y=bid_volume_df["bid_volume"],
    name='Bid Volume',
    marker_color='green'
))

# Plot ask volumes (negative for visualization purposes)
fig.add_trace(go.Bar(
    x=ask_volume_df["hour_ET"],
    y=-ask_volume_df["ask_volume"],
    name='Ask Volume',
    marker_color='red'
))

fig.update_layout(
    title='Bid and Ask Volume Distribution by Hour (ET)',
    xaxis_title='Hour (ET)',
    yaxis_title='Volume',
    barmode='relative',
    bargap=0.2,
    bargroupgap=0.1,
    xaxis=dict(
        tickmode='linear',
        tick0=0,
        dtick=1
    )
)

# Increase resolution
fig.update_layout(
    autosize=False,
    width=1280,
    height=800
)

fig.write_image("/Users/samuelho/bernoulli/settlement_det/bid_ask_volume_distribution.png")

# Next, get the difference between bids and asks, then plot that net difference 

# Calculate the net difference between bid and ask volumes
net_volume_df = bid_volume_df.join(
    ask_volume_df, on="hour_ET", how="outer"
).fill_null(0)

net_volume_df = net_volume_df.with_columns(
    (pl.col("bid_volume") - pl.col("ask_volume")).alias("net_volume")
)

# Plot the net volume difference
fig = go.Figure()

fig.add_trace(go.Bar(
    x=net_volume_df["hour_ET"],
    y=net_volume_df["net_volume"],
    name='Net Volume',
    marker_color='blue'
))

fig.update_layout(
    title='Net Volume (bids - asks) Difference by Hour (ET)',
    xaxis_title='Hour (ET)',
    yaxis_title='Net Volume (bids - asks)',
    barmode='relative',
    bargap=0.2,
    bargroupgap=0.1,
    xaxis=dict(
        tickmode='linear',
        tick0=0,
        dtick=1
    )
)

# Increase resolution
fig.update_layout(
    autosize=False,
    width=1280,
    height=800
)

fig.write_image("/Users/samuelho/bernoulli/settlement_det/net_volume_difference.png")

fig.write_image("/Users/samuelho/bernoulli/settlement_det/net_volume_difference.png")



# Next, compare broker.hashmap and broker.eod_hashmap. Create two violin plots per hour, taking the average and std. across all brokers of the net cashflow at every hours
# Initialize lists to store net cashflow data for each hour
net_cashflow_data = defaultdict(list)

# Collect net cashflow data for each broker and each hour
for broker in brokers:
    for hour, net_cashflow in broker.hashmap.items():
        net_cashflow_data[hour].append(net_cashflow)

# Convert the net cashflow data to a DataFrame
net_cashflow_df = pl.DataFrame({
    "hour": list(net_cashflow_data.keys()),
    "net_cashflow": [sum(values) / len(values) for values in net_cashflow_data.values()],
    "std_dev": [pl.Series(values).std() for values in net_cashflow_data.values()]
})

# Convert UTC hours to ET (UTC-5)
net_cashflow_df = net_cashflow_df.with_columns(
    (pl.col("hour").str.slice(0, 2).cast(pl.Int32) - 5).alias("hour_ET")
)

# Initialize lists to store end-of-day net cashflow data for each hour
eod_net_cashflow_data = defaultdict(list)

# Collect end-of-day net cashflow data for each broker and each hour
for broker in brokers:
    for hour, net_cashflow in broker.eod_hashmap.items():
        eod_net_cashflow_data[hour].append(net_cashflow)

# Convert the end-of-day net cashflow data to a DataFrame
eod_net_cashflow_df = pl.DataFrame({
    "hour": list(eod_net_cashflow_data.keys()),
    "net_cashflow": [sum(values) / len(values) for values in eod_net_cashflow_data.values()],
    "std_dev": [pl.Series(values).std() for values in eod_net_cashflow_data.values()]
})

# Convert UTC hours to ET (UTC-5)
eod_net_cashflow_df = eod_net_cashflow_df.with_columns(
    (pl.col("hour").str.slice(0, 2).cast(pl.Int32) - 5).alias("hour_ET")
)

# Plot the average net cashflow for both hashmaps as side-by-side vertical bars
fig = go.Figure()

# Plot average net cashflow for hashmap
fig.add_trace(go.Bar(
    x=net_cashflow_df["hour_ET"],
    y=net_cashflow_df["net_cashflow"],
    name='Average Net Cashflow (With Settlement Choice)',
    marker_color='blue'
))

# Plot average net cashflow for eod_hashmap
fig.add_trace(go.Bar(
    x=eod_net_cashflow_df["hour_ET"],
    y=eod_net_cashflow_df["net_cashflow"],
    name='Average Net Cashflow (Default EOD Settlement)',
    marker_color='orange'
))

fig.update_layout(
    title='Average Net Cashflow by Hour (ET)',
    xaxis_title='Hour (ET)',
    yaxis_title='Net Cashflow',
    barmode='group',
    bargap=0.2,
    bargroupgap=0.1,
    xaxis=dict(
        tickmode='linear',
        tick0=0,
        dtick=1
    )
)

# Increase resolution
fig.update_layout(
    autosize=False,
    width=1280,
    height=800
)

fig.write_image("/Users/samuelho/bernoulli/settlement_det/net_cashflow_comparison.png")



"""
Let us consider a broker-dealer. They receive orders from their clients, and are trying to keep net cash flow as close to 0 as possible (any cashflow can be netted, since we are working with a cash pool-based blockchain - we are not working with multiple custodian banks to have to net individually).

So the algorithm is very simple: considering your existing settlement obligations at different periods of times, when you receive new orders from your clients, try to place them at settlement periods wherein you will offset your previous obligations as far as possible.

Take order book data for 1 security across 1 trading day (has to be post T+1 - May 28 2024). <start implementing from here on out> Split all the limit orders randomly across 100-1000 agents. Then, for each agent, fuzzily implement the algorithm to determine the overall observed distribution of settlement periods between buy and sell.

"""

