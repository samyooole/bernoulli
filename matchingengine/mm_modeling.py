

## broad idea
# take the MBO data, and pick out the market makers specifically and figure out how much quantity they're putting up
# assume one homogenous market maker, wlog
# then calculate the spread on a minute by minute basis
# also calculate the cumulative quantity of orders put up by MMs are 
# regress % spread against change in quantity, simple linear regression

# use model parameters to (will minute -> hour make the constant different? will we have to adjust that?) figure out if I increased a rebate by some percentage point, how much more market making volume can I expect

# let's take a close look 


import polars as pl
import numpy as np

df = pl.read_csv("/Users/samuelho/databento/DBEQ-20241209-UB4KFCCU7A/dbeq-basic-20241107-20241206.mbo.csv")


df = df.filter(pl.col("ts_recv").str.starts_with("2024-12-02"))

# STEP 1: PREPROCESSING

df = df.select(['ts_event', 'side', 'price', 'size', 'action', 'order_id']) # select only relevant volumns

df = df.filter(pl.col("side") != "N") # only filter for bids and asks (idk what N is)


df = df.with_columns((pl.col('price') * 1e-9).alias('price')) # convert from fixed precision integer to decimal

df = df.with_columns(
    pl.col("ts_event").str.to_datetime()
) # convert to datetime, in UTC currently

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

# Set thresholds for exclusion (e.g., 30% cancel-to-add or modify-to-add ratio)
market_maker_order_ids = action_counts.filter(
    (pl.col("cancel_to_add_ratio") > 0.3) | (pl.col("modify_to_add_ratio") > 0.3)
).select("order_id")

# Include only these order_ids from the original dataframe
filtered_df = df.join(
    market_maker_order_ids, on="order_id", how="inner"
)

# remove absurd quoted prices (those above 1e6)
filtered_df = filtered_df.filter(pl.col("price") < 1e6)



# NOW, take the original df, and on a minute by minute basis, group the bid/ask prices, find the highest bid and the lowest ask, calculate that as the spread, make a new column, 
# side[str]: B for bid, A for ask
# price[f64]
# Calculate the spread on a minute-by-minute basis

df = df.sort('ts_event')

minute_grouped = df.group_by_dynamic(
    "ts_event", every="1h", closed="right"
).agg([
    pl.col("price").filter(pl.col("side") == "B").mean().alias("max_bid"),
    pl.col("price").filter(pl.col("side") == "A").mean().alias("min_ask"),
    pl.col('price').mean().alias("midmarket_price")
])

# Calculate the spread
minute_grouped = minute_grouped.with_columns(
    ( (pl.col("min_ask") - pl.col("max_bid") ) / (pl.col("midmarket_price"))).alias("spread")
)

# Get rid of datetimes with any null
minute_grouped = minute_grouped.drop_nulls()

# Calculate cumulative quantity of orders put up by MMs
qty = df.group_by_dynamic(
    "ts_event", every="1h", closed="right"
).agg([
    (pl.when(pl.col("action") == "A").then(pl.col("size"))
     .when(pl.col("action") == "C").then(-pl.col("size"))
     .otherwise(0)).sum().alias("quantity_delta")
])

# add quantity_delta to its lag to get cumulative
qty = qty.with_columns(
    pl.col("quantity_delta").cum_sum().alias("cumulative_quantity")
)


# Join the spread and cumulative quantity dataframes
result_df = minute_grouped.join(
    qty, on="ts_event", how="inner"
)

# remove those ts_event outside of ET trading hours
result_df = result_df.filter(
    (pl.col("ts_event").dt.hour() >= 14) & (pl.col("ts_event").dt.hour() <= 22)
)

# regress cumulative_quantity against spread
import statsmodels.api as sm
# Prepare the data for regression
X = result_df["spread"]
y = result_df["quantity_delta"]

# Add a constant to the independent variable
#X = sm.add_constant(X)

# Fit the regression model
# Coerce to numpy arrays
X = np.array(result_df["spread"])
y = np.array(result_df["quantity_delta"])

# Add a constant to the independent variable
X = sm.add_constant(X)

# Fit the regression model
model = sm.OLS(y, X).fit()

# Print the regression results
print(model.summary())

