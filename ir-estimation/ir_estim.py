import numpy as np
from scipy.optimize import minimize

# Objective function
def objective(params, r_obs, Q_obs):
    a_s, b_s, a_d, b_d = params

    # Calculate predicted r and Q for all time periods
    r_pred = (a_d - a_s) / (b_s - b_d)
    Q_pred = a_s + b_s * r_pred

    # Compute errors
    error_r = np.sum((r_pred - r_obs) ** 2)  # Sum of squared errors for r
    error_Q = np.sum((Q_pred - Q_obs) ** 2)  # Sum of squared errors for Q

    return error_r + error_Q

# Observed data (example)
# Replace with your actual data
import pandas as pd

df = pd.read_csv('ir-estimation/repostats.csv')
df = df.dropna()

# Stationarize Q (volume) if needed
df['Volume_bil_diff'] = df['Volume_bil'].diff().dropna()
df['Volume_bil_diff'] = df['Volume_bil_diff'] - df['Volume_bil_diff'].mean()
df = df.dropna()

# Observed values
r_obs = df['Rate (%)'].values
Q_obs = df['Volume_bil_diff'].values

# Initial parameter guesses
initial_guess = [1, 1.5, 1, -1.5]  # Replace with reasonable initial guesses

# Optimization
result = minimize(
    objective,
    initial_guess,
    args=(r_obs, Q_obs),
    method='BFGS'
)

# Extract optimized parameters
a_s_opt, b_s_opt, a_d_opt, b_d_opt = result.x
print("Optimized parameters:")
print(f"a_s = {a_s_opt}, b_s = {b_s_opt}, a_d = {a_d_opt}, b_d = {b_d_opt}")

# Predicted values for r and Q
r_pred = (a_d_opt - a_s_opt) / (b_s_opt - b_d_opt)
Q_pred = a_s_opt + b_s_opt * r_pred

# Residuals
residuals_r = r_obs - r_pred
residuals_Q = Q_obs - Q_pred

# Print residuals
print("Residuals for r:", residuals_r)
print("Residuals for Q:", residuals_Q)
