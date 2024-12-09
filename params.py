
import numpy as np
import scipy.optimize as optimize
import matplotlib.pyplot as plt

def market_clearing_model(params, r, Q):
    """
    Non-linear least squares objective function
    
    Parameters:
    - params: [a, b, ed, ef]
    - r: array of interest rates
    - Q: array of quantities
    
    Returns: Sum of squared residuals
    """
    a, b, ed, ef = params
    
    # Predicted quantities based on model
    Q_pred = a * np.exp(-ed * r)
    
    # Sum of squared errors
    return np.sum((Q - Q_pred)**2)

def estimate_parameters(r, Q):
    """
    Estimate market clearing parameters using non-linear least squares
    
    Args:
    - r: Interest rates
    - Q: Quantities
    
    Returns: Estimated parameters [a, b, ed, ef]
    """
    # Initial guess for parameters
    initial_guess = [
        np.max(Q),    # a: max quantity 
        np.max(Q),    # b: max quantity
        1.0,          # ed: demand rate sensitivity
        1.0           # ef: supply rate sensitivity
    ]
    
    # Bounds for parameters to prevent unrealistic values
    bounds = [
        (0, np.inf),  # a: positive
        (0, np.inf),  # b: positive
        (0, 100),     # ed: reasonable sensitivity
        (0, 100)      # ef: reasonable sensitivity
    ]
    
    # Perform non-linear least squares optimization
    result = optimize.minimize(
        lambda params: market_clearing_model(params, r, Q), 
        initial_guess, 
        method='L-BFGS-B',
        bounds=bounds
    )
    
    return result.x

# Example usage
def generate_synthetic_data():
    """Generate synthetic market data for demonstration"""
    np.random.seed(42)
    
    # True parameters
    true_a = 1000
    true_b = 1000
    true_ed = 10
    true_ef = 10
    
    # Generate interest rates
    r = np.linspace(0.01, 0.1, 100)
    
    # Generate quantities with some noise
    Q = true_a * np.exp(-true_ed * r) + np.random.normal(0, 50, r.shape)
    
    return r, Q, [true_a, true_b, true_ed, true_ef]

def main():
    # Generate synthetic data
    r, Q, true_params = generate_synthetic_data()
    
    # Estimate parameters
    estimated_params = estimate_parameters(r, Q)
    
    # Print results
    param_names = ['a', 'b', 'ed', 'ef']
    print("True Parameters:")
    for name, val in zip(param_names, true_params):
        print(f"{name}: {val}")
    
    print("\nEstimated Parameters:")
    for name, val in zip(param_names, estimated_params):
        print(f"{name}: {val}")
    
    # Plotting
    plt.figure(figsize=(10, 6))
    plt.scatter(r, Q, label='Observed Data', alpha=0.6)
    
    # True model prediction
    Q_true = true_params[0] * np.exp(-true_params[2] * r)
    plt.plot(r, Q_true, 'r-', label='True Model')
    
    # Estimated model prediction
    Q_est = estimated_params[0] * np.exp(-estimated_params[2] * r)
    plt.plot(r, Q_est, 'g--', label='Estimated Model')
    
    plt.xlabel('Interest Rate')
    plt.ylabel('Quantity')
    plt.title('Market Clearing Model: True vs Estimated')
    plt.legend()
    plt.show()

if __name__ == "__main__":
    main()