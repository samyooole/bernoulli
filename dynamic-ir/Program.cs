using System;

class InterestRateEngine
{
    // Parameters for supply and demand
    private double a_s, b_s, a_d, b_d;
    private double r_mm; // Mid-market rate
    private double r_l, r_b; // Lender and borrower rates
    private double supplyState, demandState;

    public double ScalingFactor { get; set; } = 1.0; // Default scaling factor (k)

    public InterestRateEngine(double a_s, double b_s, double a_d, double b_d, double initialRate)
    {
        this.a_s = a_s;
        this.b_s = b_s;
        this.a_d = a_d;
        this.b_d = b_d;
        this.r_mm = initialRate;
        UpdateRates();
        supplyState = CalculateSupply(r_l);
        demandState = CalculateDemand(r_b);
    }

    private void UpdateRates()
    {
        r_l = r_mm + 0.2; // Lender rate
        r_b = r_mm - 0.2; // Borrower rate
    }

    private double CalculateSupply(double rate) => ScalingFactor * (a_s + b_s * rate);

    private double CalculateDemand(double rate) => ScalingFactor * (a_d + b_d * rate);

    public void HandleSupplyAndDemandEvent(double supplyChange, double demandChange)
    {
        double absSupplyChange = supplyChange;
        double absDemandChange = demandChange;

        // Determine the net effect: positive if demandChange dominates, negative if supplyChange dominates
        double netEffect = absDemandChange - absSupplyChange;

        Console.WriteLine($"netEffect: {netEffect}");

        // Scale the rate adjustment by the magnitude of the net effect
        double adjustment = 0.001 * netEffect;

        Console.WriteLine($"adj: {adjustment}");

        // Update mid-market rate accordingly
        r_mm += adjustment;

        UpdateRates();
    }

    public void SimulateMarketResponse()
    {
        // Simulate supply response
        double newSupply = CalculateSupply(r_l);
        double supplyChange = newSupply - supplyState;

        // Simulate demand response
        double newDemand = CalculateDemand(r_b);
        double demandChange = newDemand - demandState;

        // Trigger the scaled adjustment for r_mm based on changes
        HandleSupplyAndDemandEvent(supplyChange, demandChange);

        // Adjust the supply and demand states to move halfway towards equilibrium
        if (Math.Abs(supplyChange) > 0.001) // Threshold for significant change
        {
            supplyState += supplyChange / 2;
            Console.WriteLine($"Supply Event Triggered: Change = {supplyChange:F2}, New Supply = {supplyState:F2}");
        }

        if (Math.Abs(demandChange) > 0.001) // Threshold for significant change
        {
            demandState += demandChange / 2;
            Console.WriteLine($"Demand Event Triggered: Change = {demandChange:F2}, New Demand = {demandState:F2}");
        }
    }

    public void ApplyOneTimeShift(int iteration, int targetIteration)
    {
        if (iteration == targetIteration)
        {
            a_d += 1; // Example: Increase a_d by 0.5 to simulate a demand shift
            Console.WriteLine($"One-Time Shift Applied: a_d increased to {a_d:F2}");
        }
    }

    public void PrintStatus(int iteration)
    {
        Console.WriteLine($"Iteration {iteration}");
        Console.WriteLine($"Mid-Market Rate (r_mm): {r_mm:F3}");
        Console.WriteLine($"Lender Rate (r_l): {r_l:F3}, Borrower Rate (r_b): {r_b:F3}");
        Console.WriteLine($"Supply State: {supplyState:F2}, Demand State: {demandState:F2}");
        double profit = (r_l - r_b)/100 * Math.Min(supplyState, demandState);
        Console.WriteLine($"Profit: {profit:F2}");
        Console.WriteLine(new string('-', 50));
    }
}

class Program
{
    static void Main(string[] args)
    {
        // Optimized parameters
        double a_s = -1.2, b_s = 0.65; // Adjusted a_s to shift equilibrium
        double a_d = 7.0, b_d = -0.79; // Adjusted a_d for positive Q_opt
        double initialRate = 5;

        // Initialize the system
        InterestRateEngine engine = new InterestRateEngine(a_s, b_s, a_d, b_d, initialRate);

        // Adjust scaling factor
        engine.ScalingFactor = 500.0; // Example: Scale up by a factor of 5

        // Simulate multiple iterations with a one-time shift
        int shiftIteration = 5; // Apply shift at iteration 5

        for (int i = 1; i <= 20; i++)
        {
            engine.PrintStatus(i);

            // Apply a one-time shift
            engine.ApplyOneTimeShift(i, shiftIteration);

            // Simulate market response
            engine.SimulateMarketResponse();
        }
        // Write status details to a CSV file
        using (var writer = new System.IO.StreamWriter("market_status.csv"))
        {
            writer.WriteLine("Iteration,Mid-Market Rate,Lender Rate,Borrower Rate,Supply State,Demand State,Profit");

            for (int i = 1; i <= 20; i++)
            {
                engine.PrintStatus(i);

                // Apply a one-time shift
                engine.ApplyOneTimeShift(i, shiftIteration);

                // Simulate market response
                engine.SimulateMarketResponse();

                // Collect status details
                double r_mm = (double)engine.GetType().GetField("r_mm", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance).GetValue(engine);
                double r_l = (double)engine.GetType().GetField("r_l", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance).GetValue(engine);
                double r_b = (double)engine.GetType().GetField("r_b", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance).GetValue(engine);
                double supplyState = (double)engine.GetType().GetField("supplyState", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance).GetValue(engine);
                double demandState = (double)engine.GetType().GetField("demandState", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance).GetValue(engine);
                double profit = (r_l - r_b)/100 * Math.Min(supplyState, demandState);

                // Write to CSV
                writer.WriteLine($"{i},{r_mm:F3},{r_l:F3},{r_b:F3},{supplyState:F2},{demandState:F2},{profit:F2}");
            }
        }
    }
}
