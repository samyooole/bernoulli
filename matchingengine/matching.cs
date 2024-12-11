using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Text.Json;

public class MatchingEngine
{
    private Dictionary<string, List<Contract>> _contractMap;

    // Market maker rebates for tracking incentives
    private Dictionary<string, decimal> _rebates;
    private decimal _totalRebateCost;
    private decimal _totalVolumeSettled;
    private decimal _totalRevenue;

    public MatchingEngine()
    {
        _contractMap = new Dictionary<string, List<Contract>>();
        _rebates = new Dictionary<string, decimal>();
        _totalRebateCost = 0;
        _totalVolumeSettled = 0;
        _totalRevenue = 0;
    }

    public void AddContract(Contract newContract)
    {
        string key = GenerateKey(newContract);

        if (!_contractMap.ContainsKey(newContract.SettlementHour))
        {
            _contractMap[newContract.SettlementHour] = new List<Contract>();
        }

        _contractMap[newContract.SettlementHour].Add(newContract);
    }

    public void ApplyRebate(string marketMakerId, decimal rebatePercentage)
    {
        if (_rebates.ContainsKey(marketMakerId))
        {
            _rebates[marketMakerId] = rebatePercentage;
        }
        else
        {
            _rebates.Add(marketMakerId, rebatePercentage);
        }
    }

    public void DynamicMarketMakerRebate(decimal constant, decimal gradient, decimal rebatePercentage, decimal settlementFeePercentage)
    {
        Console.WriteLine("Calculating dynamic market maker rebate...");

        foreach (var hourGroup in _contractMap)
        {
            string settlementHour = hourGroup.Key;
            var contracts = hourGroup.Value;

            // Calculate the added volume for this specific hour
            decimal totalAddedVolume = constant + gradient * (rebatePercentage * 100) * 170;
            decimal costOfRebate = totalAddedVolume * rebatePercentage;
            decimal revenueFromFee = totalAddedVolume * settlementFeePercentage;

            _totalRebateCost += costOfRebate;
            _totalRevenue += revenueFromFee;

            Console.WriteLine($"Hour {settlementHour}:");
            Console.WriteLine($"Total additional market making volume for {rebatePercentage * 100}% rebate: {totalAddedVolume}");
            Console.WriteLine($"Cost of the rebate to the platform: {costOfRebate}");
            Console.WriteLine($"Revenue from settlement fees: {revenueFromFee}");

            // Simulate settling additional volume provided by the market maker for asks
            var asks = contracts.Where(c => c.OrderType == "Ask").OrderBy(c => c.Price).ToList();

            foreach (var ask in asks)
            {
                if (totalAddedVolume <= 0) break;

                decimal settled = Math.Min(totalAddedVolume, ask.Quantity );
                ask.Quantity -= settled;
                totalAddedVolume -= settled;
                _totalVolumeSettled += settled;

                Console.WriteLine($"  Settled {settled} units additionally on asks at price {ask.Price}.");
            }

            // Update the contract map for asks after settlement
            _contractMap[settlementHour] = contracts.Where(c => c.Quantity > 0).ToList();

            // Now handle bids
            var bids = contracts.Where(c => c.OrderType == "Bid").OrderByDescending(c => c.Price).ToList();

            foreach (var bid in bids)
            {
                if (totalAddedVolume <= 0) break;

                decimal settled = Math.Min(totalAddedVolume, bid.Quantity);
                bid.Quantity -= settled;
                totalAddedVolume -= settled;
                _totalVolumeSettled += settled;

                Console.WriteLine($"  Settled {settled} units additionally on bids at price {bid.Price}.");
            }

            // Update the contract map for bids after settlement
            _contractMap[settlementHour] = contracts.Where(c => c.Quantity > 0).ToList();

            if (totalAddedVolume > 0)
            {
                Console.WriteLine($"  Unutilized additional volume: {totalAddedVolume}.");
            }
        }
    }

    public void SettleContractsAll(decimal settlementFeePercentage)
    {
        foreach (var hourGroup in _contractMap)
        {
            string settlementHour = hourGroup.Key;
            List<Contract> contracts = hourGroup.Value;

            // Separate bids and asks
            var bids = contracts.Where(c => c.OrderType == "Bid").OrderByDescending(c => c.Price).ToList();
            var asks = contracts.Where(c => c.OrderType == "Ask").OrderBy(c => c.Price).ToList();

            int bidIndex = 0;
            int askIndex = 0;

            while (bidIndex < bids.Count && askIndex < asks.Count)
            {
                Contract bid = bids[bidIndex];
                Contract ask = asks[askIndex];

                if (bid.Price >= ask.Price)
                {
                    decimal settledQuantity = Math.Min(bid.Quantity, ask.Quantity);
                    bid.Quantity -= settledQuantity;
                    ask.Quantity -= settledQuantity;

                    decimal settlementValue = settledQuantity * ask.Price;
                    _totalVolumeSettled += settlementValue;
                    _totalRevenue += settlementValue * settlementFeePercentage;

                    Console.WriteLine($"Settled {settledQuantity} between Bid {bid.Id} and Ask {ask.Id} at hour {settlementHour}");

                    if (bid.Quantity == 0) bidIndex++;
                    if (ask.Quantity == 0) askIndex++;
                }
                else
                {
                    break; // No more matches possible for this hour
                }
            }

            // Update the contract map with remaining contracts
            _contractMap[settlementHour] = bids.Where(b => b.Quantity > 0).Concat(asks.Where(a => a.Quantity > 0)).ToList();
        }
    }

    public void LoadContractsFromJson(string filePath)
    {
        var jsonData = File.ReadAllText(filePath);
        var contracts = JsonSerializer.Deserialize<List<Contract>>(jsonData);

        foreach (var contract in contracts)
        {
            AddContract(contract);
        }
    }

    public void PrintUnsettledContracts()
    {
        Console.WriteLine("Unsettled contracts by settlement hour:");
        foreach (var hourGroup in _contractMap)
        {
            string settlementHour = hourGroup.Key;
            decimal totalUnsettled = hourGroup.Value.Sum(c => c.Quantity);
            Console.WriteLine($"{settlementHour}: {totalUnsettled} units");
        }
    }

    public void PrintMetrics()
    {
        Console.WriteLine($"Total volume settled: {_totalVolumeSettled}");
        Console.WriteLine($"Total revenue from settlement fees: {_totalRevenue}");
        Console.WriteLine($"Total rebate cost: {_totalRebateCost}");
        Console.WriteLine($"Net profit: {_totalRevenue - _totalRebateCost}");
    }

    private string GenerateKey(Contract contract)
    {
        // Key structure: SettlementHour:Price:OrderType (Bid/Ask)
        return $"{contract.SettlementHour}:{contract.Price}:{contract.OrderType}";
    }
}

public class Contract
{
    public string Id { get; set; }
    public string SettlementHour { get; set; }
    public decimal Price { get; set; }
    public decimal Quantity { get; set; }
    public string OrderType { get; set; } // "Bid" or "Ask"
    public string MarketMakerId { get; set; } // Optional, for identifying market makers
}

class Program
{
    static void Main(string[] args)
    {
        MatchingEngine engine = new MatchingEngine();

        // Load contracts from the JSON file exported by Python
        engine.LoadContractsFromJson("contracts.json");

        // Settle all contracts
        decimal settlementFeePercentage = 0.000002m; // 0.0001%
        // DTCC is 0.00000046++ net
        engine.SettleContractsAll(settlementFeePercentage);

        // Print unsettled contracts
        engine.PrintUnsettledContracts();

        // Dynamic market maker rebate simulation
        decimal constant = 20760000; // Constant from regression
        decimal gradient = 15710000; // Gradient from regression
        decimal rebatePercentage = 0.0014m; // 0.5% rebate

        engine.DynamicMarketMakerRebate(constant, gradient, rebatePercentage, settlementFeePercentage);

        engine.PrintUnsettledContracts();

        // Print metrics
        engine.PrintMetrics();
    }
}