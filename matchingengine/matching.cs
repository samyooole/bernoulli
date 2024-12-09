using System.Collections.Generic;

public class MatchingEngine
{
    private Dictionary<string, List<Contract>> _contractMap;

    public MatchingEngine()
    {
        _contractMap = new Dictionary<string, List<Contract>>();
    }

    public void AddContract(Contract newContract)
    {
        string key = GenerateKey(newContract);

        if (_contractMap.ContainsKey(key))
        {
            var matches = _contractMap[key];

            if (matches.Count > 0)
            {
                // Found a match, pair and process
                Contract match = matches[0];
                matches.RemoveAt(0);

                // Perform settlement
                SettleContracts(newContract, match);
            }
            else
            {
                // Add the contract to the list for future matching
                _contractMap[key].Add(newContract);
            }
        }
        else
        {
            // Create a new entry in the map
            _contractMap[key] = new List<Contract> { newContract };
        }
    }

    private string GenerateKey(Contract contract)
    {
        // Create a unique key based on the contract parameters
        return $"{contract.Parameter1}:{contract.Parameter2}:{contract.Parameter3}";
    }

    private void SettleContracts(Contract contract1, Contract contract2)
    {
        // Logic to settle the two contracts
        Console.WriteLine($"Settled: {contract1.Id} with {contract2.Id}");
    }
}

public class Contract
{
    public string Id { get; set; }
    public string Parameter1 { get; set; }
    public string Parameter2 { get; set; }
    public string Parameter3 { get; set; }
}


