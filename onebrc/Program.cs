namespace onebrc;

public class CityMeasurements(double measurement)
{
    public double Max = measurement;
    public double Min = measurement;
    public double Mean => Total / SampleCount;

    public string Summary(string city) => $"{city}={Min:F1}/{Mean:F1}/{Max:F1}";

    public double Total = measurement;
    public int SampleCount = 1;

    public void AddMeasurement(double measurement)
    {
        Max = Math.Max(measurement, Max);
        Min = Math.Min(measurement, Min);
        Total += measurement;
        SampleCount++;
    }
}

public class Program
{
    public static async Task<int> Main(string[] args)
    {
        var filePath = args[0];
        var table = await CalculateAverages(filePath);

        PrintResults(table);

        return 0;
    }

    private static async Task<Dictionary<string, CityMeasurements>> CalculateAverages(string filePath)
    {
        var measurements = ReadMeasurements(filePath);

        var table = new Dictionary<string, CityMeasurements>();

        await foreach (var (city, measurement) in measurements)
        {
            if (table.TryGetValue(city, out var existingMeasurements))
            {
                existingMeasurements.AddMeasurement(measurement);
            }
            else
            {
                table[city] = new CityMeasurements(measurement);
            }
        }

        return table;
    }

    public static async IAsyncEnumerable<(string, double)> ReadMeasurements(string filePath)
    {
        using var reader = new StreamReader(filePath);

        string? line;

        while ((line = await reader.ReadLineAsync()) is not null)
        { 
            var fields = line.Split(";");
            var city = fields[0];
            var measurement = double.Parse(fields[1]);

            yield return (city, measurement);
        }
    }

    private static void PrintResults(Dictionary<string, CityMeasurements> table)
    {
        Console.Write("{");
        Console.Write(string.Join(", ", table.OrderBy(x => x.Key).Select(x => x.Value.Summary(x.Key))));
        Console.WriteLine("}");
    }
}