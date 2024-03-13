using System.Collections.Concurrent;
using System.Globalization;
using System.IO.MemoryMappedFiles;
using System.Text;

namespace onebrc;

public class CityMeasurements(float measurement)
{
    public float Max = measurement;
    public float Min = measurement;
    public float Mean => Total / SampleCount;

    public string Summary(string city) => $"{city}={Min:F1}/{Mean:F1}/{Max:F1}";
    public string Summary(byte[] city) => $"{Encoding.UTF8.GetString(city)}={Min:F1}/{Mean:F1}/{Max:F1}";
    public string Summary<T>(T city) => throw new NotImplementedException();

    public float Total = measurement;
    public int SampleCount = 1;

    public void AddMeasurement(float measurement)
    {
        Max = Math.Max(measurement, Max);
        Min = Math.Min(measurement, Min);
        Total += measurement;
        SampleCount++;
    }
}

public class ByteArrayComparer : IEqualityComparer<byte[]>
{
    public bool Equals(byte[]? x, byte[]? y)
        => x.SequenceEqual(y);

    public int GetHashCode(byte[] x)
        => x.Aggregate(0, HashCode.Combine);
}

public class CalculateMetrics
{

    public static readonly byte[] eolBytes = Encoding.UTF8.GetBytes(Environment.NewLine);
    public static readonly byte[] separatorBytes = ";"u8.ToArray();

    public static Task MmfBytes(string filePath)
    {
        var bytes = new byte[512 * 1024];
        var eolBytes = Encoding.UTF8.GetBytes(Environment.NewLine);
        using var file = MemoryMappedFile.CreateFromFile(filePath, FileMode.Open);
        using var accessor = file.CreateViewAccessor();
        var capacity = accessor.Capacity;
        var offset = 3L;
        var dst = new ConcurrentDictionary<byte[], CityMeasurements>(new ByteArrayComparer());
        while (capacity - offset > 0)
        {
            var readBytes = accessor.ReadArray(offset, bytes, 0, (int)Math.Min(bytes.LongLength, capacity - offset));
            var readUpToEol = new ReadOnlySpan<byte>(bytes, 0, readBytes).LastIndexOf(eolBytes);
            if (readUpToEol > 0)
            {
                offset += readUpToEol + eolBytes.Length;
                CalcBytes(bytes, readUpToEol + eolBytes.Length, dst);
            }
            else
            {
                break;
            }
        };

        PrintResults(dst);

        return Task.CompletedTask;
    }

    public static Task MmfString(string filePath)
    {
        var bytes = new byte[1024 * 1024];
        using var file = MemoryMappedFile.CreateFromFile(filePath, FileMode.Open);
        using var accessor = file.CreateViewAccessor();
        var capacity = accessor.Capacity;
        var offset = 3L;
        var dst = new ConcurrentDictionary<string, CityMeasurements>();
        while (capacity - offset > 0)
        {
            var readBytes = accessor.ReadArray(offset, bytes, 0, (int)Math.Min(bytes.LongLength, capacity - offset));
            var readUpToEol = new ReadOnlySpan<byte>(bytes, 0, readBytes).LastIndexOf(eolBytes);
            if (readUpToEol > 0)
            {
                offset += readUpToEol + eolBytes.Length;
                CalcString(bytes, readUpToEol + eolBytes.Length, dst);
            }
            else
            {
                break;
            }
        };

        PrintResultsString(dst);

        return Task.CompletedTask;
    }

    public readonly record struct Page(byte[] Bytes, int Length);
    public readonly record struct Page2(long Start, int Length);

    public static async Task MmfStringProducerConsumer(string filePath)
    {
        var cpuCount = Environment.ProcessorCount;
        using var queue = new BlockingCollection<Page>(Environment.ProcessorCount);
        
        var producer = Task.Run(() =>
        {
            using var file = MemoryMappedFile.CreateFromFile(filePath, FileMode.Open);
            using var accessor = file.CreateViewAccessor();
            var capacity = accessor.Capacity;
            var offset = 3L;

            while (capacity - offset > 0)
            {
                var bytes = new byte[1024 * 1024];
                var readBytes = accessor.ReadArray(offset, bytes, 0, (int)Math.Min(bytes.LongLength, capacity - offset));
                var readUpToEol = new ReadOnlySpan<byte>(bytes, 0, readBytes).LastIndexOf(eolBytes);
                if (readUpToEol > 0)
                {
                    offset += readUpToEol + eolBytes.Length;
                    queue.Add(new Page(bytes, readUpToEol + eolBytes.Length));

                }
                else
                {
                    break;
                }
            };

            queue.CompleteAdding();
        });

        var consumers = Enumerable.Range(0, 10)
            .Select(_ =>
                Task.Run(() =>
                {
                    var dst = new ConcurrentDictionary<string, CityMeasurements>();

                    foreach (var page in queue.GetConsumingEnumerable())
                    { 
                        CalcString(page.Bytes, page.Length, dst);
                    }

                    PrintResultsString(dst);
                }));

        await Task.WhenAll(consumers.Append(producer));
    }
    
    public static async Task MmfStringProducerConsumer2(string filePath)
    {
        var cpuCount = Environment.ProcessorCount;
        using var queue = new BlockingCollection<Page2>(Environment.ProcessorCount);
        using var file = MemoryMappedFile.CreateFromFile(filePath, FileMode.Open);
        using var accessor = file.CreateViewAccessor();
        
        var producer = Task.Run(() =>
        {
            var capacity = accessor.Capacity;
            var offset = 3L;

            var eolWindow = new byte[eolBytes.Length];
            while (capacity - offset > 0)
            {
                var count = (int)Math.Min(1024 * 1024, capacity - offset);
                var readUpToEol = offset + count - eolWindow.Length;
                for (; readUpToEol >= offset; readUpToEol--)
                {
                    accessor.ReadArray(readUpToEol, eolWindow, 0, eolWindow.Length);
                    if (eolWindow.SequenceEqual(eolBytes))
                    {
                        break;
                    }
                }
                if (readUpToEol > offset)
                {
                    queue.Add(new Page2(offset, (int)(readUpToEol - offset) + eolBytes.Length));
                    offset = readUpToEol + eolBytes.Length;
                }
                else
                {
                    break;
                }
            };

            queue.CompleteAdding();
        });

        var consumers = Enumerable.Range(0, 10)
            .Select(_ =>
                Task.Run(() =>
                {
                    var dst = new ConcurrentDictionary<string, CityMeasurements>();
                    var bytes = new byte[1024 * 1024];

                    foreach (var page in queue.GetConsumingEnumerable())
                    { 
                        var readBytes = accessor.ReadArray(page.Start, bytes, 0, page.Length);
                        CalcString(bytes, page.Length, dst);
                    }

                    PrintResultsString(dst);
                }));

        await Task.WhenAll(consumers.Append(producer));
    }

    private static void CalcBytes(byte[] bytes, int length, IDictionary<byte[], CityMeasurements> table)
    {
        var left = 0;
        do
        {
            var buffer = new ReadOnlySpan<byte>(bytes, left, length - left);
            var indexOfSeparator = buffer.IndexOf(separatorBytes);
            var city = buffer[0..indexOfSeparator].ToArray();
            var indexOfEol = buffer[(indexOfSeparator + 1)..].IndexOf(eolBytes);
            //var measurementString = Encoding.UTF8.GetString(buffer[(indexOfSeparator + 1)..(indexOfSeparator + indexOfEol + 1)]);
            _ = csFastFloat.FastFloatParser.TryParseFloat(buffer[(indexOfSeparator + 1)..(indexOfSeparator + indexOfEol + 1)], out var measurement, styles: NumberStyles.AllowDecimalPoint);
            if (table.TryGetValue(city, out var existingMeasurements))
            {
                existingMeasurements.AddMeasurement(measurement);
            }
            else
            {
                table[city] = new CityMeasurements(measurement);
            }
            left += indexOfSeparator + indexOfEol + eolBytes.Length + 1;
        }
        while (left < length);
    }

    private static void CalcString(byte[] bytes, int length, IDictionary<string, CityMeasurements> table)
    {
        var left = 0;
        do
        {
            var buffer = new ReadOnlySpan<byte>(bytes, left, length - left);
            var indexOfSeparator = buffer.IndexOf(separatorBytes);
            var city = Encoding.UTF8.GetString(buffer[0..indexOfSeparator]);
            var indexOfEol = buffer[(indexOfSeparator + 1)..].IndexOf(eolBytes);
            //var measurementString = Encoding.UTF8.GetString(buffer[(indexOfSeparator + 1)..(indexOfSeparator + indexOfEol + 1)]);
            _ = csFastFloat.FastFloatParser.TryParseFloat(buffer[(indexOfSeparator + 1)..(indexOfSeparator + indexOfEol + 1)], out var measurement, styles: NumberStyles.AllowDecimalPoint);
            if (table.TryGetValue(city, out var existingMeasurements))
            {
                existingMeasurements.AddMeasurement(measurement);
            }
            else
            {
                table[city] = new CityMeasurements(measurement);
            }
            left += indexOfSeparator + indexOfEol + eolBytes.Length + 1;
        }
        while (left < length);
    }

    public static void PrintResults<T>(IDictionary<T, CityMeasurements> table)
    {
        Console.Write("{");
        Console.Write(string.Join(", ", table.OrderBy(x => x.Key).Select(x => x.Value.Summary(x.Key))));
        Console.WriteLine("}");
    }

    public static void PrintResultsString(IDictionary<string, CityMeasurements> table)
    {
        Console.Write("{");
        Console.Write(string.Join(", ", table.OrderBy(x => x.Key).Select(x => x.Value.Summary(x.Key))));
        Console.WriteLine("}");
    }
}


public class Program
{
    public static async Task<int> Main(string[] args)
    {
        var filePath = args[0];

        //await CalculateMetrics<ReadOnlyMemory<char>>.PrintCalculatedMetricsString(filePath);
        //await CalculateMetrics<ReadOnlyMemory<char>>.BlockingCollection(filePath);
        //await CalculateMetrics.MmfBytes(filePath);
        await CalculateMetrics.MmfStringProducerConsumer2(filePath);

        return 0;
    }


}