using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Globalization;
using System.IO.MemoryMappedFiles;
using System.Text;

namespace onebrc;

public class CityMeasurements
{
    public float Max;
    public float Min;
    public float Total;
    public int SampleCount;

    public float Mean => Total / SampleCount;
    public string Summary(string city) => $"{city}={Min:F1}/{Mean:F1}/{Max:F1}";


    public CityMeasurements(float measurement)
    {
        Max = measurement;
        Min = measurement;
        Total = measurement;
        SampleCount = 1;
    }

    public CityMeasurements(float max, float min, float total, int sampleCount)
    {
        Max = max;
        Min = min;
        Total = total;
        SampleCount = sampleCount;
    }

    public void AddMeasurement(float measurement)
    {
        Max = Math.Max(measurement, Max);
        Min = Math.Min(measurement, Min);
        Total += measurement;
        SampleCount++;
    }

    public static CityMeasurements Combine(IEnumerable<CityMeasurements> measurements)
    {
        return new CityMeasurements(
            measurements.Max(x => x.Max),
            measurements.Min(x => x.Min),
            measurements.Sum(x => x.Total),
            measurements.Sum(x => x.SampleCount));
    }
}

public class CalculateMetrics
{
    public static readonly byte[] eolBytes = Encoding.UTF8.GetBytes(Environment.NewLine);
    public static readonly byte[] separatorBytes = ";"u8.ToArray();

    public static readonly int cpuCount = Environment.ProcessorCount;
    public const int pageSize = 1024 * 1024;

    public readonly record struct Page(long Start, int Length);

    public static async Task MmfStringProducerConsumer2(string filePath)
    {
        using var queue = new BlockingCollection<Page>(cpuCount);
        using var file = MemoryMappedFile.CreateFromFile(filePath, FileMode.Open);
        using var accessor = file.CreateViewAccessor();
        
        var producer = Task.Run(() => ProducePages(queue, accessor));

        var consumers = Enumerable.Range(0, cpuCount)
            .Select(_ => Task.Run(() => ConsumePages(queue, accessor)))
            .ToList();

        await producer;
        var dsts = await Task.WhenAll(consumers);
        var dst = dsts
            .SelectMany(x => x)
            .GroupBy(kvp => kvp.Key)
            .ToDictionary(g => g.Key, g => CityMeasurements.Combine(g.Select(h => h.Value)));
        
        PrintResultsString(dst);
    }

    private static unsafe Dictionary<string, CityMeasurements> ConsumePages(BlockingCollection<Page> queue, MemoryMappedViewAccessor accessor)
    {
        var dst = new Dictionary<string, CityMeasurements>();

        byte* pointer = (byte*)0;
        accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref pointer);
        foreach (var page in queue.GetConsumingEnumerable())
        { 
            CalcString(new ReadOnlySpan<byte>(pointer + page.Start, page.Length), dst);
        }
        accessor.SafeMemoryMappedViewHandle.ReleasePointer();

        return dst;
    }

    private static unsafe void ProducePages(BlockingCollection<Page> queue, MemoryMappedViewAccessor accessor)
    {
        var capacity = accessor.Capacity;
        var offset = 3L;
        byte* pointer = (byte*)0;
        accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref pointer);

        while (capacity - offset > 0)
        {
            var count = (int)Math.Min(pageSize, capacity - offset);
            var readUpToEol = offset + count - eolBytes.Length;
            for (; readUpToEol >= offset; readUpToEol--)
            {
                var eolWindow = new ReadOnlySpan<byte>(pointer + readUpToEol, eolBytes.Length);
                if (eolWindow.SequenceEqual(eolBytes))
                {
                    break;
                }
            }
            if (readUpToEol > offset)
            {
                queue.Add(new Page(offset, (int)(readUpToEol - offset) + eolBytes.Length));
                offset = readUpToEol + eolBytes.Length;
            }
            else
            {
                break;
            }
        };
        accessor.SafeMemoryMappedViewHandle.ReleasePointer();

        queue.CompleteAdding();
    }

    private static void CalcString(ReadOnlySpan<byte> page, Dictionary<string, CityMeasurements> table)
    {
        var left = 0;
        do
        {
            var buffer = page[left..page.Length];
            var indexOfSeparator = buffer.IndexOf(separatorBytes);
            var city = Encoding.UTF8.GetString(buffer[0..indexOfSeparator]);
            var indexOfEol = buffer[(indexOfSeparator + 1)..].IndexOf(eolBytes);
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
        while (left < page.Length);
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

        await CalculateMetrics.MmfStringProducerConsumer2(filePath);

        return 0;
    }


}