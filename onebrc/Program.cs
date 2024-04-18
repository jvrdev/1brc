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

    public static CityMeasurements Combine(IList<CityMeasurements> measurements)
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
    private const byte EolByte = (byte)'\n';
    private const byte SeparatorByte = (byte)';';

    private static readonly int CpuCount = Environment.ProcessorCount;

    private readonly record struct Page(long Start, int Length);

    public static async Task MmfStringProducerConsumer2(string filePath)
    {
        using var queue = new BlockingCollection<Page>(CpuCount);
        using var file = MemoryMappedFile.CreateFromFile(filePath, FileMode.Open);
        using var accessor = file.CreateViewAccessor();
        
        var producer = Task.Run(() => ProducePages(queue, accessor));

        var consumers = Enumerable.Range(0, CpuCount)
            .Select(_ => Task.Run(() => ConsumePages(queue, accessor)))
            .ToList();

        await producer;
        var dsts = await Task.WhenAll(consumers);
        var dst = dsts
            .SelectMany(x => x)
            .GroupBy(kvp => kvp.Key)
            .ToDictionary(g => g.Key, g => CityMeasurements.Combine(g.Select(h => h.Value).ToList()));
        
        PrintResultsString(dst);
    }

    private static unsafe Dictionary<string, CityMeasurements> ConsumePages(BlockingCollection<Page> queue, MemoryMappedViewAccessor accessor)
    {
        var dst = new Dictionary<string, CityMeasurements>();

        var pointer = (byte*)0;
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
        var pointer = (byte*)0;
        accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref pointer);

        var pageSize = capacity / CpuCount;
        while (capacity - offset > 0)
        {
            var count = (int)Math.Min(pageSize, capacity - offset);
            var window = new ReadOnlySpan<byte>(pointer + offset, count);
            var readUpToEol = window.LastIndexOf(EolByte);
            if (readUpToEol > 0)
            {
                queue.Add(new Page(offset, readUpToEol + 1));
                offset += readUpToEol + 1;
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
            var buffer = page[left..];
            var indexOfSeparator = buffer.IndexOf(SeparatorByte);
            var city = Encoding.UTF8.GetString(buffer[..indexOfSeparator]);
            var indexOfEol = buffer[(indexOfSeparator + 1)..].IndexOf(EolByte);
            var measurement = ParseMeasurement3(buffer[(indexOfSeparator + 1)..(indexOfSeparator + indexOfEol + 1)]);
            if (table.TryGetValue(city, out var existingMeasurements))
            {
                existingMeasurements.AddMeasurement(measurement);
            }
            else
            {
                table[city] = new CityMeasurements(measurement);
            }
            left += indexOfSeparator + indexOfEol + 1 + 1;
        }
        while (left < page.Length);
    }

    private static float ParseMeasurement(ReadOnlySpan<byte> buffer)
    {
        _ = csFastFloat.FastFloatParser.TryParseFloat(buffer, out var measurement, styles: NumberStyles.AllowDecimalPoint);
        
        return measurement;
    }
    
    private static float ParseMeasurement2(ReadOnlySpan<byte> buffer)
    {
        if (buffer[0] == (byte)'-')
        {
            return -ParseMeasurement2(buffer[1..]);
        }
        
        var x = 0;
        foreach (var t in buffer)
        {
            if (t != (byte)'.')
            {
                x = x * 10 + t - (byte)'0';
            }
        }

        return x / 10.0f;
    }
    
    private static float ParseMeasurement3(ReadOnlySpan<byte> buffer)
    {
        if (buffer[0] == (byte)'-')
        {
            return -ParseMeasurement3(buffer[1..]);
        }

        const byte zero = (byte)'0';

        return buffer.Length switch
        {
            4 => buffer[0] * 10 + buffer[1] + buffer[3] * 0.1f - zero * 11.1f ,
            3 => buffer[0] + buffer[2] * 0.1f - zero * 1.1f ,
            _ => throw new Exception()
        };
    }

    private static float ParseMeasurement4(ReadOnlySpan<byte> buffer)
    {
        float.TryParse(buffer, NumberStyles.AllowDecimalPoint | NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var result);

        return result;
    }

    private static void PrintResultsString(IDictionary<string, CityMeasurements> table)
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
