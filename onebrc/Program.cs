using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Data.SqlTypes;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;

namespace onebrc;

public record CityMeasurements<TCity>(float measurement)
{
    public float Max = measurement;
    public float Min = measurement;
    public float Mean => Total / SampleCount;

    public string Summary(TCity city) => $"{city}={Min:F1}/{Mean:F1}/{Max:F1}";

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

public class CalculateMetrics<TCity>
{
    public static Dictionary<string, CityMeasurements<TCity>> CalculateAveragesSync(string filePath)
    {
        var measurements = ReadMeasurementsMemoryMappedFile(filePath);
        //var measurements = ReadMeasurementsStreamReader(filePath);

        var table = new Dictionary<string, CityMeasurements<TCity>>();

        foreach (var (city, measurement) in measurements)
        {
            if (table.TryGetValue(city, out var existingMeasurements))
            {
                existingMeasurements.AddMeasurement(measurement);
            }
            else
            {
                table[city] = new CityMeasurements<TCity>(measurement);
            }
        }

        return table;
    }

    public static async Task<IDictionary<string, CityMeasurements<string>>> CalculateAveragesAsyncString(string filePath)
    {
        //var measurements = ReadMeasurementsMemoryMappedFile(filePath);
        var measurements = ReadMeasurementsStreamReaderString(filePath);

        //var table = await measurements.AggregateAsync(
        //    new ConcurrentDictionary<string, CityMeasurements<string>>(),
        //    (table, pair) =>
        //    {
        //        table.AddOrUpdate(
        //            pair.Item1,
        //            new CityMeasurements<string>(pair.Item2),
        //            (city, existing) => { existing.AddMeasurement(pair.Item2); return existing; });
        //        return table;
        //    });

        var table = new ConcurrentDictionary<string, CityMeasurements<string>>();

        await foreach (var (city, measurement) in measurements)
        {
            if (table.TryGetValue(city, out var existingMeasurements))
            {
                existingMeasurements.AddMeasurement(measurement);
            }
            else
            {
                table[city] = new CityMeasurements<string>(measurement);
            }
        }

        return table;
    }

    public static async Task<IDictionary<ReadOnlyMemory<char>, CityMeasurements<TCity>>> CalculateAveragesAsync(string filePath)
    {
        //var measurements = ReadMeasurementsMemoryMappedFile(filePath);
        var measurements = ReadMeasurementsStreamReader(filePath);

        var table = new Dictionary<ReadOnlyMemory<char>, CityMeasurements<TCity>>();

        await foreach (var (city, measurement) in measurements)
        {
            if (table.TryGetValue(city, out var existingMeasurements))
            {
                existingMeasurements.AddMeasurement(measurement);
            }
            else
            {
                table[city] = new CityMeasurements<TCity>(measurement);
            }
        }

        //await Parallel.ForEachAsync(measurements, new ParallelOptions { MaxDegreeOfParallelism = 8 }, (x, s) =>
        //{
        //    if (table.TryGetValue(x.Item1, out var existingMeasurements))
        //    {
        //        existingMeasurements.AddMeasurement(x.Item2);
        //    }
        //    else
        //    {
        //        table[x.Item1] = new CityMeasurements(x.Item2);
        //    }

        //    return ValueTask.CompletedTask;
        //});

        return table;
    }

    public static async Task BlockingCollection(string filePath)
    {

        using var bc = new BlockingCollection<List<string>>(1024);

        var producerTask = Task.Run(async () =>
        {
            string? line;
            using var reader = new StreamReader(filePath, Encoding.UTF8, true, 64 * 1024);
            do
            {
                const int pageSize = 1024;
                var page = new List<string>(pageSize);
                for (var i = 0; (line = await reader.ReadLineAsync()) is not null && i < pageSize; i++)
                    ;// page.Add(line);
                //bc.Add(page);
            }
            while (line is not null);
            bc.CompleteAdding();
        });

        var table = new ConcurrentDictionary<string, CityMeasurements<string>>();

        var consumerTask = Task.Run(() =>
        {
            List<string> page;
            while ((page = bc.Take()) is not null)
            {
                foreach (var line in page)
                {
                    int separatorPosition = line.IndexOf(';');
                    var city = line[..separatorPosition];
                    //float.TryParse(line.AsSpan(separatorPosition + 1, line.Length - separatorPosition - 1), NumberStyles.AllowDecimalPoint, null, out var measurement);
                    _ = csFastFloat.FastFloatParser.TryParseFloat(line.AsSpan(separatorPosition + 1, line.Length - separatorPosition - 1), out var measurement, styles: NumberStyles.AllowDecimalPoint);
                    if (table.TryGetValue(city, out var existingMeasurements))
                    {
                        existingMeasurements.AddMeasurement(measurement);
                    }
                    else
                    {
                        table[city] = new CityMeasurements<string>(measurement);
                    }
                }
            }
        });

        await Task.WhenAll(consumerTask, producerTask);

        CalculateMetrics<string>.PrintResults(table);
    }

    public static async IAsyncEnumerable<(ReadOnlyMemory<char>, float)> ReadMeasurementsStreamReader(string filePath)
    {
        using var reader = new StreamReader(filePath, Encoding.UTF8, true, 64 * 1024);

        string? line;

        while ((line = await reader.ReadLineAsync()) is not null)
        {
            int separatorPosition = line.IndexOf(';');
            var city = line.AsMemory(0, separatorPosition);
            float.TryParse(line.AsSpan(separatorPosition + 1, line.Length - separatorPosition - 1), NumberStyles.AllowDecimalPoint, null, out var measurement);
            //_ = csFastFloat.FastFloatParser.TryParseFloat(line.AsSpan(separatorPosition + 1, line.Length - separatorPosition - 1), out var measurement, styles: NumberStyles.AllowDecimalPoint);

            yield return (city, measurement);
        }
    }

    public static async IAsyncEnumerable<(string, float)> ReadMeasurementsStreamReaderString(string filePath)
    {
        using var reader = new StreamReader(filePath, Encoding.UTF8, true, 64 * 1024);

        string? line;

        while ((line = await reader.ReadLineAsync()) is not null)
        {
            int separatorPosition = line.IndexOf(';');
            var city = line[..separatorPosition];
            //float.TryParse(line.AsSpan(separatorPosition + 1, line.Length - separatorPosition - 1), NumberStyles.AllowDecimalPoint, null, out var measurement);
            _ = csFastFloat.FastFloatParser.TryParseFloat(line.AsSpan(separatorPosition + 1, line.Length - separatorPosition - 1), out var measurement, styles: NumberStyles.AllowDecimalPoint);

            yield return (city, measurement);
        }
    }

    private static void Calc(byte[] bytes, int length, IDictionary<string, CityMeasurements<string>> table)
    {
        var eolBytes = Encoding.UTF8.GetBytes(Environment.NewLine);
        var separatorBytes = Encoding.UTF8.GetBytes(";");
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
                table[city] = new CityMeasurements<string>(measurement);
            }
            left += indexOfSeparator + indexOfEol + eolBytes.Length + 1;
        }
        while (left < length);
    }

    public static Task Mmf(string filePath)
    {
        var bytes = new byte[512 * 1024];
        var eolBytes = Encoding.UTF8.GetBytes(Environment.NewLine);
        using var file = MemoryMappedFile.CreateFromFile(filePath, FileMode.Open);
        using var accessor = file.CreateViewAccessor();
        var capacity = accessor.Capacity;
        var offset = 3L;
        var dst = new ConcurrentDictionary<string, CityMeasurements<string>>();
        while (capacity - offset > 0)
        {
            var readBytes = accessor.ReadArray(offset, bytes, 0, (int)Math.Min(bytes.LongLength, capacity - offset));
            var readUpToEol = new ReadOnlySpan<byte>(bytes, 0, readBytes).LastIndexOf(eolBytes);
            if (readUpToEol > 0)
            {
                offset += readUpToEol + eolBytes.Length;
                Calc(bytes, readUpToEol + eolBytes.Length, dst);
            }
            else
            {
                break;
            }
        };

        CalculateMetrics<string>.PrintResults(dst);

        return Task.CompletedTask;
    }

    public static IEnumerable<(string, float)> ReadMeasurementsMemoryMappedFile(string filePath)
    {
        var bytes = new byte[4096];
        var chars = new ArrayBufferWriter<char>(bytes.Length);
        using var file = MemoryMappedFile.CreateFromFile(filePath);
        using var accessor = file.CreateViewAccessor();
        var pageStart = 3;

        var bytesRead = 0;

        do
        {
            bytesRead = accessor.ReadArray(pageStart, bytes, 0, (int)Math.Min(bytes.Length, accessor.Capacity - pageStart));
            var charsRead = Encoding.UTF8.GetChars(new ReadOnlySpan<byte>(bytes, 0, bytesRead), chars);

            var left = 0;
            var separatorPosition = -1;
            var eolPosition = -1;
            do
            {
                var line = chars.WrittenSpan[left..];
                separatorPosition = line.IndexOf(';');
                eolPosition = line.IndexOf(Environment.NewLine);
                if (eolPosition > separatorPosition && separatorPosition > -1)
                {
                    var city = line[..separatorPosition];
                    var measurement = float.Parse(line[(separatorPosition + 1)..eolPosition]);
                    left += eolPosition + Environment.NewLine.Length;
                    pageStart += left;
                    yield return (new string(city), measurement);
                }
            } while (eolPosition > separatorPosition && separatorPosition > -1);
        } while (bytesRead == bytes.Length);
    }

    public static void PrintResults(IDictionary<TCity, CityMeasurements<TCity>> table)
    {
        Console.Write("{");
        Console.Write(string.Join(", ", table.OrderBy(x => x.Key).Select(x => x.Value.Summary(x.Key))));
        Console.WriteLine("}");
    }

    public static async Task PrintCalculatedMetricsRom(string filePath)
    {
        var table = await CalculateMetrics<ReadOnlyMemory<char>>.CalculateAveragesAsync(filePath);

        CalculateMetrics<ReadOnlyMemory<char>>.PrintResults(table);
    }

    public static async Task PrintCalculatedMetricsString(string filePath)
    {
        var table = await CalculateAveragesAsyncString(filePath);

        CalculateMetrics<string>.PrintResults(table);
    }
}


public class Program
{
    public static async Task<int> Main(string[] args)
    {
        var filePath = args[0];

        //await CalculateMetrics<ReadOnlyMemory<char>>.PrintCalculatedMetricsString(filePath);
        //await CalculateMetrics<ReadOnlyMemory<char>>.BlockingCollection(filePath);
        await CalculateMetrics<ReadOnlyMemory<char>>.Mmf(filePath);
        
        return 0;
    }


}