using System;
using Godot;
using Microsoft.Extensions.Logging;

namespace PugPong.Client;

/// <summary>
/// Minimal <see cref="ILogger{T}"/> that forwards PUG.Ensemble's structured
/// logs to Godot's stdout/stderr (which the run-demo harness captures into
/// client-{A,B}.log). Without this, the sample passes
/// <c>NullLogger</c> and the SDK's diagnostics — connection decisions, dial
/// outcomes, queue errors — are invisible during an e2e.
/// </summary>
public sealed class GodotLogger<T> : ILogger<T>, ILogger
{
    private readonly string _category = typeof(T).Name;

    IDisposable? ILogger.BeginScope<TState>(TState state) => null;

    public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None;

    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        if (!IsEnabled(logLevel)) return;
        var line = $"[{_category}] {logLevel}: {formatter(state, exception)}";
        if (exception is not null) line += $"\n{exception}";
        if (logLevel >= LogLevel.Error)
            GD.PrintErr(line);
        else
            GD.Print(line);
    }
}
