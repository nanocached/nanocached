namespace Nanocached;

/// <summary>
/// What a <see cref="CancellationToken"/> means for a call on the client.
/// Requests are pipelined on one connection and answered in order, so a
/// request already written cannot be taken back, and no token ever reaches
/// the stream (a half-written frame would desynchronize the pipeline).
/// Cancelling therefore <em>abandons the wait</em>: the returned task ends
/// with <see cref="OperationCanceledException"/>, the connection stays
/// usable, and the operation itself may still run to completion on the
/// cluster, its result discarded.
/// <para>A token that is already cancelled when the call is made sends
/// nothing. After a cancellation a write — and above all a counter or
/// compare-and-set operation — has an outcome the caller does not know,
/// exactly as after <see cref="ConnectionLostException"/>. A cancelled
/// call that had not reached the wire yet (parked on a ring refresh or a
/// redial) keeps going and may land after calls the caller made later, so
/// an abandoned write can overwrite a newer one for the same key.</para>
/// <para>Because the abandoned operation can keep running after the call
/// has thrown, it must not keep reading arrays the caller gets back: a
/// <c>byte[]</c> key, value or batch value passed with a token that can be
/// cancelled is copied at call time (<see cref="Own"/>), before the first
/// await, so a caller that reuses the buffer after a cancellation
/// (an <c>ArrayPool</c>, a request buffer) cannot make the abandoned
/// operation store, delete, increment or touch a different key or
/// value.</para>
/// </summary>
internal static class Cancellation
{
    /// <summary>The caller's array, or a copy when
    /// <paramref name="cancellationToken"/> can be cancelled (a token that
    /// never is — <c>default</c> — costs nothing). Applied to the arguments
    /// of operations that write or change a key's expiry; an abandoned read
    /// that reads a changed buffer only returns a value nobody awaits.
    /// <c>null</c> is passed on so the argument check still reports
    /// it.</summary>
    internal static byte[] Own(byte[] array, CancellationToken cancellationToken) =>
        cancellationToken.CanBeCanceled && array is not null ? (byte[])array.Clone() : array!;

    /// <summary><see cref="Own"/> for the values of a batch: a new
    /// dictionary with each value copied, so neither the arrays nor the
    /// dictionary can change under the abandoned operation.</summary>
    internal static IReadOnlyDictionary<string, byte[]> OwnValues(
        IReadOnlyDictionary<string, byte[]> values, CancellationToken cancellationToken)
    {
        if (!cancellationToken.CanBeCanceled || values is null)
        {
            return values!;
        }
        var copy = new Dictionary<string, byte[]>(values.Count);
        foreach ((string key, byte[] value) in values)
        {
            copy[key] = value is null ? value! : (byte[])value.Clone();
        }
        return copy;
    }

    internal static Task<T> Abandonable<T>(Task<T> work, CancellationToken cancellationToken)
    {
        if (!cancellationToken.CanBeCanceled || work.IsCompleted)
        {
            return work;
        }
        Observe(work);
        return work.WaitAsync(cancellationToken);
    }

    internal static Task Abandonable(Task work, CancellationToken cancellationToken)
    {
        if (!cancellationToken.CanBeCanceled || work.IsCompleted)
        {
            return work;
        }
        Observe(work);
        return work.WaitAsync(cancellationToken);
    }

    /// <summary>An abandoned task that later fails must not surface as an
    /// unobserved exception.</summary>
    private static void Observe(Task work) =>
        work.ContinueWith(
            static finished => _ = finished.Exception,
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
}
