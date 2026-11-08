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
/// </summary>
internal static class Cancellation
{
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
