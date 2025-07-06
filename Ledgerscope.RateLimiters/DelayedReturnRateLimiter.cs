using System.Collections.Concurrent;
using System.Threading.RateLimiting;

namespace Ledgerscope.RateLimiters
{
    /// <summary>
    /// A rate limiter that returns permits to the pool after a specified delay.
    /// Similar to a concurrency limiter but with delayed return of permits.
    /// </summary>
    public class DelayedReturnRateLimiter : RateLimiter
    {
        private readonly ConcurrencyLimiter _innerLimiter;
        private readonly TimeSpan _returnDelay;
        private readonly ConcurrentQueue<Task> _delayedReturns;
        private readonly Timer _cleanupTimer;
        private volatile bool _disposed;

        public DelayedReturnRateLimiter(int permitLimit, int queueLimit, TimeSpan returnDelay)
        {
            var options = new ConcurrencyLimiterOptions
            {
                PermitLimit = permitLimit,
                QueueProcessingOrder = QueueProcessingOrder.OldestFirst,
                QueueLimit = queueLimit
            };

            _innerLimiter = new ConcurrencyLimiter(options);
            _returnDelay = returnDelay;
            _delayedReturns = new ConcurrentQueue<Task>();

            // Cleanup completed tasks every 30 seconds to prevent memory leaks
            _cleanupTimer = new Timer(CleanupCompletedTasks, null, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30));
        }

        public override TimeSpan? IdleDuration => _innerLimiter.IdleDuration;

        protected override RateLimitLease AttemptAcquireCore(int permitCount)
        {
            if (permitCount <= 0)
                throw new ArgumentOutOfRangeException(nameof(permitCount));

            if (_disposed)
                return CreateFailedLease();

            var lease = _innerLimiter.AttemptAcquire(permitCount);
            if (lease.IsAcquired)
            {
                return new DelayedReturnLease(this, lease, permitCount);
            }

            lease.Dispose();
            return CreateFailedLease();
        }

        protected override async ValueTask<RateLimitLease> AcquireAsyncCore(int permitCount, CancellationToken cancellationToken = default)
        {
            if (permitCount <= 0)
                throw new ArgumentOutOfRangeException(nameof(permitCount));

            if (_disposed)
                return CreateFailedLease();

            var lease = await _innerLimiter.AcquireAsync(permitCount, cancellationToken);
            if (lease.IsAcquired)
            {
                return new DelayedReturnLease(this, lease, permitCount);
            }

            lease.Dispose();
            return CreateFailedLease();
        }

        private void ReturnPermit(RateLimitLease innerLease)
        {
            if (_disposed)
            {
                innerLease.Dispose();
                return;
            }

            // Schedule the return of the permit after the delay
            var delayTask = Task.Delay(_returnDelay).ContinueWith(_ =>
            {
                innerLease.Dispose();
            });

            _delayedReturns.Enqueue(delayTask);
        }

        private void CleanupCompletedTasks(object? state)
        {
            var tasksToKeep = new List<Task>();

            while (_delayedReturns.TryDequeue(out var task))
            {
                if (!task.IsCompleted)
                {
                    tasksToKeep.Add(task);
                }
            }

            // Re-enqueue uncompleted tasks
            foreach (var task in tasksToKeep)
            {
                _delayedReturns.Enqueue(task);
            }
        }

        private static RateLimitLease CreateFailedLease()
        {
            return new FailedLease();
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing && !_disposed)
            {
                _disposed = true;
                _cleanupTimer?.Dispose();
                _innerLimiter?.Dispose();
            }
            base.Dispose(disposing);
        }

        public override RateLimiterStatistics? GetStatistics()
        {
            return _innerLimiter.GetStatistics();
        }

        private class DelayedReturnLease : RateLimitLease
        {
            private readonly DelayedReturnRateLimiter _limiter;
            private readonly RateLimitLease _innerLease;
            private readonly int _permitCount;
            private bool _disposed;

            public DelayedReturnLease(DelayedReturnRateLimiter limiter, RateLimitLease innerLease, int permitCount)
            {
                _limiter = limiter;
                _innerLease = innerLease;
                _permitCount = permitCount;
            }

            public override bool IsAcquired => !_disposed && _innerLease.IsAcquired;

            public override IEnumerable<string> MetadataNames => _innerLease.MetadataNames;

            public override bool TryGetMetadata(string metadataName, out object? metadata)
            {
                return _innerLease.TryGetMetadata(metadataName, out metadata);
            }

            protected override void Dispose(bool disposing)
            {
                if (disposing && !_disposed)
                {
                    _disposed = true;
                    _limiter.ReturnPermit(_innerLease);
                }
            }
        }

        private class FailedLease : RateLimitLease
        {
            public override bool IsAcquired => false;

            public override IEnumerable<string> MetadataNames => Array.Empty<string>();

            public override bool TryGetMetadata(string metadataName, out object? metadata)
            {
                metadata = null;
                return false;
            }
        }
    }
}
