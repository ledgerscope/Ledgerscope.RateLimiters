using System.Collections.Concurrent;
using System.Threading.RateLimiting;
using Microsoft.Extensions.Logging;

namespace Ledgerscope.RateLimiters
{
    /// <summary>
    /// A rate limiter that returns permits to the pool after a specified delay.
    /// Similar to a concurrency limiter but with delayed return of permits.
    /// </summary>
    public partial class DelayedReturnRateLimiter : RateLimiter
    {
        private readonly ConcurrencyLimiter _innerLimiter;
        private readonly TimeSpan _returnDelay;
        private readonly ConcurrentQueue<Task> _delayedReturns;
        private readonly Timer _cleanupTimer;
        private readonly ILogger<DelayedReturnRateLimiter> _logger;
        private volatile bool _disposed;

        public DelayedReturnRateLimiter(int permitLimit, int queueLimit, TimeSpan returnDelay, ILogger<DelayedReturnRateLimiter> logger)
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
            _logger = logger;

            LogRateLimiterCreated(permitLimit, queueLimit, returnDelay);

            // Cleanup completed tasks every 30 seconds to prevent memory leaks
            _cleanupTimer = new Timer(CleanupCompletedTasks, null, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30));
        }

        public override TimeSpan? IdleDuration => _innerLimiter.IdleDuration;

        protected override RateLimitLease AttemptAcquireCore(int permitCount)
        {
            if (permitCount <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(permitCount));
            }

            if (_disposed)
            {
                LogAttemptAcquireOnDisposed();
                return CreateFailedLease();
            }

            LogAttemptingAcquire(permitCount);
            var lease = _innerLimiter.AttemptAcquire(permitCount);
            if (lease.IsAcquired)
            {
                LogPermitAcquired(permitCount);
                return new DelayedReturnLease(this, lease, permitCount);
            }

            LogPermitAcquisitionFailed(permitCount);
            lease.Dispose();
            return CreateFailedLease();
        }

        protected override async ValueTask<RateLimitLease> AcquireAsyncCore(int permitCount, CancellationToken cancellationToken = default)
        {
            if (permitCount <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(permitCount));
            }

            if (_disposed)
            {
                LogAttemptAcquireOnDisposed();
                return CreateFailedLease();
            }

            LogAttemptingAcquireAsync(permitCount);
            var lease = await _innerLimiter.AcquireAsync(permitCount, cancellationToken);
            if (lease.IsAcquired)
            {
                LogPermitAcquiredAsync(permitCount);
                return new DelayedReturnLease(this, lease, permitCount);
            }

            LogPermitAcquisitionFailedAsync(permitCount);
            lease.Dispose();
            return CreateFailedLease();
        }

        private void ReturnPermit(RateLimitLease innerLease)
        {
            if (_disposed)
            {
                LogReturnPermitOnDisposed();
                innerLease.Dispose();
                return;
            }

            LogSchedulingPermitReturn(_returnDelay);
            
            // Schedule the return of the permit after the delay
            var delayTask = Task.Delay(_returnDelay).ContinueWith(_ =>
            {
                LogPermitReturned();
                innerLease.Dispose();
            });

            _delayedReturns.Enqueue(delayTask);
        }

        private void CleanupCompletedTasks(object? state)
        {
            var tasksToKeep = new List<Task>();
            int completedTasks = 0;
            int totalTasks = 0;

            while (_delayedReturns.TryDequeue(out var task))
            {
                totalTasks++;
                if (!task.IsCompleted)
                {
                    tasksToKeep.Add(task);
                }
                else
                {
                    completedTasks++;
                }
            }

            // Re-enqueue uncompleted tasks
            foreach (var task in tasksToKeep)
            {
                _delayedReturns.Enqueue(task);
            }

            LogCleanupCompleted(completedTasks, totalTasks);
        }

        private static RateLimitLease CreateFailedLease()
        {
            return new FailedLease();
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing && !_disposed)
            {
                LogDisposing();
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

        // Source-generated logging methods
        [LoggerMessage(Level = LogLevel.Information, Message = "DelayedReturnRateLimiter created with permitLimit={PermitLimit}, queueLimit={QueueLimit}, returnDelay={ReturnDelay}")]
        private partial void LogRateLimiterCreated(int permitLimit, int queueLimit, TimeSpan returnDelay);

        [LoggerMessage(Level = LogLevel.Warning, Message = "Attempt to acquire permit on disposed rate limiter")]
        private partial void LogAttemptAcquireOnDisposed();

        [LoggerMessage(Level = LogLevel.Debug, Message = "Attempting to acquire {PermitCount} permits")]
        private partial void LogAttemptingAcquire(int permitCount);

        [LoggerMessage(Level = LogLevel.Debug, Message = "Successfully acquired {PermitCount} permits")]
        private partial void LogPermitAcquired(int permitCount);

        [LoggerMessage(Level = LogLevel.Debug, Message = "Failed to acquire {PermitCount} permits")]
        private partial void LogPermitAcquisitionFailed(int permitCount);

        [LoggerMessage(Level = LogLevel.Debug, Message = "Attempting to acquire {PermitCount} permits asynchronously")]
        private partial void LogAttemptingAcquireAsync(int permitCount);

        [LoggerMessage(Level = LogLevel.Debug, Message = "Successfully acquired {PermitCount} permits asynchronously")]
        private partial void LogPermitAcquiredAsync(int permitCount);

        [LoggerMessage(Level = LogLevel.Debug, Message = "Failed to acquire {PermitCount} permits asynchronously")]
        private partial void LogPermitAcquisitionFailedAsync(int permitCount);

        [LoggerMessage(Level = LogLevel.Warning, Message = "Attempt to return permit on disposed rate limiter")]
        private partial void LogReturnPermitOnDisposed();

        [LoggerMessage(Level = LogLevel.Debug, Message = "Scheduling permit return with delay {ReturnDelay}")]
        private partial void LogSchedulingPermitReturn(TimeSpan returnDelay);

        [LoggerMessage(Level = LogLevel.Debug, Message = "Permit returned to pool")]
        private partial void LogPermitReturned();

        [LoggerMessage(Level = LogLevel.Debug, Message = "Cleanup completed: {CompletedTasks} completed tasks out of {TotalTasks} total tasks")]
        private partial void LogCleanupCompleted(int completedTasks, int totalTasks);

        [LoggerMessage(Level = LogLevel.Information, Message = "DelayedReturnRateLimiter is being disposed")]
        private partial void LogDisposing();

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
