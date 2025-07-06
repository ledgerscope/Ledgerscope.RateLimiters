using System.Threading.RateLimiting;
using Ledgerscope.RateLimiters;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ledgerscope.RateLimiters.Tests
{
    [TestClass]
    public class DelayedReturnRateLimiterTests
    {
        private DelayedReturnRateLimiter _rateLimiter;

        [TestCleanup]
        public void TestCleanup()
        {
            _rateLimiter?.Dispose();
        }

        [TestMethod]
        public void MyTestMethod()
        {
            var cl = new ConcurrencyLimiter(new ConcurrencyLimiterOptions
            {
                PermitLimit = 1,
                QueueProcessingOrder = QueueProcessingOrder.OldestFirst,
                QueueLimit = 0 // No queueing, fail immediately if no permits available
            });


            var result = cl.AttemptAcquire(1);
            Assert.IsTrue(result.IsAcquired);
        }

        [TestMethod]
        public void Constructor_ValidParameters_CreatesInstance()
        {
            // Arrange & Act
            _rateLimiter = new DelayedReturnRateLimiter(5, 0, TimeSpan.FromMilliseconds(100), NullLogger<DelayedReturnRateLimiter>.Instance);

            // Assert
            Assert.IsNotNull(_rateLimiter);
            Assert.IsNotNull(_rateLimiter.IdleDuration);
        }

        [TestMethod]
        public void GetStatistics_InitialState_ReturnsCorrectAvailablePermits()
        {
            // Arrange
            const int permitLimit = 3;
            _rateLimiter = new DelayedReturnRateLimiter(permitLimit, 0, TimeSpan.FromMilliseconds(100), NullLogger<DelayedReturnRateLimiter>.Instance);

            // Act
            var statistics = _rateLimiter.GetStatistics();

            // Assert
            Assert.AreEqual(permitLimit, statistics.CurrentAvailablePermits);
        }

        [TestMethod]
        [DataRow(0)]
        [DataRow(-1)]
        [DataRow(-5)]
        public void AttemptAcquire_InvalidPermitCount_ThrowsArgumentOutOfRangeException(int permitCount)
        {
            // Arrange
            _rateLimiter = new DelayedReturnRateLimiter(5, 0, TimeSpan.FromMilliseconds(100), NullLogger<DelayedReturnRateLimiter>.Instance);

            // Act & Assert
            Assert.ThrowsException<ArgumentOutOfRangeException>(() => _rateLimiter.AttemptAcquire(permitCount));
        }

        [TestMethod]
        public void AttemptAcquire_ValidPermitCount_ReturnsSuccessfulLease()
        {
            // Arrange
            _rateLimiter = new DelayedReturnRateLimiter(5, 0, TimeSpan.FromMilliseconds(100), NullLogger<DelayedReturnRateLimiter>.Instance);

            // Act
            using var lease = _rateLimiter.AttemptAcquire(1);

            // Assert
            Assert.IsTrue(lease.IsAcquired);
        }

        [TestMethod]
        public void AttemptAcquire_ExceedsPermitLimit_ReturnsFailedLease()
        {
            // Arrange
            const int permitLimit = 2;
            _rateLimiter = new DelayedReturnRateLimiter(permitLimit, 0, TimeSpan.FromMilliseconds(100), NullLogger<DelayedReturnRateLimiter>.Instance);

            // Act
            using var lease1 = _rateLimiter.AttemptAcquire(1);
            using var lease2 = _rateLimiter.AttemptAcquire(1);
            using var lease3 = _rateLimiter.AttemptAcquire(1);

            // Assert
            Assert.IsTrue(lease1.IsAcquired);
            Assert.IsTrue(lease2.IsAcquired);
            Assert.IsFalse(lease3.IsAcquired);
        }

        [TestMethod]
        public async Task AcquireAsync_ValidPermitCount_ReturnsSuccessfulLease()
        {
            // Arrange
            _rateLimiter = new DelayedReturnRateLimiter(5, 0, TimeSpan.FromMilliseconds(100), NullLogger<DelayedReturnRateLimiter>.Instance);

            // Act
            using var lease = await _rateLimiter.AcquireAsync(1);

            // Assert
            Assert.IsTrue(lease.IsAcquired);
        }

        [TestMethod]
        [DataRow(0)]
        [DataRow(-1)]
        [DataRow(-5)]
        public async Task AcquireAsync_InvalidPermitCount_ThrowsArgumentOutOfRangeException(int permitCount)
        {
            // Arrange
            _rateLimiter = new DelayedReturnRateLimiter(5, 0, TimeSpan.FromMilliseconds(100), NullLogger<DelayedReturnRateLimiter>.Instance);

            // Act & Assert
            await Assert.ThrowsExceptionAsync<ArgumentOutOfRangeException>(() => _rateLimiter.AcquireAsync(permitCount).AsTask());
        }

        [TestMethod]
        public async Task AcquireAsync_WithCancellation_ThrowsOperationCanceledException()
        {
            // Arrange
            _rateLimiter = new DelayedReturnRateLimiter(1, 5, TimeSpan.FromMilliseconds(100), NullLogger<DelayedReturnRateLimiter>.Instance);

            using var lease = _rateLimiter.AttemptAcquire(1); // Consume the only permit
            using var cts = new CancellationTokenSource();

            // Act
            var acquireTask = _rateLimiter.AcquireAsync(1, cts.Token);
            cts.Cancel();

            // Assert
            await Assert.ThrowsExceptionAsync<TaskCanceledException>(() => acquireTask.AsTask());
        }

        [TestMethod]
        public async Task DelayedReturn_PermitReturnedAfterDelay()
        {
            // Arrange
            const int permitLimit = 1;
            var returnDelay = TimeSpan.FromMilliseconds(200);
            _rateLimiter = new DelayedReturnRateLimiter(permitLimit, 0, returnDelay, NullLogger<DelayedReturnRateLimiter>.Instance);

            // Act - Acquire and immediately dispose to trigger delayed return
            using (var lease = _rateLimiter.AttemptAcquire(1))
            {
                Assert.IsTrue(lease.IsAcquired);
            }

            // Immediately after disposal, permit should not be available
            using var immediateRetryLease = _rateLimiter.AttemptAcquire(1);
            Assert.IsFalse(immediateRetryLease.IsAcquired);

            // Wait for the delay period plus some buffer
            await Task.Delay(returnDelay.Add(TimeSpan.FromMilliseconds(50)));

            // Now the permit should be available again
            using var delayedRetryLease = _rateLimiter.AttemptAcquire(1);
            Assert.IsTrue(delayedRetryLease.IsAcquired);
        }

        [TestMethod]
        public void StatisticsUpdate_AfterAcquireAndReturn()
        {
            // Arrange
            const int permitLimit = 3;
            _rateLimiter = new DelayedReturnRateLimiter(permitLimit, 0, TimeSpan.FromMilliseconds(100), NullLogger<DelayedReturnRateLimiter>.Instance);

            // Act & Assert - Initial state
            var initialStats = _rateLimiter.GetStatistics();
            Assert.AreEqual(permitLimit, initialStats.CurrentAvailablePermits);

            // Acquire permit
            using var lease = _rateLimiter.AttemptAcquire(1);
            var afterAcquireStats = _rateLimiter.GetStatistics();
            Assert.AreEqual(permitLimit - 1, afterAcquireStats.CurrentAvailablePermits);
        }

        [TestMethod]
        public void Lease_MetadataOperations_BehavesCorrectly()
        {
            // Arrange
            _rateLimiter = new DelayedReturnRateLimiter(5, 0, TimeSpan.FromMilliseconds(100), NullLogger<DelayedReturnRateLimiter>.Instance);

            // Act
            using var lease = _rateLimiter.AttemptAcquire(1);

            // Assert
            Assert.AreEqual(1, lease.MetadataNames.Count());
            Assert.IsFalse(lease.TryGetMetadata("any-key", out var metadata));
            Assert.IsNull(metadata);
        }

        [TestMethod]
        public void FailedLease_MetadataOperations_BehavesCorrectly()
        {
            // Arrange
            _rateLimiter = new DelayedReturnRateLimiter(1, 0, TimeSpan.FromMilliseconds(100), NullLogger<DelayedReturnRateLimiter>.Instance);
            using var successfulLease = _rateLimiter.AttemptAcquire(1);

            // Act - Try to acquire when no permits available
            using var failedLease = _rateLimiter.AttemptAcquire(1);

            // Assert
            Assert.IsFalse(failedLease.IsAcquired);
            Assert.AreEqual(0, failedLease.MetadataNames.Count());
            Assert.IsFalse(failedLease.TryGetMetadata("any-key", out var metadata));
            Assert.IsNull(metadata);
        }

        [TestMethod]
        public void Dispose_AfterDispose_AcquireReturnsFailedLease()
        {
            // Arrange
            _rateLimiter = new DelayedReturnRateLimiter(5, 0, TimeSpan.FromMilliseconds(100), NullLogger<DelayedReturnRateLimiter>.Instance);

            // Act
            _rateLimiter.Dispose();
            using var lease = _rateLimiter.AttemptAcquire(1);

            // Assert
            Assert.IsFalse(lease.IsAcquired);
        }

        [TestMethod]
        public async Task Dispose_AfterDispose_AcquireAsyncReturnsFailedLease()
        {
            // Arrange
            _rateLimiter = new DelayedReturnRateLimiter(5, 0, TimeSpan.FromMilliseconds(100), NullLogger<DelayedReturnRateLimiter>.Instance);

            // Act
            _rateLimiter.Dispose();
            using var lease = await _rateLimiter.AcquireAsync(1);

            // Assert
            Assert.IsFalse(lease.IsAcquired);
        }

        [TestMethod]
        public async Task ConcurrentAccess_MultipleThreads_BehavesCorrectly()
        {
            // Arrange
            const int permitLimit = 2;
            const int threadCount = 5;
            _rateLimiter = new DelayedReturnRateLimiter(permitLimit, 0, TimeSpan.FromMilliseconds(50), NullLogger<DelayedReturnRateLimiter>.Instance);
            var successCount = 0;
            var tasks = new Task[threadCount];

            // Act
            for (int i = 0; i < threadCount; i++)
            {
                tasks[i] = Task.Run(() =>
                {
                    using var lease = _rateLimiter.AttemptAcquire(1);
                    if (lease.IsAcquired)
                    {
                        Interlocked.Increment(ref successCount);
                    }
                });
            }

            await Task.WhenAll(tasks);

            // Assert
            Assert.AreEqual(permitLimit, successCount);
        }

        [TestMethod]
        public void Lease_Dispose_IsAcquiredBecomesFalse()
        {
            // Arrange
            _rateLimiter = new DelayedReturnRateLimiter(5, 0, TimeSpan.FromMilliseconds(100), NullLogger<DelayedReturnRateLimiter>.Instance);
            var lease = _rateLimiter.AttemptAcquire(1);

            // Act
            Assert.IsTrue(lease.IsAcquired);
            lease.Dispose();

            // Assert
            Assert.IsFalse(lease.IsAcquired);
        }

        [TestMethod]
        public void MultiplePermits_AcquireAndRelease_WorksCorrectly()
        {
            // Arrange
            const int permitLimit = 5;
            const int permitsToAcquire = 3;
            _rateLimiter = new DelayedReturnRateLimiter(permitLimit, 0, TimeSpan.FromMilliseconds(100), NullLogger<DelayedReturnRateLimiter>.Instance);

            // Act
            using var lease = _rateLimiter.AttemptAcquire(permitsToAcquire);
            var statistics = _rateLimiter.GetStatistics();

            // Assert
            Assert.IsTrue(lease.IsAcquired);
            Assert.AreEqual(permitLimit - permitsToAcquire, statistics.CurrentAvailablePermits);
        }

        [TestMethod]
        public async Task CleanupTimer_DoesNotCrashApplication()
        {
            // Arrange
            _rateLimiter = new DelayedReturnRateLimiter(1, 0, TimeSpan.FromMilliseconds(50), NullLogger<DelayedReturnRateLimiter>.Instance);

            // Act - Create several leases to populate the delayed returns queue
            for (int i = 0; i < 10; i++)
            {
                using var lease = _rateLimiter.AttemptAcquire(1);
                await Task.Delay(60); // Wait longer than return delay
            }

            // Assert - If we reach here, the cleanup timer didn't crash
            Assert.IsTrue(true);
        }

        [TestMethod]
        public async Task SequentialAcquire_WithDelayedReturn_EventuallySucceeds()
        {
            // Arrange
            const int permitLimit = 1;
            var returnDelay = TimeSpan.FromMilliseconds(100);
            _rateLimiter = new DelayedReturnRateLimiter(permitLimit, 0, returnDelay, NullLogger<DelayedReturnRateLimiter>.Instance);

            // Act & Assert
            // First acquisition should succeed
            using (var lease1 = _rateLimiter.AttemptAcquire(1))
            {
                Assert.IsTrue(lease1.IsAcquired);
            }

            // Second acquisition should fail immediately
            using (var lease2 = _rateLimiter.AttemptAcquire(1))
            {
                Assert.IsFalse(lease2.IsAcquired);
            }

            // Wait for delayed return
            await Task.Delay(returnDelay.Add(TimeSpan.FromMilliseconds(50)));

            // Third acquisition should succeed after delay
            using (var lease3 = _rateLimiter.AttemptAcquire(1))
            {
                Assert.IsTrue(lease3.IsAcquired);
            }
        }
    }
}