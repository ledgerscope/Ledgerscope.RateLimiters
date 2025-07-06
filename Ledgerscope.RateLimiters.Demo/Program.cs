using Ledgerscope.RateLimiters;
using Microsoft.Extensions.Logging;

namespace Ledgerscope.RateLimiters.Demo
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            // Configure logging
            using var loggerFactory = LoggerFactory.Create(builder =>
                builder
                    .SetMinimumLevel(LogLevel.Debug)
                    .AddConsole());

            var logger = loggerFactory.CreateLogger<DelayedReturnRateLimiter>();

            using var limiter = new DelayedReturnRateLimiter(
                5, // Maximum number of permits
                10, // Maximum number in queue
                TimeSpan.FromSeconds(2), // Delay for 2 seconds after returning a permit
                logger
            );

            Console.WriteLine("DelayedReturnRateLimiter Demo with Logging");
            Console.WriteLine("=========================================");

            while (true)
            {
                Console.WriteLine("\nPress any key to acquire a lease, CTRL + C to quit");
                Console.ReadKey();

                Console.WriteLine(DateTime.UtcNow.ToString("u") + " Acquiring lease");

                using var lease = await limiter.AcquireAsync(1);

                Console.WriteLine(DateTime.UtcNow.ToString("u") + " Lease returned: " + lease.IsAcquired);
                
                if (lease.IsAcquired)
                {
                    Console.WriteLine("Lease will be returned to pool after 2 seconds...");
                }
            }
        }
    }
}
