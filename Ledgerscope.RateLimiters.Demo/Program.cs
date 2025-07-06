using Ledgerscope.RateLimiters;

namespace Ledgerscope.RateLimiters.Demo
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            using var limiter = new DelayedReturnRateLimiter(
                5, // Maximum number of permits
                10, // Maximum number of delayed returns
                System.TimeSpan.FromSeconds(10) // Delay for 1 second after returning a permit
            );

            while (true)
            {
                Console.WriteLine("Press any key to acquire a lease, CTRL + C to quit");
                Console.ReadKey();

                Console.WriteLine(DateTime.UtcNow.ToString("u") + " Acquiring lease");

                using var lease = await limiter.AcquireAsync(1);

                Console.WriteLine(DateTime.UtcNow.ToString("u") + " Lease returned: " + lease.IsAcquired);
            }
        }
    }
}
