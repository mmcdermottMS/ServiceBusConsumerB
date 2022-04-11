using Microsoft.Azure.Functions.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;

[assembly: FunctionsStartup(typeof(ServiceBusConsumerB.Startup))]
namespace ServiceBusConsumerB
{
    public class Startup : FunctionsStartup
    {
        public override void Configure(IFunctionsHostBuilder builder)
        {
            builder.Services.AddApplicationInsightsTelemetryWorkerService();
        }
    }
}
