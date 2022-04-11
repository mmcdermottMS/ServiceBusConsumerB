using System;
using System.Diagnostics;
using System.Net.Http;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.ServiceBus;
using Microsoft.Extensions.Logging;

namespace ServiceBusConsumerB
{
    public class ServiceBusConsumerB
    {
        private static readonly HttpClient _httpClient = new();
        private readonly TelemetryClient _telemetryClient;

        public ServiceBusConsumerB(TelemetryClient telemetryClient)
        {
            _telemetryClient = telemetryClient;
        }

        [FunctionName("ServiceBusConsumerB")]
        public async Task Run(
            [ServiceBusTrigger("%TopicName%", "%SubscriberName%", Connection = "SbnsConnString")]
            ServiceBusReceivedMessage message,
            ServiceBusMessageActions messageActions,
            ILogger log)
        {
            //Grab the existing Diagnostic-Id value from the received message.  The body of this conditional will
            //not execute if the Diagnostic-Id is not a valid string - NFR may dictate a different way of handling that condition
            if (message.ApplicationProperties.TryGetValue("Diagnostic-Id", out var objectId) && objectId is string diagnosticId)
            {
                //Create an activity specific to SB message processing.  See list of all
                //available instrumented operations here: https://docs.microsoft.com/en-us/azure/service-bus-messaging/service-bus-end-to-end-tracing?tabs=net-standard-sdk-2#instrumented-operations
                var activity = new Activity("ServiceBusProcessor.ProcessMessage");

                //Set the parent Id of the activity to the Diagnostic-Id from the received message.  This will 
                //allow the Diagnostic-Id to propagate through the call chain
                activity.SetParentId(diagnosticId);

                //Start a telemetry operation using the newly created activity
                using var operation = _telemetryClient.StartOperation<RequestTelemetry>(activity);

                try
                {
                    /******  DO WORK HERE  ******/
                    var result = await _httpClient.SendAsync(new HttpRequestMessage(HttpMethod.Get, Environment.GetEnvironmentVariable("WeatherServiceUri")));
                    log.LogInformation($"Got Weather Report: {result}");
                    await messageActions.CompleteMessageAsync(message);

                    //Optional custom trace message
                    _telemetryClient.TrackTrace($"Subscriber {Environment.GetEnvironmentVariable("SubscriberName")} processed message: {message.Body}");
                    /********  END WORK  ********/
                }
                catch (Exception ex)
                {
                    await messageActions.AbandonMessageAsync(message);
                    log.LogError(ex, $"Error consuming message from topic {Environment.GetEnvironmentVariable("SubscriberName")} for subscriber {Environment.GetEnvironmentVariable("SubscriberName")}");

                    //For any given failure, track the exception with the TelemetryClient and set the success flag to false.
                    _telemetryClient.TrackException(ex);
                    operation.Telemetry.Success = false;
                    throw;
                }

                //Stop the teleemetry opration once the function has completed.
                operation.Telemetry.Stop();
            }
        }
    }
}
