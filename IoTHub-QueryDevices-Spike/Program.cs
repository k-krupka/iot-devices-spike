using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Devices;
using Microsoft.Azure.Devices.Common.Exceptions;
using Microsoft.Azure.Devices.Shared;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace IoTHub_QueryDevices_Spike
{
    public class Program
    {
        public static async Task Main()
        {
            // *************************************************************
            // Get device count for final validation
            // *************************************************************
            // await GetDeviceCountStatsViaQuery("iothub-devices-100");
            // await GetDeviceCountStatsViaQuery("iothub-devices-1000");
            // await GetDeviceCountStatsViaQuery("iothub-devices-10000");
            // await GetDeviceCountStatsViaQuery("iothub-devices-100000");
            // await GetDeviceCountStatsViaQuery("iothub-devices-1000000");

            // *************************************************************
            // Create devices in each of the IoT Hubs
            // *************************************************************
            // await CreateDeices("iothub-devices-100", 100);
            // await CreateDeices("iothub-devices-1000", 1000);
            // await CreateDeices("iothub-devices-10000", 10000);
            // await CreateDeices("iothub-devices-100000", 100000);
            // await CreateDeices("iothub-devices-1000000", 1000000);

            // *************************************************************
            // Query devices... examples
            // *************************************************************

            await QueryQueryDevicesSuite().ConfigureAwait(false);

            Console.WriteLine("press any key to continue...");
            Console.ReadKey();
        }

        private static async Task GetDeviceCountStatsViaQuery(string iotHubName)
        {
            RegistryManager registryManager = RegistryManager.CreateFromConnectionString(Settings.IotHubNameToConnectionStringDictionary[iotHubName]);

            Stopwatch timeToExecuteQuery = new Stopwatch();
            timeToExecuteQuery.Start();

            var count = await GetTotalDeviceCountForGivenIotHub(registryManager).ConfigureAwait(false);

            timeToExecuteQuery.Stop();
            Console.WriteLine($"iot hub: '{iotHubName}' contains a total of {count} devices. Total secons: {timeToExecuteQuery.Elapsed.TotalSeconds:N0}");
        }

        private static async Task QueryQueryDevicesSuite()
        {
            //this dictionary represents the size of iot hub + paging size
            Dictionary<int, List<int>> collectionToAnalyze = new Dictionary<int, List<int>>
            {
                // doesn't make any sense to measure those two numbers, are we always have instant results
                // {100, new List<int>     {500, 1000} },
                // {1000, new List<int>    {500, 1000} },
                // {10000, new List<int>   {500, 1000, 2000, 5000, 10000} },
                // {100000, new List<int>  {500, 1000, 2000, 5000, 10000, 20000, 50000, 100000} },
                // {1000000, new List<int> {500, 1000, 2000, 5000, 10000, 20000, 50000, 100000} },
                {1000000, new List<int> {10000} },
            };

            foreach (KeyValuePair<int, List<int>> input in collectionToAnalyze)
            {
                string iotHubName = "iothub-devices-" + input.Key;

                foreach (int pageSize in input.Value)
                {
                    await QueryDevices(iotHubName, pageSize).ConfigureAwait(false);
                }
            }
        }

        private static async Task QueryDevices(string iotHubName, int pageSize)
        {
            RegistryManager registryManager = RegistryManager.CreateFromConnectionString(Settings.IotHubNameToConnectionStringDictionary[iotHubName]);

            Console.WriteLine($"{iotHubName,-25}, {DateTime.UtcNow:T} before query for number of devices");

            //this piece of code is to hit for the first time the iothub with a query
            string queryString = "SELECT COUNT() AS numberOfDevices FROM devices";
            await registryManager.CreateQuery(queryString, 1).GetNextAsJsonAsync();

            Console.WriteLine($"{iotHubName,-25}, {DateTime.UtcNow:T} after  query for number of devices");

            //starting the actual measurement...
            Stopwatch stopwatch = new Stopwatch();

            stopwatch.Start();

            Console.WriteLine($"{iotHubName,-25}, {DateTime.UtcNow:T} elapsed (before query)... total seconds: {stopwatch.Elapsed.TotalSeconds,7:N0}");

            var query = registryManager.CreateQuery("SELECT deviceId FROM devices", pageSize);

            Console.WriteLine($"{iotHubName,-25}, {DateTime.UtcNow:T} elapsed (after query )... total seconds: {stopwatch.Elapsed.TotalSeconds,7:N0}");

            int currentCount = 0;

            while (query.HasMoreResults)
            {
                Console.WriteLine($"{iotHubName,-25}, {DateTime.UtcNow:T} elapsed (start)... total seconds: {stopwatch.Elapsed.TotalSeconds,7:N0}");

                IEnumerable<string> jsons = await query.GetNextAsJsonAsync().ConfigureAwait(false);

                foreach (string json in jsons)
                {
                    var deviceId = JObject.Parse(json).SelectToken("deviceId")?.Value<string>();

                    currentCount++;
                }

                Console.WriteLine($"{iotHubName,-25}, {DateTime.UtcNow:T}, currentCount {currentCount,7}, elapsed (end  )  ... total seconds: {stopwatch.Elapsed.TotalSeconds,7:N0}");
            }

            stopwatch.Stop();

            Console.WriteLine($"{iotHubName,-25}, paging {pageSize,7}, total seconds: {stopwatch.Elapsed.TotalSeconds,7:N0}");
        }

        private static async Task CreateDeices(string iotHubName, int numberOfDevicesToCreate)
        {
            Console.WriteLine($"{DateTime.Now} Starting processing.");

            Stopwatch createTheDevicesStopwatch = new Stopwatch();
            createTheDevicesStopwatch.Start();

            RegistryManager registryManager = RegistryManager.CreateFromConnectionString(Settings.IotHubNameToConnectionStringDictionary[iotHubName]);

            int totalDeviceCount = await GetTotalDeviceCountForGivenIotHub(registryManager);

            Console.WriteLine("number of all to create    : " + numberOfDevicesToCreate);
            Console.WriteLine("number of currently created: " + totalDeviceCount);
            Console.WriteLine("number to create           : " + (numberOfDevicesToCreate - totalDeviceCount));

            if (totalDeviceCount > numberOfDevicesToCreate)
            {
                Console.WriteLine($"{DateTime.Now} iot hub: '{iotHubName}'. Too many devices created. Preparing to remove.");

                int numberOfRemovedDevices = 0;

                while (numberOfRemovedDevices < totalDeviceCount - numberOfDevicesToCreate)
                {
                    string queryString = "SELECT deviceId FROM devices";
                    IQuery query = registryManager.CreateQuery(queryString, 10000);
                    IEnumerable<string> jsons = await query.GetNextAsJsonAsync().ConfigureAwait(false);


                    foreach (string json in jsons)
                    {
                        Console.WriteLine($"removing {numberOfRemovedDevices} / {totalDeviceCount - numberOfDevicesToCreate}");
                        var deviceIdToRemove = JObject.Parse(json).SelectToken("deviceId")?.Value<string>();

                        await registryManager.RemoveDeviceAsync(deviceIdToRemove);

                        numberOfRemovedDevices++;

                        if (numberOfRemovedDevices == totalDeviceCount - numberOfDevicesToCreate)
                        {
                            break;
                        }
                    }
                }

            }
            else if (totalDeviceCount == numberOfDevicesToCreate)
            {
                Console.WriteLine($"{DateTime.Now} iot hub: '{iotHubName}'. Creation of devices skipped. All devices already created.");
            }
            else
            {
                Console.WriteLine($"{DateTime.Now} iot hub: '{iotHubName}'. Creation of {numberOfDevicesToCreate} devices starting...");

                int currentIndex = totalDeviceCount;

                IList<int> devicesToCreate = Enumerable.Range(0, numberOfDevicesToCreate - totalDeviceCount).ToList();

                var allTasks = new List<Task>();
                var throttler = new SemaphoreSlim(40);

                foreach (var _ in devicesToCreate)
                {
                    // do an async wait until we can schedule again
                    await throttler.WaitAsync();

                    allTasks.Add(Task.Run(async () =>
                    {
                        try
                        {
                            while (true) //this while is a dummy implementation of retry policy
                            {
                                try
                                {
                                    await registryManager.AddDeviceAsync(new Device("device-" + Guid.NewGuid())).ConfigureAwait(false);
                                    break;
                                }
                                catch (ThrottlingException)
                                {
                                    Console.WriteLine($"[{iotHubName}] throttling exception... retrying");
                                    await Task.Delay(1000 * 10).ConfigureAwait(false);
                                }
                            }
                        }
                        finally
                        {
                            throttler.Release();

                            Interlocked.Increment(ref currentIndex);
                            if (currentIndex % 50 == 0) Console.WriteLine($"status ('{iotHubName}'): {currentIndex}");
                        }
                    }));
                }

                await Task.WhenAll(allTasks);

                createTheDevicesStopwatch.Stop();

                Console.WriteLine($"{DateTime.Now} iothub: '{iotHubName}'. Creation of {numberOfDevicesToCreate} devices took {createTheDevicesStopwatch.Elapsed.TotalSeconds:N0} seconds");
            }
        }

        private static async Task<int> GetTotalDeviceCountForGivenIotHub(RegistryManager regMgr)
        {
            string queryString = "SELECT COUNT() AS numberOfDevices FROM devices";
            IQuery query = regMgr.CreateQuery(queryString, 1);
            string json = (await query.GetNextAsJsonAsync()).FirstOrDefault();
            Dictionary<string, long> data = JsonConvert.DeserializeObject<Dictionary<string, long>>(json);
            long count1 = data["numberOfDevices"];
            return (int)count1;
        }
    }
}
