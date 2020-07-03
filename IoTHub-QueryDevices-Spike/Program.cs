using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Devices;
using Microsoft.Azure.Devices.Common.Exceptions;
using Newtonsoft.Json;

namespace IoTHub_QueryDevices_Spike
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            // *************************************************************
            // Get device count for final validation
            // *************************************************************
            await GetDeviceCountStatsViaStatistics("iothub-devices-100").ConfigureAwait(false);
            await GetDeviceCountStatsViaStatistics("iothub-devices-1000").ConfigureAwait(false);
            await GetDeviceCountStatsViaStatistics("iothub-devices-10000").ConfigureAwait(false);
            await GetDeviceCountStatsViaStatistics("iothub-devices-100000").ConfigureAwait(false);
            await GetDeviceCountStatsViaStatistics("iothub-devices-1000000").ConfigureAwait(false);
            
            await GetDeviceCountStatsViaQuery("iothub-devices-100").ConfigureAwait(false);
            await GetDeviceCountStatsViaQuery("iothub-devices-1000").ConfigureAwait(false);
            await GetDeviceCountStatsViaQuery("iothub-devices-10000").ConfigureAwait(false);
            await GetDeviceCountStatsViaQuery("iothub-devices-100000").ConfigureAwait(false);
            await GetDeviceCountStatsViaQuery("iothub-devices-1000000").ConfigureAwait(false);

            // *************************************************************
            // Create devices in each of the IoT Hubs
            // *************************************************************
            // CreateDeices("iothub-devices-100", 100);
            // CreateDeices("iothub-devices-1000", 1000);
            // CreateDeices("iothub-devices-10000", 10000);
            // CreateDeices("iothub-devices-100000", 100000);
            // CreateDeices("iothub-devices-1000000", 1000000);

            // *************************************************************
            // Query devices... examples
            // *************************************************************

            //await QueryQueryDevicesSuite().ConfigureAwait(false);

            Console.WriteLine("press any key to continue...");
            Console.ReadKey();
        }

        private static async Task GetDeviceCountStatsViaStatistics(string iotHubName)
        {
            RegistryManager registryManager = RegistryManager.CreateFromConnectionString(Settings.IotHubNameToConnectionStringDictionary[iotHubName]);

            RegistryStatistics stats = await registryManager.GetRegistryStatisticsAsync().ConfigureAwait(false);
            int totalDeviceCount = (int)stats.TotalDeviceCount;

            Console.WriteLine($"iothub (statistics): '{iotHubName}' contains a total of {totalDeviceCount} devices");
        }

        private static async Task GetDeviceCountStatsViaQuery(string iotHubName)
        {
            RegistryManager registryManager = RegistryManager.CreateFromConnectionString(Settings.IotHubNameToConnectionStringDictionary[iotHubName]);

            string queryString = "SELECT COUNT() AS numberOfDevices FROM devices";
            IQuery query = registryManager.CreateQuery(queryString, 1);
            string json = (await query.GetNextAsJsonAsync()).FirstOrDefault();
            Dictionary<string, long> data = JsonConvert.DeserializeObject<Dictionary<string, long>>(json);
            long count1 = data["numberOfDevices"];

            Console.WriteLine($"iothub (query): '{iotHubName}' contains a total of {count1} devices");
        }

        private static async Task QueryQueryDevicesSuite()
        {
            //this dictionary represents the size of iot hub + paging size
            Dictionary<int, List<int>> collectionToAnalyze = new Dictionary<int, List<int>>
            {
                {100, new List<int> {200, 500, 1000} },
                {1000, new List<int> {200, 500, 1000} },
                {10000, new List<int> {500, 1000, 2000, 5000, 10000} },
                // {100000, new List<int> {500, 1000, 2000, 5000, 10000} },
            };

            foreach (KeyValuePair<int, List<int>> input in collectionToAnalyze)
            {
                string currentIotHubName = "iothub-devices-" + input.Key;

                foreach (int pageSize in input.Value)
                {
                    await QueryDevices(currentIotHubName, pageSize).ConfigureAwait(false);
                }
            }
        }

        private static async Task QueryDevices(string iotHubName, int pageSize)
        {
            RegistryManager registryManager = RegistryManager.CreateFromConnectionString(Settings.IotHubNameToConnectionStringDictionary[iotHubName]);

            {
                //this piece of code is to hit for the first time the iothub with a query

                string queryString = "SELECT COUNT() AS numberOfDevices FROM devices";
                IQuery query2 = registryManager.CreateQuery(queryString, 1);
                await query2.GetNextAsJsonAsync();
            }

            LinkedList<string> devicesCollection = new LinkedList<string>();
            Stopwatch stopwatch = new Stopwatch();

            stopwatch.Start();

            var query = registryManager.CreateQuery("SELECT * FROM devices", pageSize);
            while (query.HasMoreResults)
            {
                var page = await query.GetNextAsTwinAsync();
                foreach (var twin in page)
                {
                    devicesCollection.AddLast(twin.DeviceId);
                }
            }

            stopwatch.Stop();

            Console.WriteLine($"{iotHubName,-25}, total seconds: {stopwatch.Elapsed.TotalSeconds,7:N0}, fetched {devicesCollection.Count,7} items, paging {pageSize,4}");
        }

        private static async Task CreateDeices(string iotHubName, int numberOfDevicesToCreate)
        {
            Console.WriteLine($"{DateTime.Now} Starting processing.");

            Stopwatch createTheDevicesStopwatch = new Stopwatch();
            createTheDevicesStopwatch.Start();

            RegistryManager registryManager = RegistryManager.CreateFromConnectionString(Settings.IotHubNameToConnectionStringDictionary[iotHubName]);

            RegistryStatistics stats = await registryManager.GetRegistryStatisticsAsync().ConfigureAwait(false);
            int totalDeviceCount = (int)stats.TotalDeviceCount;

            Console.WriteLine("number of already created: " + totalDeviceCount);
            Console.WriteLine("number to create         : " + numberOfDevicesToCreate);

            if (totalDeviceCount == numberOfDevicesToCreate)
            {
                Console.WriteLine($"{DateTime.Now} iothub: '{iotHubName}'. Creation of {numberOfDevicesToCreate} devices skipped. All devices already created.");
                return;
            }

            int currentNumberOfCreatedDevices = totalDeviceCount;

            Parallel.For(totalDeviceCount, numberOfDevicesToCreate, async i =>
            {
                Interlocked.Increment(ref currentNumberOfCreatedDevices);

                if (currentNumberOfCreatedDevices % 100 == 0)
                {
                    Console.WriteLine($"status ('{iotHubName}'): {currentNumberOfCreatedDevices}");
                }

                try
                {
                    await ExecuteWithInfiniteRetry(() => registryManager.AddDeviceAsync(new Device("device-" + Guid.NewGuid())));
                }
                catch (DeviceAlreadyExistsException)
                {
                    // ignored
                }
            });

            createTheDevicesStopwatch.Stop();

            Console.WriteLine($"{DateTime.Now} iothub: '{iotHubName}'. Creation of {numberOfDevicesToCreate} devices took {createTheDevicesStopwatch.Elapsed.TotalSeconds:N0} seconds");

            static async Task ExecuteWithInfiniteRetry(Action action)
            {
                while (true)
                {
                    try
                    {
                        action();

                        break;
                    }
                    catch (ThrottlingException)
                    {
                        Console.WriteLine("throttling exception...");

                        await Task.Delay(1000 * 5).ConfigureAwait(false);

                        //continue;
                    }
                }
            }
        }
    }
}
