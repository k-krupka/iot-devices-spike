using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Devices;
using Microsoft.Azure.Devices.Common.Exceptions;

namespace IoTHub_QueryDevices_Spike
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            // string option = string.Empty;
            //
            // if (args.Length == 0)
            // {
            //     option = "10000";
            //     Console.WriteLine("using value set from code");
            // }
            // else
            // {
            //     Console.WriteLine("using value set from args");
            // }
            //
            // switch (option)
            // {
            //     case "100":
            //         await CreateDeices("iothub-devices-100", 100);
            //         break;
            //     case "1000":
            //         await CreateDeices("iothub-devices-1000", 1000);
            //         break;
            //     case "10000":
            //         await CreateDeices("iothub-devices-10000", 10000);
            //         break;
            //     case "100000":
            //         await CreateDeices("iothub-devices-100000", 100000);
            //         break;
            //     case "1000000":
            //         await CreateDeices("iothub-devices-1000000", 1000000);
            //         break;
            //
            //     default:
            //         throw new Exception("unknown option");
            // }

            CreateDeices("iothub-devices-100", 100);
            CreateDeices("iothub-devices-1000", 1000);
            CreateDeices("iothub-devices-10000", 10000);
            CreateDeices("iothub-devices-100000", 100000);
            CreateDeices("iothub-devices-1000000", 1000000);

            Console.WriteLine("press any key to continue...");
            Console.ReadKey();
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
                    await ExecuteWithInfiniteRetry(() => registryManager.AddDeviceAsync(new Device("device-" + Guid.NewGuid().ToString())));
                    // await registryManager.AddDeviceAsync(new Device("device-" + i));
                }
                catch (DeviceAlreadyExistsException)
                {
                    // ignored
                }
            });

            createTheDevicesStopwatch.Stop();

            Console.WriteLine($"{DateTime.Now} iothub: '{iotHubName}'. Creation of {numberOfDevicesToCreate} devices took {createTheDevicesStopwatch.Elapsed.TotalSeconds:N0} seconds");
        }

        private static async Task ExecuteWithInfiniteRetry(Action action)
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
