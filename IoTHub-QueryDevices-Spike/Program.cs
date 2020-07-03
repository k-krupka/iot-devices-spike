using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Devices;
using Microsoft.Azure.Devices.Common.Exceptions;

namespace IoTHub_QueryDevices_Spike
{
    public class Program
    {
        public static async Task Main()
        {
            await CreateDeices("iothub-devices-100", 100);
            await CreateDeices("iothub-devices-1000", 1000);
            await CreateDeices("iothub-devices-10000", 10000);
            await CreateDeices("iothub-devices-100000", 100000);
            await CreateDeices("iothub-devices-1000000", 1000000);

            Console.ReadKey();
        }

        private static async Task CreateDeices(string iotHubName, int numberOfDevicesToCreate)
        {
            Console.WriteLine($"{DateTime.Now} Starting processing.");

            Stopwatch createTheDevicesStopwatch = new Stopwatch();
            createTheDevicesStopwatch.Start();

            RegistryManager registryManager = RegistryManager.CreateFromConnectionString(Settings.IotHubNameToConnectionStringDictionary[iotHubName]);

            var tasks = Enumerable.Range(0, numberOfDevicesToCreate).Select(async i =>
            {
                try
                {
                    await registryManager.AddDeviceAsync(new Device("device-" + i));
                }
                catch (DeviceAlreadyExistsException)
                {
                    // ignored
                }
            });
            await Task.WhenAll(tasks).ConfigureAwait(false);

            createTheDevicesStopwatch.Stop();

            Console.WriteLine($"{DateTime.Now} iothub: '{iotHubName}'. Creation of {numberOfDevicesToCreate} devices took {createTheDevicesStopwatch.Elapsed.TotalSeconds:N0} seconds");
        }
    }
}
