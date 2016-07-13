using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Logging;
using Rebus.Transport.Msmq;
#pragma warning disable 1998

namespace Rebus.Tests.Transport.Msmq
{
    [TestFixture]
    public class RealisticMsmqScenario : FixtureBase
    {
        BuiltinHandlerActivator _activator;

        protected override void SetUp()
        {
            _activator = Using(new BuiltinHandlerActivator());

            Configure.With(_activator)
                .Logging(l => l.Console(LogLevel.Warn))
                .Transport(t => t.UseMsmq(TestConfig.QueueName("realistic")))
                .Options(o =>
                {
                    o.SetNumberOfWorkers(10);
                    o.SetMaxParallelism(20);
                })
                .Start();
        }

        /*
Initial:

10000 messages received in 3,3 s - that's 2985,8 msg/s
10000 messages received in 3,6 s - that's 2749,0 msg/s
10000 messages received in 3,3 s - that's 3073,5 msg/s
10000 messages received in 3,3 s - that's 3016,6 msg/s
         
10000 messages received in 3,7 s - that's 2727,1 msg/s
Number of receive exceptions: 10
Number of empty messages: 0

10000 messages received in 3,5 s - that's 2826,8 msg/s
Number of receive exceptions: 10
Number of empty messages: 0

 
             */
        [TestCase(10000, 2)]
        public async Task NizzleName(int messageCount, int workerCount)
        {
            _activator.Bus.Advanced.Workers.SetNumberOfWorkers(0);

            Console.WriteLine($"Sending {messageCount} messages");

            var sendStopwatch = Stopwatch.StartNew();

            await Task.WhenAll(Enumerable.Range(0, messageCount)
                .Select(i => _activator.Bus.SendLocal($"THIS IS MESSAGE {i}")));

            var elapsedSending = sendStopwatch.Elapsed;

            Console.WriteLine($"{messageCount} messages sent in {elapsedSending.TotalSeconds:0.0} s - that's {messageCount / elapsedSending.TotalSeconds:0.0} msg/s");

            var receivedMessages = new ConcurrentDictionary<string, int>();
            var counter = new SharedCounter(messageCount);

            _activator.Handle<string>(async message =>
            {
                receivedMessages.AddOrUpdate(message, m => 1, (m, c) => c + 1);
                counter.Decrement();
            });

            Console.WriteLine("Receiving messages");

            var receiveStopwatch = Stopwatch.StartNew();

            _activator.Bus.Advanced.Workers.SetNumberOfWorkers(workerCount);
            counter.WaitForResetEvent(20);

            var elapsedReceiving = receiveStopwatch.Elapsed;

            Console.WriteLine($"{messageCount} messages received in {elapsedReceiving.TotalSeconds:0.0} s - that's {messageCount/elapsedReceiving.TotalSeconds:0.0} msg/s");

            Console.WriteLine($@"Number of receive exceptions: {MsmqTransport.NumberOfReceiveExceptions}
Number of empty messages: {MsmqTransport.NumberOfEmptyMessages}");

            var messagesReceivedMoreThanOnce = receivedMessages.Where(kvp => kvp.Value > 1).ToList();

            if (messagesReceivedMoreThanOnce.Any())
            {
                Assert.Fail($@"The following messages were received more than once:

{string.Join(Environment.NewLine, messagesReceivedMoreThanOnce.Select(kvp => kvp.Key))}
");
            }
        }
    }
}