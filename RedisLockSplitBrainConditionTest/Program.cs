using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RedLock;

namespace RedisLockSplitBrainConditionTest
{
    class Program
    {
        static void Main(string[] args)
        {
            startTest(1, 200, 100, GetLock, WriteLog);

            Console.ReadLine();
        }

        static void startTest(int count, int expire, int interval, Func<List<LockTestClass>, List<Task<RedisLock>>> getLock, Action<List<LockTestClass>> writeLog)
        {
            List<LockTestClass> lockClasses = new List<LockTestClass>();

            Parallel.For(0, count, c => lockClasses.Add(new LockTestClass(expire, interval)));
            Console.WriteLine("Init redis instance completed.");
            var tasks = getLock(lockClasses);

            Task.WhenAll(tasks).ContinueWith(t => {
                Console.WriteLine("lock test completed, begin to write log.");
                writeLog(lockClasses);
                Console.WriteLine("Write log completed.");
            });
        }

        static List<Task<RedisLock>> GetLock(List<LockTestClass> testClasses)
        {
            List<Task<RedisLock>> tasks = new List<Task<RedisLock>>();
            foreach (var testClass in testClasses)
            {
                var task = testClass.GetLockAsync();
                tasks.Add(task);
            }
            return tasks;
        }

        static void WriteLog(List<LockTestClass> testClasses)
        {
            foreach (var testClass in testClasses)
            {
                testClass.WriteLog();
            }
        }

    }

    class LockTestClass
    {
        string _resource;
        TimeSpan _expiry;
        int _interval;
        RedisLockFactory _redisLockFactory;
        RedisLock _redisLock;

        public LockTestClass(int expire, int interval)
        {
            var endPoints = new[]
            {
                     new DnsEndPoint("redis1", 9991),
                     new DnsEndPoint("redis2", 9992),
                     new DnsEndPoint("redis3", 9993),
                     new DnsEndPoint("redis3", 9994),
                     new DnsEndPoint("redis3", 9995)
                };
            _redisLockFactory = new RedisLockFactory(endPoints);

            _resource = "the-thing-we-are-locking-on";
            _expiry = TimeSpan.FromMilliseconds(expire);
            _interval = interval;
        }

        public async Task<RedisLock> GetLockAsync()
        {
            using (var redisLock = await _redisLockFactory.CreateAsync(_resource, _expiry))
            {
                _redisLock = redisLock;

                if (redisLock.IsAcquired)
                {
                    Thread.Sleep(_interval);
                }
                return redisLock;
            }
        }

        public void WriteLog()
        {
            if (!_redisLock.IsAcquired)
            {
                Console.WriteLine(Environment.NewLine);
                foreach (var keyValue in _redisLock.RedisKeyValues)
                {
                    Console.WriteLine($"key={keyValue.Item1}, value={keyValue.Item2}");
                }
                Console.WriteLine(Environment.NewLine);
            }
        }
    }
}
