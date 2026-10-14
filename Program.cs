using System.Data;
using LastHomework;
using System.Collections.Concurrent;

class OrderRun
{
    public int processedCount;
    public int totalSum;
    private volatile bool isRun;
    public object locker1 = new object();
    public object locker2 = new object();
    Semaphore semaphore = new Semaphore(2, 2);
    ConcurrentQueue<Order> queue = new ConcurrentQueue<Order>();

    public void Run()
    {
        var random = new Random();
        var orders = Enumerable.Range(1, 100)
        .Select(i => new Order
        {
            Id = i,
            Price = random.Next(100, 1000),
            IsProcessed = false
        })
        .ToList();

        foreach (var order in orders)
        {
            queue.Enqueue(order);
        }

        File.WriteAllText("results.txt", "");
        isRun = true;

        var threads = new List<Thread>();
        for (int i = 0; i < 5; i++)
        {
            var thread = new Thread(Worker);
            threads.Add(thread);
            thread.Start();
        }
        foreach (var t in threads)
        {
            t.Join();
        }

        isRun = false;
        ShowInfo(orders);
    }

    public void Worker()
    {
        while (queue.TryDequeue(out Order? order))
        {
            semaphore.WaitOne();
            try
            {
                Process(order);
            }
            finally
            {
                semaphore.Release();
            }
        }
    }

    public void ShowInfo(List<Order> orders)
    {
        Console.WriteLine($"Processed: {processedCount}");
        Console.WriteLine($"Orders: {orders.Count}");
        Console.WriteLine($"Paralel total sum: {totalSum}");
        Console.WriteLine($"Total sum: {orders.Sum(o => o.Price)}");
    }

    public void Process(Order order)
    {
        bool locker = false;
        try
        {
            Monitor.Enter(locker1, ref locker);
            if (order.IsProcessed)
            {
                return;
            }

            order.IsProcessed = true;
        }
        finally
        {
            if (locker)
            {
                Monitor.Exit(locker1);
            }
        }

        Interlocked.Increment(ref processedCount);
        Interlocked.Add(ref totalSum, order.Price);

        WriteResult(order);
    }

    public void WriteResult(Order order)
    {
        lock (locker2)
        {
            File.AppendAllText("results.txt", $"Order {order.Id}, Price {order.Price}, Thread {Thread.CurrentThread.ManagedThreadId}{Environment.NewLine}");
        }
    }
}
class Program
{
    static void Main()
    {
        OrderRun order = new OrderRun();
        order.Run();
    }
}
