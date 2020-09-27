# autoscaling_Infrastructure_based_on_JVM

**1. The overview design of the system**

In this project, we have five types of VMs, that is the Master, Frond-Tier VMs, Middle-Tier VMs, the Cache and the Database.

• Master: master is used to manage the overall information of the total system. In this system, I only have one master, this master manages a queue, the front-tier VMs got requests and transfer the requests into the queue in master, at the same time, the middle-tier VMs got requests from the queue in master and process it. The front-tier VMs are like producer and middle-tier VMs are like consumer. The master checks the front-tier load and middle-tier load in a specific time to decide whether to scale out or scale in a specific type of VM in time. At the same time, the master is also acted as a kind of producer as it also adds the requests to the queue itself.

• Front-Tier VM: Front-Tier VM receives the requests and transfers it to the queue in master.

• Middle-Tier VM: Middle-Tier VM processes the requests. It also checks itself if it is idle in some time, if it is, it will shut down itself.

• Cache: Cache will check if a specific type of items required by a request is already processed and the result of which is in the cache. If it is, it will return the value from the cache directly, else it will access the database and cache the item and the value.

• Database: Database is initialized at the beginning of the simulation, and we need to fetch the items from the database if it is not in the cache.

For every specific time period, the master will check the front queue length and the middle queue length, divided them by the number of front machines and the number of middle machines. Thus, I could get the average queue length for the front end. The same for middle.

According the this we could decide whether to scale out or scale in the front-tier or middle-tier VMs. In addition, for middle-tier servers, every time it gets null request, it means it is idle. It will check the idle time at the same time to see how long it is. If it is too long, it will shut down itself, which makes it scale in more efficiently. For the master, in every loop, it will check the waiting period of each requests in the queue, if the time is too long, it will drop it so to make the later requests happy.

**2. Scaling Policy**

Middle Scale Out: I compare the total size of requests with total number of middle machines, I divide them and get the middle load, if it exceeds middle\_load\_max which is a parameter we can tune, then we scale out. Them number of VMs to scale out is dependent by a scale factor multiplied by the middle load, if the load is too heavier, I scale more otherwise I scale less.

Middle Scale In: There are two ways to scale in the middle, one is to check the number of requests in the queue is too low, if it is too low, I will scale in the middle. Another way to check is the idle time. Whenever a middle-tier VM get a null request, it means they are idle, so that they will check the idle time. If the idle time is too long, it will scale in itself.

Front Scale Out and Front Scale In: The total size of requests with total number of front machines are also compared as what I did in the middle, but for the front, I calculate the average number of requests per machine. I have two parameters that is the average number of max front requests and average number of min front requests. If the calculated result exceeds the average number of max front requests, I scale out. If the calculated result is less than the average number of min front requests, I scale in.

**3. The design of Cache**

I implemented the databaseOps object which implements the cache. When the simulation start, the master will initialize this object, and the object will get the database object to access later. When get requests come, it will check the requests and use cache to process it. For other requests, it simply transfer it to database part.

**4. Other detail design of the system**

In this system, I measured the beginning arrival rate of the requests, according to the beginning arrival rate, I used different configuration of parameters. This intelligent pattern match increased the efficiency of the total system.

Another design is that, I divided the requests into three kind of request since each request need different time to process. To avoid a certain kind of request congest the whole queue, I divided those requests into three queues, and when I fetches the request, I will fetch one certain kind of request from the three queues randomly. In this way, the system could tolerate some worst cases such as a long sequence of long-processing-time requests arriving at a short period.
