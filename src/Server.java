/*
 * Autoscanling Program Simulation and Tuning.
 * Author: Wenyan Liu
 */

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.rmi.NotBoundException;
import java.net.MalformedURLException;

public class Server implements VMS {

    private int role = -1; //master role
    public static Queue<Integer> fronts = new ArrayDeque<Integer>();
    public static Queue<Integer> middles = new ArrayDeque<Integer>();
    public static ConcurrentHashMap<Integer, Integer> vm_role = new ConcurrentHashMap<Integer, Integer>();

    public static ConcurrentHashMap<Integer, Long> questStartTime = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<Integer, Long> questFinishTime = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<Integer, Integer> frontQueueLen = new ConcurrentHashMap<>();
    public static BlockingQueue<Cloud.FrontEndOps.Request> reqQueue= new LinkedBlockingQueue<Cloud.FrontEndOps.Request>();
    public static BlockingQueue<Cloud.FrontEndOps.Request> initReqQueue= new LinkedBlockingQueue<Cloud.FrontEndOps.Request>();
    public static BlockingQueue<Cloud.FrontEndOps.Request> roReqQueue= new LinkedBlockingQueue<Cloud.FrontEndOps.Request>();

    // important parameters that may change the behavior
    public static int frontNum = 1;
    public static int middleNum = 1;
    public static float scale_factor = 3f;
    public static int drop_time_limit = 900;
    public static long idle_threshold = 2000;
    public static long time_measure_interval = 200;
    public static int low_queue_limit = 2;
    public static int low_queue_count_threshold = 10;
    public static float middle_queue_max_ratio = 1.5f;

    // this is a counter
    public static int low_queue_count = 0;

    public static long receivedRequestCounter = 0;
    public static int timeout_process = 50;
    public static int time_to_boot_vm = 5000;
    public static int port = 1099;
    public static int vm_id;
    public static ServerLib SL = null;
    public static Cloud.DatabaseOps cacheDB = null;

    public static long scale_in_delay = 1000 / time_measure_interval;
    public static final String TOPLEVEL = "TOPLEVEL";

    public static final float FRONT_QUEUE_AVG_MAX = 5f;
    public static final float FRONT_QUEUE_AVG_MIN = 2f;

    public static final float MIDDLE_LOAD_MAX = 0.95f;

    //DEBUG option, if set to true, all log function will print debug information.
    private static final Boolean DEBUG = false;

    private static final int FRONT_TIER = 2;
    private static final int MIDDLE_TIER = 3;
    private static final int MASTER_NODE = 1;
    private static int requestCount = 0;
    private static int front_scale_in_counter = 0;
    private static boolean cache_ready = false;

    public Server() {
    }

    /**
     * log function for debug.
     * if enabled by DEBUG parameter, the commandline will print log msg.
     * @param msg msg to print out
     */
    private static void log(String msg) {
        if (DEBUG) {
            long time = System.currentTimeMillis();
            System.err.println("[" + time + "]" + "<" + vm_id + ">" + msg);
        }
    }

    /**
     * Function to process request by middle tier servers.
     * Measuring the time of processing a single request, which is helpful for debugging.
     * @param r the request to process
     */
    private static void processRequestHelper(Cloud.FrontEndOps.Request r) {
         log("gotReqest RequestID " + r.id + "#");
         long start = System.currentTimeMillis();
        SL.processRequest(r, cacheDB);
         long end = System.currentTimeMillis();
         log("finished RequestID " + r.id + "# in " + (end - start));
    }

    /**
     * StartVM function.
     * start an VM, and will print debug information if DEBUG is set to true.
     * @return the vm_id
     */
    private static int startVMHelper() {
        int vm_id = SL.startVM();
        log("Started vm_boot " + vm_id);
        return vm_id;
    }

    /**
     * printSettings function.
     * if DEBUG enabled, the configuration will be print out.
     */
    private static void printSettings() {
        log("Config: frontNum:\t" + frontNum);
        log("Config: middleNum:\t" + middleNum);
        log("Config: scale_factor:\t" + scale_factor);
        log("Config: drop_time_limit:\t" + drop_time_limit);
        log("Config: idle_threshold:\t" + idle_threshold);
        log("Config: time_measure_interval:\t" + time_measure_interval);
        //log("Config: middle_load_min:\t" + middle_load_min);
        log("Config: low_queue_limit:\t" + low_queue_limit);
        log("Config: low_queue_count_threshold:\t" + low_queue_count_threshold);
        log("Config: middle_queue_max_rati:\t" + middle_queue_max_ratio);
    }


    /**
     * readSettingsFromEnv function.
     * if DEBUG enabled, could read parameters from environment, which is more convenient to tune the parameters.
     */
    private static void readSettingsFromEnv() {
        if (!DEBUG) {
            return;
        }

        String frontNumStr = System.getenv("CLOUD_FRONT_NUM");
        if (frontNumStr != null) {
            frontNum=Integer.parseInt(frontNumStr);
        }

        String middleNumStr = System.getenv("CLOUD_MIDDLE_NUM");
        if (middleNumStr != null) {
            middleNum=Integer.parseInt(middleNumStr);
        }

        String scale_factorStr = System.getenv("CLOUD_SCALE_FACTOR");
        if (scale_factorStr != null) {
            scale_factor=Float.parseFloat(scale_factorStr);
        }

        String drop_time_limitStr = System.getenv("CLOUD_DROP_TIME_LIMIT");
        if (drop_time_limitStr != null) {
            drop_time_limit=Integer.parseInt(drop_time_limitStr);
        }

        String idle_thresholdStr = System.getenv("CLOUD_IDLE_THRESHOLD");
        if (idle_thresholdStr != null) {
            idle_threshold=Integer.parseInt(idle_thresholdStr);
        }

        String time_measure_intervalStr = System.getenv("CLOUD_CHECK_INTERVAL");
        if (time_measure_intervalStr != null) {
            time_measure_interval=Integer.parseInt(time_measure_intervalStr);
        }

        String low_queue_limitStr = System.getenv("CLOUD_LOW_QUEUE_LIMIT");
        if (low_queue_limitStr != null) {
            low_queue_limit=Integer.parseInt(low_queue_limitStr);
        }

        String low_queue_count_thresholdStr = System.getenv("CLOUD_LOW_QUEUE_COUNT_THRESHOLD");
        if (low_queue_count_thresholdStr != null) {
            low_queue_count_threshold=Integer.parseInt(low_queue_count_thresholdStr);
        }

        String middle_queue_max_ratioStr = System.getenv("CLOUD_MIDDLE_QUEUE_MAX_RATIO");
        if (middle_queue_max_ratioStr != null) {
            middle_queue_max_ratio=Float.parseFloat(middle_queue_max_ratioStr);
        }
    }

    /**
     * Main function for the server.
     * The function check whether it is a master or not, if it is a master
     * it will initialize cache DB firstly, and then initialize its RMI i
     * nterface, then start a one front VM and one middle VM to have a try
     * later, it will get some information for arrival rate and do the initial
     * settings by callinng trySetup[arameters() function, and then do processMaster()
     * routine. If it is not master, it will first get its role from the master,
     * the role could be middle or front, then it will act as its role using the
     * corrosponding routine.
     * @param args command line arguments passed through
     * @throws Exception
     */
    public static void main ( String args[] ) throws Exception {
        if (args.length != 3) throw new Exception("Need 3 args: <cloud_ip> <cloud_port> <VM id>");
        port = Integer.parseInt(args[1]);
        vm_id = Integer.parseInt(args[2]);
        SL = new ServerLib(args[0], port);
        int myrole = -1;
        Server master_server = null;
        VMS master = null;
        Server slave_server = null;
        if (vm_id == MASTER_NODE) {
            cacheDB = new DB(args[0], args[1]);
            //only one master
            log("Config: initial ---");
            printSettings();
            try {
                master_server = new Server();
                master = (VMS) UnicastRemoteObject.exportObject(master_server, 0);
                // Bind the remote object's stub in the registry
                Registry registry = LocateRegistry.getRegistry(port);
                registry.bind("VMS", master);
                log("Master ready. Register the front-end service.");
            } catch (Exception e) {
                log("Master exception: " + e.toString());
                e.printStackTrace();
            }
            for (int i = 0; i < frontNum; i++) {
                log("Starting the initial front tier server");
                int new_id = startVMHelper();
                vm_role.put(new_id, FRONT_TIER);
                log("Starting the initial front tier server vm_id " + new_id);
            }
            for (int i = 0; i < middleNum; i++) {
                log("Starting the initial middle tier server");
                int new_id = startVMHelper();
                vm_role.put(new_id, MIDDLE_TIER);
                log("Starting the initial middle tier server vm_id " + new_id);
            }
            SL.register_frontend();
            log("Try setup the parameters.");
            trySetupParameters();

            log("Start processMaster");
            processMaster();
        } else {
            log("Starting slave ....");
            try {
                slave_server = new Server();
                VMS stub = (VMS) UnicastRemoteObject.exportObject(slave_server, 0);
                Registry registry = LocateRegistry.getRegistry(port);
                registry.bind("VMS_" + vm_id, stub);
                master = (VMS) Naming.lookup("//localHost:"+ port + "/VMS");
                log("Slave ready");
            } catch (Exception e) {
                log("Slave exception: " + e.toString());
                e.printStackTrace();
            }

            myrole = master.getRole(vm_id);
            while (myrole == -1) {
                myrole = master.getRole(vm_id);
            }
            master.registerVM(vm_id, myrole);
            if (myrole == FRONT_TIER) {
                SL.register_frontend();
                log("Finished vm_boot, start processFront");
                processFront(master);
            } else {
                log("Finished vm_boot, start processMiddle");
                processMiddle(master, vm_id);
                log("vm_stopped because of idling, done processMiddle");
            }
        }
    }


    /**
     * Master getRole function.
     * The slave VM could get role from master using their vm_id.
     * @param id vm_machine id
     * @return the role of the VM
     * @throws RemoteException
     */
    public int getRole(int id) throws RemoteException {
        return vm_role.getOrDefault(id, -1);
    }

    /**
     * registerVM function.
     * for slave to register itself into the corrosponding queues.
     * @param id VM_id
     * @param role the role the slave acts, could be FRONT_TIER or MIDDLE_TIER
     * @throws RemoteException
     */
    public void registerVM(int id, int role) throws RemoteException {
        if (role == FRONT_TIER) {
            fronts.offer(id);
        } else {
            middles.offer(id);
        }
    }

    /**
     * doneRequest function.
     * calculate the finished time when a request get finished and store it into the queue.
     * @param r master RMI object
     * @param middle_num initial middle tier number
     * @throws RemoteException
     * @throws InterruptedException
     */
    public void doneRequest(Cloud.FrontEndOps.Request r) throws RemoteException {
        long time=System.currentTimeMillis();
        questFinishTime.put(r.id, time);
        long startTme = questStartTime.get(r.id);
        log("RequestID " + r.id+ "# used time " + (time-startTme));
    }

    /**
     * addRequest function.
     * add request into the corrosponding queue.
     * @param r the request to process
     * @throws RemoteException
     */
    public void addRequest(Cloud.FrontEndOps.Request r) throws RemoteException {
        addRequest_helper(r);
    }

    /**
     * addRequest_helper function.
     * classificate requests and deliever those request into different queues.
     * put the start time of the request into the startTime queue.
     * @param r requests to process
     */
    private static void addRequest_helper(Cloud.FrontEndOps.Request r) {
        log("Adding RequestID " + r.id + "# " + r.toString() + " [" + r.item + "]");
        if (TOPLEVEL.equals(r.item)) {
            initReqQueue.offer(r);
        } else {
            if (r.isPurchase) {
                reqQueue.offer(r);
            } else {
                roReqQueue.offer(r);
            }
        }
        questStartTime.put(r.id, System.currentTimeMillis());
        receivedRequestCounter++;
    }

    /**
     * setQueueLength function.
     * record the queueLength and the croosponding vm_id into the frontQueueLen map.
     * @param vm_id the number of vm machine
     * @param newQueueLen the length of the queue
     * @throws RemoteException
     */
    public void setQueueLength(int vm_id, int newQueueLen)throws RemoteException {
        frontQueueLen.put(vm_id, newQueueLen);
    }

    /**
     * get request from one kind of queue to process to avoid to process a specific kind of requests in a fixed amount of time.
     * if got empty request, or the request wait too long in the queue, then drop the request, then try to get the request again
     * @return r the request get from the queue
     * @throws RemoteException
     */
    public Cloud.FrontEndOps.Request getRequest() throws RemoteException {
        Cloud.FrontEndOps.Request r = null;
        Boolean tryAgain = true;

        while (tryAgain) {
            int rand = new Random().nextInt(10);
            try {
                if (rand < 4 && reqQueue.size() > 0) {
                    r = reqQueue.poll(timeout_process, TimeUnit.MILLISECONDS);
                } else if (rand < 8 && roReqQueue.size() > 0) {
                    r = roReqQueue.poll(timeout_process, TimeUnit.MILLISECONDS);
                } else {
                    r = initReqQueue.poll(timeout_process, TimeUnit.MILLISECONDS);
                }
                log("ReqQueue returned OK with r " + r);
            } catch (InterruptedException e) {
                log("ReqQueue interrupted. Will try again.");
                r = null;
                e.printStackTrace();
            }
            if (r != null) {
                log("ReqQueue/InitReqQueue got request r " + r);
                long startTime = questStartTime.get(r.id);
                long time = System.currentTimeMillis();
                if (time - startTime > drop_time_limit) {
                    log("RequestID " + r.id + "# dropped after waiting " + (time - startTime));
                    SL.drop(r);
                    r = null;
                    tryAgain = true;
                } else {
                    log("RequestID " + r.id + "# is OK to run after waiting " + (time - startTime));
                    tryAgain = false;
                }
            } else {
                log("getRequest() call got empty request");
                tryAgain = false;
            }
        }
        return r;
    }

    /**
     * masterProcessFront function.
     * Act as a function to process the front requests to mitigate some load.
     */
    private static void masterProcessFront() {
        int newQueueLen = SL.getQueueLength();
        frontQueueLen.put(vm_id, newQueueLen);
        log("processFront master getQueueLength: "  + newQueueLen);
        log("processFront master getNextRequest (counter on current server):"  + requestCount++);
        Cloud.FrontEndOps.Request r = SL.getNextRequest();
        log("processFront master gotReqest RequestID " + r.id + "#");
        addRequest_helper(r);
        log("processFront master addedRequest RequestID " + r.id+ "#");
    }

    /**
     * masterProcessQueue function.
     * check the request in all three queues, drop the requests that waiting too long in all three queues.
     */
    private static void masterProcessQueues() {
        Cloud.FrontEndOps.Request r = null;
        Boolean toCheck = true;
        while ( toCheck ) {
            r = reqQueue.peek();
            if ( r == null ) {
                toCheck = false;
            } else {
                long startTime = questStartTime.get(r.id);
                long time = System.currentTimeMillis();
                if (time - startTime > drop_time_limit) {
                    log("[check]RequestID " + r.id + "# dropped after waiting " + (time - startTime));
                    reqQueue.remove(r);
                    SL.drop(r);
                } else {
                    toCheck = false;
                }
            }
        }
        toCheck = true;
        while ( toCheck ) {
            r = initReqQueue.peek();
            if ( r == null ) {
                toCheck = false;
            } else {
                long startTime = questStartTime.get(r.id);
                long time = System.currentTimeMillis();
                if (time - startTime > drop_time_limit) {
                    log("[check]RequestID " + r.id + "# dropped after waiting " + (time - startTime));
                    initReqQueue.remove(r);
                    SL.drop(r);
                } else {
                    toCheck = false;
                }
            }
        }
        toCheck = true;
        while ( toCheck ) {
            r = roReqQueue.peek();
            if ( r == null ) {
                toCheck = false;
            } else {
                long startTime = questStartTime.get(r.id);
                long time = System.currentTimeMillis();
                if (time - startTime > drop_time_limit) {
                    log("[check]RequestID " + r.id + "# dropped after waiting " + (time - startTime));
                    roReqQueue.remove(r);
                    SL.drop(r);
                } else {
                    toCheck = false;
                }
            }
        }
    }

    /**
     * trySetupParameters function.
     * check the avg_arrival_time of the requests at the beginning and set up the initial configuration
     * according to this avg_arrival_time.
     * @return true
     */
    private static Boolean trySetupParameters() {
        long time_lapsed = 0;
        int check_count = 3;
        long last_ts = 0;
        for (int i = 0; i < check_count; i++) {
            Cloud.FrontEndOps.Request r = SL.getNextRequest();
            if (last_ts == 0) {
                last_ts = System.currentTimeMillis();
            } else {
                long current_ts = System.currentTimeMillis();
                time_lapsed += current_ts - last_ts;
                last_ts = current_ts;
            }
            log("processFront master gotReqest RequestID " + r.id + "#");
            addRequest_helper(r);
        }
        long avg_arrival_time = time_lapsed / (check_count - 1);
        log("The avg arrival time so far is " + avg_arrival_time);
        if ( avg_arrival_time <= 150) {
            if (middleNum < 5) {
                for (int i = 0; i < 5 - middleNum; i++) {
                    log("Starting the extra middle tier server");
                    int new_id = startVMHelper();
                    vm_role.put(new_id, MIDDLE_TIER);
                    log("Starting the extra middle tier server vm_id " + new_id);
                }
                middleNum = 5;
            }
            scale_factor = 3f;
            drop_time_limit = 900;
            idle_threshold = 2000;
            time_measure_interval = 200;
            low_queue_limit = 2;
            low_queue_count_threshold = 20;
            middle_queue_max_ratio = 1f;
        } else if (avg_arrival_time <= 500) {
            if (middleNum < 4) {
                for (int i = 0; i < 4 - middleNum; i++) {
                    log("Starting the extra middle tier server");
                    int new_id = startVMHelper();
                    vm_role.put(new_id, MIDDLE_TIER);
                    log("Starting the extra middle tier server vm_id " + new_id);
                }
                middleNum = 4;
            }
            scale_factor = 2.3f;
            drop_time_limit = 900;
            idle_threshold = 2000;
            time_measure_interval = 300;
            low_queue_limit = 1;
            low_queue_count_threshold = 10;
            middle_queue_max_ratio = 1.5f;
        } else {
            middleNum = 3;
            if (middleNum < 3) {
                for (int i = 0; i < 3 - middleNum; i++) {
                    log("Starting the extra middle tier server");
                    int new_id = startVMHelper();
                    vm_role.put(new_id, MIDDLE_TIER);
                    log("Starting the extra middle tier server vm_id " + new_id);
                }
                middleNum = 3;
            }
            scale_factor = 1.8f;
            drop_time_limit = 900;
            idle_threshold = 2000;
            time_measure_interval = 300;
            low_queue_limit = 1;
            low_queue_count_threshold = 7;
            middle_queue_max_ratio = 1.8f;
        }
        log("Config -- New settings have been changed");
        printSettings();
        return true;
    }

    /**
     * processMaster routine function.
     * for each time_measure_interval check the load, in other time, just process the front and check the queues.
     */
    private static void processMaster() {
        long first = System.currentTimeMillis();
        long lastReceivedRequestCounter = receivedRequestCounter;

        while (true) {
            masterProcessFront();
            masterProcessQueues();
            long second = System.currentTimeMillis();
            if (second - first > time_measure_interval) {
                long currentReceivedRequestCounter = receivedRequestCounter;
                long newRequestCount = currentReceivedRequestCounter - lastReceivedRequestCounter;
                check(newRequestCount);
                first = System.currentTimeMillis();
                lastReceivedRequestCounter = receivedRequestCounter;
            } else {
                log("No need to check capacity yet.");
            }
        }
    }

    /**
     * check function.
     * check if the load is too much for front tier servers and middle tier servers.
     * it check the average length of the queue, it calculate the total length of the request queue size
     * and the total length of the front queue size, then divided by the number of front-tier machines and
     * middle tier machines, respectively, which get the average number of the front queuesize and the average
     * number of middle queue size, and decide whether to scale in or scale out the corrosponding tiers according
     * to the ratio between the real average number of queuesize per machine and the referenced average number of
     * queuesize as a parameter.
     * @param newRequestCount the new arrived request during the time_measureing_time
     */
    private static void check(long newRequestCount) {
        int num_front = 1;
        int num_middle = 0;
        for (int key:vm_role.keySet()) {
            int role = vm_role.get(key);
            if (role == FRONT_TIER) {
                num_front++;
            } else {
                num_middle++;
            }
        }

        int totalFrontQueueLen = 0;
        for (Integer key : frontQueueLen.keySet()) {
            int len = frontQueueLen.get(key);
            log("vm_id: " + key + ", queue length: " + len);
            totalFrontQueueLen += len;
        }
        log("front queue length combined: " + totalFrontQueueLen);

        int requestQueueSize = reqQueue.size() + initReqQueue.size() + roReqQueue.size();

        float front_queue_avg = (float)totalFrontQueueLen / (float) num_front;

        log("[check]----------------------------------------");
        log("[check]newRequestCount:  \t" + newRequestCount);
        log("[check]front_queue_len:  \t" + totalFrontQueueLen);
        log("[check]middle_queue_len: \t" + requestQueueSize);
        log("[check]front_vm_count:   \t" + num_front);
        log("[check]middle_vm_count:  \t" + num_middle);
        log("[check]real middle vm:   \t" + middles.size());
        log("[check]front_queue_avg:  \t" + front_queue_avg);
        log("[check]----------------------------------------");

        if (num_front == fronts.size()) {
            if (front_queue_avg > FRONT_QUEUE_AVG_MAX) {
                log("[check decision][Scale-Front-out]front_queue_avg is bigger than " + FRONT_QUEUE_AVG_MAX);
                scaleOutFront();
                front_scale_in_counter = 0;
            } else {
                if (front_queue_avg < FRONT_QUEUE_AVG_MIN) {
                    if (front_scale_in_counter > scale_in_delay) {
                        log("[check decision][Scale-Front-in]front_queue_avg is less than " + FRONT_QUEUE_AVG_MIN);
                        scaleInFront();
                        front_scale_in_counter = 0;
                    } else {
                        front_scale_in_counter++;
                    }
                } else {
                    front_scale_in_counter++;
                }
            }
        }
        if (num_middle == middles.size()) {
            if (requestQueueSize >= num_middle * middle_queue_max_ratio) {
                log("[check decision][Scale-Middle-Out]middle_queue_len is more than " + middle_queue_max_ratio
                    + " times middle_vm " + num_middle);
                scaleOutMiddle((float)requestQueueSize/(float)num_middle);
            } else {
                if (requestQueueSize <= low_queue_limit) {
                    low_queue_count++;
                    if (low_queue_count >= low_queue_count_threshold) {
                        log("[check decision][Scale-Middle-in]requestQueueSize is less than low_queue_limit "
                                + low_queue_limit + " for " + low_queue_count_threshold + "times.");
                        scaleInMiddle();
                        low_queue_count=0;
                    } // else need to wait a bit
                } else {
                    // reset the counter
                    low_queue_count=0;
                }
            }
        }
    }

    /**
     * the function to scaleInFront.
     * get an id from the front queue and shutdown that machine to scaleIn front.
     */
    private static void scaleInFront() {
        if (fronts.size() > 1) {
        	log("[check]scale In Front!");
            int id = fronts.poll();
            VMS to_remove;
            try {
                to_remove = (VMS) Naming.lookup("//localHost:" + port + "/VMS_" + id);
                // TODO: unregister the VM from the pool and stop later
                to_remove.stop();
            } catch (RemoteException | NotBoundException | MalformedURLException e) {
                e.printStackTrace();
            }
            vm_role.remove(id);
            SL.endVM(id);
            log("[check]scale In Front finished.");
        } else {
            log("[check]Last front sever.");
        }
    }

    /**
     * scaleOutFront function.
     * scale out the front by starting a new VM machine.
     */
    private static void scaleOutFront() {
        log("[check]scale Out Front!");
        int vm_id = startVMHelper();
        vm_role.put(vm_id, FRONT_TIER);
    }

    /**
     * checkMiddleLoad function.
     * Scale in Middle Server function for master, but need to check if the middle still exists.
     * @param id the id of the Middle VM to shutdown
     * @throws RemoteException
     */
    public void checkMiddleLoad(int id) throws RemoteException {
        boolean toRemvoe = false;
        synchronized (middles) {
            if (middles.size() > 1) {
                middles.remove(id);
                toRemvoe = true;
            }
        }
        if (!toRemvoe) {
            log("[check]vm_id " + id + " is the last middle machine.");
            return;
        }
        VMS to_remove;
        try {
            to_remove = (VMS) Naming.lookup("//localHost:" + port + "/VMS_" + id);
            // TODO: unregister the VM from the pool and stop later when it finishes the request
            to_remove.stop();
        } catch (RemoteException | NotBoundException | MalformedURLException e) {
            e.printStackTrace();
        }
        vm_role.remove(id);
        SL.endVM(id);
    }

    /**
     * scaleInMiddle function.
     * to scaleIn a middle from the queue.
     */
    private static void scaleInMiddle() {
        if (middles.size() > 1) {
        	log("[check]scale in middle!");
            int id = middles.poll();
            log("[check]scale in middle! : " + middles.size());
            VMS to_remove;
            try {
                to_remove = (VMS) Naming.lookup("//localHost:" + port + "/VMS_" + id);
                // TODO: unregister the VM from the pool and stop later when it finishes the request
                to_remove.stop();
            } catch (RemoteException | NotBoundException | MalformedURLException e) {
                e.printStackTrace();
            }
            vm_role.remove(id);
            SL.endVM(id);
            log("[check]scale in middle finished.");
        }  else {
            log("[check]Last middle sever.");
        }
    }

    /**
     * scaleOutMiddle function.
     * to scale out the middle according to the load parameter and scale_factor.
     * fixed the number of scaled middles to 5 at one time.
     * @param middle_load the middle_load calculated
     */
    private static void scaleOutMiddle(float middle_load) {
        log("[check]scale Out Middle!");
        int currentSize = middles.size();
        int toScale = (int)Math.ceil(scale_factor*middle_load);
        if (toScale > 5) {
            toScale = 5; // fix the scale up to 5 VMs most
        }
        log("[check]Scale Out Middle: " + currentSize + " with " + toScale + " new hosts.");
        for (int i=0; i < toScale; i++) {
            int id = startVMHelper();
            vm_role.put(id, MIDDLE_TIER);
        }
        log("[check]Scale Out Middle finished.");
    }

    /**
     * processFront function.
     * to process the Front machines.
     * @param master master RMI object
     * @throws RemoteException
     */
    private static void processFront(VMS master) throws RemoteException {
        cacheDB = master.getCacheDB();
        while (true) {
            int newQueueLen = SL.getQueueLength();
            master.setQueueLength(vm_id, newQueueLen);
            log("processFront getQueueLength: "  + newQueueLen);
            log("processFront getNextRequest (counter on current server): "  + requestCount++);
            Cloud.FrontEndOps.Request r = SL.getNextRequest();
            log("processFront gotReqest RequestID " + r.id + "#");
            master.addRequest(r);
            log("processFront addedRequest RequestID " + r.id + "#");
        }
    }

    /**
     * getCacheDB function.
     * to get the database interactive with cache object.
     * @return databaseops object
     * @throws RemoteException
     */
    public Cloud.DatabaseOps getCacheDB() throws RemoteException {
        return cacheDB;
    }

    /**
     * processMiddle function.
     * process the request, if didnot get the request, then measure the idle time,
     * if idle too long, check and close the VM.
     * @param master master object
     * @param vm_id middle tier vm number
     * @throws RemoteException
     */
    private static void processMiddle(VMS master, int vm_id) throws RemoteException {
        // main loop
        long lastProcessTime = System.currentTimeMillis();
        cacheDB = master.getCacheDB();
        while (true) {
            log("processMiddle getNextRequest (counter on current server): " + requestCount++);
            Cloud.FrontEndOps.Request r = master.getRequest();
            if (r != null) {
                processRequestHelper(r);
                master.doneRequest(r);
                lastProcessTime = System.currentTimeMillis();
            } else {
                log("processMiddle got emety request.");
                long currentTime = System.currentTimeMillis();
                if (currentTime - lastProcessTime > idle_threshold) {
                    log("Idle server found.");
                    master.checkMiddleLoad(vm_id);
                }
            }
        }
    }

    /**
     * stop function.
     * the function to stop clean.
     * @throws RemoteException
     */
    public void stop() throws RemoteException {
    	log("stop!");
        SL.unregister_frontend();
        UnicastRemoteObject.unexportObject(this, true);
    }
}


