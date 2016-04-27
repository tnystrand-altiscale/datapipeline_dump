import org.apache.spark.api.java.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.hive.orc.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.hive.*;
import org.apache.spark.rdd.*;
import org.apache.spark.api.java.function.*;
import java.sql.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import scala.Tuple2;
import java.util.Iterator;
import java.util.Set;
import java.util.Collections;
import java.util.Comparator;

public class BucketJobCategories { 
  public static void main(String[] args) {
    class ReduceKey implements java.io.Serializable{
        private final int x;
        private final String y;

        public ReduceKey(int x,String y) {
            this.x = x;
            this.y = y;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ReduceKey)) return false;
                ReduceKey key = (ReduceKey) o;
            return x == key.x && y.equals(key.y);
        }
        
        @Override
        public int hashCode() {
            int hash=1;
            hash=hash+x+y.hashCode();
            return hash;
        }
    }

    SparkConf conf = new SparkConf().setAppName("robbed_jobs_report");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    int debugPrint = 0;
    if (args.length>3) 
        debugPrint = Integer.parseInt(args[3]);

    System.out.println("Loading data from hive tables "+args[0]+" and "+args[1]+" into RDD's...");
    HiveContext hc = new org.apache.spark.sql.hive.HiveContext(jsc.sc());

    // Select modified container_time_series and cluster states
    DataFrame df_stt = hc.sql("SELECT * FROM "+args[0]);
    DataFrame df_cts = hc.sql("SELECT * FROM "+args[1]);


    JavaRDD<Row> rdd_cts = df_cts.javaRDD();
    JavaRDD<Row> rdd_stt = df_stt.javaRDD();
    System.out.println("...[DONE]");

    // Predefine Pairfunction which is used to extract minute_start as keys
    class PairMinRow implements PairFunction<Row,ReduceKey,Row> {
        public Tuple2<ReduceKey,Row> call(Row row) {
            ReduceKey key = new ReduceKey(row.getInt(row.fieldIndex("minute_start")),
                                          row.getString(row.fieldIndex("system")));
            return new Tuple2(key, row);
        }
    }

    System.out.print("Mapping hive tables to key-value pair RDD's...");
    JavaPairRDD<ReduceKey, Row> minuteStartSeriesKeyValue = rdd_cts.mapToPair(new PairMinRow());
    JavaPairRDD<ReduceKey, Row> minuteStartStateKeyValue = rdd_stt.mapToPair(new PairMinRow());
    System.out.println("[DONE]");

    // Group same minute_start from the different RDD's into a new RDD
    System.out.print("Combine request_assign_realease and state_perminute_job into one RDD...");
    JavaPairRDD<ReduceKey, Tuple2<Iterable<Row>, Iterable<Row>>> coGroupedContainers = 
        minuteStartSeriesKeyValue.cogroup(minuteStartStateKeyValue);
    System.out.println("[DONE]");


    if (debugPrint>0) {
        Map<ReduceKey,Tuple2<Iterable<Row>, Iterable<Row>>> localResults1 = coGroupedContainers.collectAsMap();
        for(Map.Entry<ReduceKey,Tuple2<Iterable<Row>, Iterable<Row>>> entry : localResults1.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }

    // Map each minute_start to output wait categories for each job
    // job_id is output key
    System.out.print("Compute the wait categories...");
    JavaPairRDD<String, Row> jobWaitCategories = coGroupedContainers.flatMapToPair(
        new PairFlatMapFunction<Tuple2<ReduceKey,Tuple2<Iterable<Row>, Iterable<Row>>>, String, Row> () {

        // Pointer-class. Just a wrapper.
        class MemoryPointer{
            private long memory;
            private long vcores;
            private long number;
            //private int vcores;
            public MemoryPointer(long memory, long vcores, long number) {
                this.memory = memory;
                this.vcores = vcores;
                this.number = number;
            }
        }

        // Organize the wait categories
        // These are calculated in MB-seconds
        class WaitReasons{
            private double waitOnQueueLim;
            private double waitOnOtherUser;
            private double multiWaitOnQueuLim;
            private double waitOnUserLim;
            private double waitOnElasticUnfair;
            private double waitOnCompeteJob;

            public WaitReasons() {
                waitOnQueueLim = 0;
                multiWaitOnQueuLim = 0;
                waitOnUserLim = 0;
                waitOnElasticUnfair = 0;
                waitOnCompeteJob = 0;
            }
        }

        class compareRows implements Comparator {

            public int compare(Object o1, Object o2) {
                Row r1 = (Row) o1;
                Row r2 = (Row) o2;
                long t1 = r1.getLong(r1.fieldIndex("timestamp"));
                long t2 = r2.getLong(r2.fieldIndex("timestamp"));
                int a1 = r1.getInt(r1.fieldIndex("action"));
                int a2 = r2.getInt(r2.fieldIndex("action"));

                if(t1==t2) 
                    return Integer.compare(a1,a2);
                else
                    return Long.compare(t1,t2);
                
            }
        }

        class Key {
            private final String x;
            private final String y;

            public Key(String x,String y) {
                this.x = x;
                this.y = y;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (!(o instanceof Key)) return false;
                    Key key = (Key) o;
                return x.equals(key.x) && y.equals(key.y);
            }
            
            @Override
            public int hashCode() {
                int hash=1;
                hash=hash+x.hashCode()+y.hashCode();
                return hash;
            }
        }

        // Class to describe current job status
        // This status is updated as progressing through minute
        // and containers are requested/assigned/released
        class JobStatus{
            private String queue_name; 
            private String user_name; 
            private int maximumRequestedContainerSize;
            private long last_transition_time;
            private double unaggWaitTime;
            private double fullClusterTime;
            private double slothour;

            private long job_req_mem;
            private long job_run_mem;
            private MemoryPointer que_run;
            private MemoryPointer usr_run;
            private MemoryPointer sys_run;

            private long job_req_vcr;
            private long job_run_vcr;

            private long job_req_num;
            private long job_run_num;

            private double REG_MEM_CAP;
            private double MAX_MEM_CAP;
            private double SYS_MEM;
            private double REG_VCR_CAP;
            private double MAX_VCR_CAP;
            private double SYS_VCR;

            private double waitTotalMem;
            private WaitReasons memWaitReasons;
            private WaitReasons vcrWaitReasons;

            private HashMap<String,Double> otherUserWaits; 
            private HashMap<String,Double> otherQueueWaits;

            long max_waiting_containers;
            long max_running_containers;
            double sum_waiting_containers;
            long max_waiting_memory;
            long max_running_memory;
            double sum_waiting_memory;
            long max_waiting_vcores;
            long max_running_vcores;
            double sum_waiting_vcores;

            int num_measures;


            public JobStatus(
                            String queue_name,
                            String user_name,

                            long last_transition_time,

                            long job_req_mem,
                            long job_run_mem,

                            long job_req_vcr,
                            long job_run_vcr,

                            long job_req_num,
                            long job_run_num,

                            MemoryPointer que_run,
                            MemoryPointer usr_run,
                            MemoryPointer sys_run,

                            double memory_capacity, 
                            double memory_max_capacity,
                            double cluster_memory_capacity,
                            double vcore_capacity,
                            double vcore_max_capacity,     
                            double cluster_vcore_capacity,

                            HashMap<String,Double> otherUserWaits,
                            HashMap<String,Double> otherQueueWaits
                            ) {
                // The 'assumed' last biggest requested container memory for this job.
                // Assuming is inaccurate, but will only affect one container each time interval
                // This should be used for the maximum capacity wait category if being 'exact'
                // and should be the maximum of waiting container sizes (assuming no priority)
                maximumRequestedContainerSize = 5000; 
                waitTotalMem = 0;
                fullClusterTime = 0;
                unaggWaitTime = 0;
                slothour = 0;
                this.memWaitReasons = new WaitReasons();
                this.vcrWaitReasons = new WaitReasons();

                this.queue_name = queue_name;
                this.user_name = user_name;

                this.last_transition_time = last_transition_time;

                this.job_req_mem = job_req_mem;
                this.job_run_mem = job_run_mem;

                this.job_req_vcr = job_req_vcr;
                this.job_run_vcr = job_run_vcr;

                this.job_req_num = job_req_num;
                this.job_run_num = job_run_num;

                this.que_run = que_run;
                this.usr_run = usr_run;
                this.sys_run = sys_run;

                this.otherUserWaits = new HashMap(otherUserWaits);
                this.otherQueueWaits = new HashMap(otherQueueWaits);

                this.otherUserWaits.put("-",new Double(0));
                this.otherQueueWaits.put("-",new Double(0));

                REG_MEM_CAP     = memory_capacity;
                MAX_MEM_CAP     = memory_max_capacity;
                SYS_MEM         = cluster_memory_capacity;
                REG_VCR_CAP     = vcore_capacity;
                MAX_VCR_CAP     = vcore_max_capacity;
                SYS_VCR         = cluster_vcore_capacity;

                max_waiting_containers = 0;
                max_running_containers = 0;
                sum_waiting_containers = 0;
                max_waiting_memory = 0;
                max_running_memory = 0;
                sum_waiting_memory = 0;
                max_waiting_vcores = 0;
                max_running_vcores = 0;
                sum_waiting_vcores = 0;

                num_measures = 0;
            }

            public void updateInstantMeasures() {
                
                max_waiting_containers = Math.max(max_waiting_containers,job_req_num);
                max_running_containers = Math.max(max_running_containers,job_run_num);
                sum_waiting_containers += job_req_num;
                max_waiting_memory = Math.max(max_waiting_memory,job_req_mem);
                max_running_memory = Math.max(max_running_memory,job_run_mem);
                sum_waiting_memory += job_req_mem;
                max_waiting_vcores = Math.max(max_waiting_vcores,job_req_vcr);
                max_running_vcores = Math.max(max_running_vcores,job_run_vcr);
                sum_waiting_vcores += job_req_vcr;

                num_measures ++;
            }

            // Function to calculate wait categories at each moment
            public void updateWaits(double memory_waittime,
                                    double deltatime,
                                    Set<Map.Entry<Key, MemoryPointer>> usrSet,
                                    Set<Map.Entry<String,MemoryPointer>> queSet) {
                int activeUsers = 0;

                String otherUserName = "-";
                long otherUserMem = 0;

                slothour=slothour+job_run_mem*deltatime;

                // User limits check
                // Test to see if there are other active users in the same queue
                for(Map.Entry<Key, MemoryPointer> user : usrSet) {
                    long memory = user.getValue().memory;
                    String name = user.getKey().x;
                    String qname = user.getKey().y;
                    // NOTE: testing for same queue here. Forgetting this will
                    // result in that users in other queues will also be taken into account
                    // which we do not want (that is handled by otherQueueWaits)
                    // No Need to check for system here since
                    // Only one system is procssed at a time
                    if (memory>0 && qname.equals(queue_name) && !name.equals(user_name)) {
                        activeUsers++;
                        if (memory>otherUserMem) {
                            otherUserMem=memory;
                            otherUserName=name;
                        }
                    }
                }


                // Similar but for queues
                String otherQueueName = "-";
                long otherQueueMem = 0;
                for(Map.Entry<String, MemoryPointer> queue : queSet) {
                    long memory = queue.getValue().memory;
                    String name = queue.getKey();
                    if (memory>otherQueueMem && !name.equals(queue_name)) {
                        otherQueueMem=memory;
                        otherQueueName=name;
                    }
                }

                // These are the same for vcores and memory
                otherQueueWaits.put(otherQueueName,otherQueueWaits.get(otherQueueName)+memory_waittime);
                otherUserWaits.put(otherUserName,otherUserWaits.get(otherUserName)+memory_waittime);

                // Calculate waits up till timestamp
                // Semi-arbitrary 0.1 to compensate for inaccuracies
                double memoryOffset = MAX_MEM_CAP*0.10;
                double vcoresOffset = MAX_VCR_CAP*0.10;
                waitTotalMem += memory_waittime;
                updateWaitHelper(memoryOffset,
                                memory_waittime,
                                job_run_mem,
                                usr_run.memory,
                                que_run.memory,
                                sys_run.memory,
                                MAX_MEM_CAP,
                                REG_MEM_CAP,
                                SYS_MEM,
                                memWaitReasons,
                                activeUsers);

                updateWaitHelper(vcoresOffset,
                                memory_waittime,
                                job_run_vcr,
                                usr_run.vcores,
                                que_run.vcores,
                                sys_run.vcores,
                                MAX_VCR_CAP,
                                REG_VCR_CAP,
                                SYS_VCR,
                                vcrWaitReasons,
                                activeUsers);
            }

            // Helper since vocres and memory are treated the same
            public void updateWaitHelper(double offset,
                                         double memwait,
                                         long jobRunning,
                                         long usrRunning,
                                         long queRunning,
                                         long sysRunning,
                                         double maxCap,
                                         double regCap,
                                         double sysMem,
                                         WaitReasons waitReasons,
                                         int activeusers
                                         ) {

                // Mutually exclusive which favours maxCaps
                // Consider changing this so elasticunfair and competing jobs get
                // more 'hits'
                if(jobRunning >= maxCap*0.9)
                    waitReasons.waitOnQueueLim += memwait;

                else if(jobRunning < maxCap*0.9
                        && queRunning >= maxCap*0.9) {
                    waitReasons.multiWaitOnQueuLim += memwait;
                    if (activeusers>1) {
                        waitReasons.waitOnUserLim += memwait;
                    }
                }

                else if(jobRunning < regCap
                        && sysRunning > sysMem*0.9
                        && queRunning < maxCap*0.9)
                    waitReasons.waitOnElasticUnfair += memwait;

                else if(jobRunning < maxCap*0.9
                        && jobRunning > regCap
                        && sysRunning > sysMem*0.9
                        && queRunning < maxCap*0.9)
                    waitReasons.waitOnCompeteJob += memwait;
            }

            public void updateUnaggWaitTime(double deltaTime) {
                if (job_req_mem>0)
                    unaggWaitTime+=deltaTime;
            }

            public void update90ClusterTime(double deltaTime) {
                if (sys_run.memory > SYS_MEM*0.9)
                    fullClusterTime+=deltaTime;
            }

            public void updateRequestedState(long memory,long vcores,long number) {
                job_req_mem += memory;
                job_req_vcr += vcores;
                job_req_num += number;
            }
                
            public void updateRunningState(long memory,long vcores,long number) {
                job_run_mem += memory;
                job_run_vcr += vcores;
                job_run_num += number;

                que_run.memory += memory;
                usr_run.memory += memory;
                sys_run.memory += memory;

                que_run.vcores += vcores;
                usr_run.vcores += vcores;
                sys_run.vcores += vcores;

                que_run.number += number;
                usr_run.number += number;
                sys_run.number += number;
            }
        }


        // Sequentially change the cluster state as containers get allocated and deallocated
        public Iterable<Tuple2<String,Row>> call(Tuple2<ReduceKey,Tuple2<Iterable<Row>, Iterable<Row>>> groupedContainersTuple) {
            // Dechipher the input
            int minute_start = groupedContainersTuple._1().x;
            long minute_start_ms = ((long) minute_start)*1000;
            long minute_end_ms = ((long) minute_start)*1000+60*1000;

            Tuple2<Iterable<Row>,Iterable<Row>> groupedContainers = groupedContainersTuple._2();
            Iterable<Row> serie = groupedContainers._1();
            Iterable<Row> state = groupedContainers._2();

            // All maps to contain pointers so values are only updated once
            HashMap<String, JobStatus> job_map = new HashMap<String, JobStatus>();
            HashMap<String, MemoryPointer> cluster_map = new HashMap<String, MemoryPointer>();
            HashMap<Key, MemoryPointer> user_map = new HashMap<Key, MemoryPointer>();
            HashMap<String, MemoryPointer> queue_map = new HashMap<String, MemoryPointer>();

            HashMap<String, Double> user_map_empty = new HashMap<String, Double>();
            HashMap<String, Double> queue_map_empty = new HashMap<String, Double>();

            HashMap<String, String> jobMapInfo = new HashMap<String, String>();
            
            // Output list
            ArrayList jobCategorized = new ArrayList();

            Iterator<Row> it_state = state.iterator();
            Iterator<Row> it_serie = serie.iterator();

            String theDate = "-";
 
            // Check if state exists since on occasion round-off errors in container_time_series
            // can fail to prdouce states
            if(it_state.hasNext()) {
                Row row_st = it_state.next();

                // Pre-find the positions indecies of columns using the first row
                int job_id_id                = row_st.fieldIndex("job_id");
                int minute_start_id          = row_st.fieldIndex("minute_start");
                int system_id                = row_st.fieldIndex("system");
                int date_id                  = row_st.fieldIndex("date");
                int user_key_id              = row_st.fieldIndex("user_key");
                int measure_date_id          = row_st.fieldIndex("measure_date");
                int queue_id                 = row_st.fieldIndex("queue");
                int memory_req_job_id        = row_st.fieldIndex("memory_req_job");
                int memory_job_id            = row_st.fieldIndex("memory_job");
                int memory_user_id           = row_st.fieldIndex("memory_user");
                int memory_queue_id          = row_st.fieldIndex("memory_queue");
                int memory_cluster_id        = row_st.fieldIndex("memory_cluster");
                int vcores_req_job_id        = row_st.fieldIndex("vcores_req_job");
                int vcores_job_id            = row_st.fieldIndex("vcores_job");
                int vcores_user_id           = row_st.fieldIndex("vcores_user");
                int vcores_queue_id          = row_st.fieldIndex("vcores_queue");
                int vcores_cluster_id        = row_st.fieldIndex("vcores_cluster");
                int number_req_job_id        = row_st.fieldIndex("number_req_job");
                int number_job_id            = row_st.fieldIndex("number_job");
                int number_user_id           = row_st.fieldIndex("number_user");
                int number_queue_id          = row_st.fieldIndex("number_queue");
                int number_cluster_id        = row_st.fieldIndex("number_cluster");

                int memory_capacity_id          = row_st.fieldIndex("memory_capacity");
                int memory_max_capacity_id      = row_st.fieldIndex("memory_max_capacity");
                int cluster_memory_capacity_id  = row_st.fieldIndex("cluster_memory_capacity");
                int vcore_capacity_id           = row_st.fieldIndex("vcore_capacity");
                int vcore_max_capacity_id       = row_st.fieldIndex("vcore_max_capacity");
                int cluster_vcore_capacity_id   = row_st.fieldIndex("cluster_vcore_capacity");

                theDate = row_st.getString(measure_date_id);

                // Initialize hash tables for system, queues and users
                for(Row r : state) {
                    MemoryPointer sys = new MemoryPointer(r.getLong(memory_cluster_id),
                                                          r.getLong(vcores_cluster_id),
                                                          r.getLong(number_cluster_id));
                    MemoryPointer usr = new MemoryPointer(r.getLong(memory_user_id),
                                                          r.getLong(vcores_user_id),
                                                          r.getLong(number_user_id));
                    MemoryPointer que = new MemoryPointer(r.getLong(memory_queue_id),
                                                          r.getLong(vcores_queue_id),
                                                          r.getLong(number_queue_id));

                    cluster_map.put(r.getString(system_id), sys);
                    user_map.put(new Key(r.getString(user_key_id),r.getString(queue_id)), usr);
                    queue_map.put(r.getString(queue_id), que);

                    user_map_empty.put(r.getString(user_key_id), new Double(0));
                    queue_map_empty.put(r.getString(queue_id), new Double(0));
                    
                    jobMapInfo.put(r.getString(job_id_id),r.getString(system_id));
                }

                // Create job MemoryPointers and 'point' its 'pointers'
                for(Row r : state) {
                    long last_transition_time = minute_start_ms;
                    MemoryPointer que_run = queue_map.get(r.getString(queue_id));
                    MemoryPointer usr_run = user_map.get(new Key(r.getString(user_key_id),r.getString(queue_id)));
                    MemoryPointer sys_run = cluster_map.get(r.getString(system_id));
                    long job_run_mem = r.getLong(memory_job_id);
                    long job_req_mem = r.getLong(memory_req_job_id);
                    long job_run_vcr = r.getLong(vcores_job_id);
                    long job_req_vcr = r.getLong(vcores_req_job_id);
                    long job_run_num = r.getLong(number_job_id);
                    long job_req_num = r.getLong(number_req_job_id);

                    double memory_capacity          = r.getDouble(memory_capacity_id);
                    double memory_max_capacity      = r.getDouble(memory_max_capacity_id);
                    double cluster_memory_capacity  = r.getDouble(cluster_memory_capacity_id);
                    double vcore_capacity           = r.getDouble(vcore_capacity_id);
                    double vcore_max_capacity       = r.getDouble(vcore_max_capacity_id);
                    double cluster_vcore_capacity   = r.getDouble(cluster_vcore_capacity_id);

                    JobStatus tmp = new JobStatus(
                                r.getString(queue_id),
                                r.getString(user_key_id),

                                last_transition_time,
                                job_req_mem,
                                job_run_mem,
                                job_req_vcr,
                                job_run_vcr,
                                job_req_num,
                                job_run_num,

                                que_run,
                                usr_run,
                                sys_run,
                                memory_capacity, 
                                memory_max_capacity,
                                cluster_memory_capacity,
                                vcore_capacity,
                                vcore_max_capacity,     
                                cluster_vcore_capacity,

                                user_map_empty,
                                queue_map_empty
                                );

                    job_map.put(r.getString(job_id_id),tmp);
                }

                Set<Map.Entry<Key, MemoryPointer>> usrSet = user_map.entrySet();
                Set<Map.Entry<String, MemoryPointer>> queSet = queue_map.entrySet();

                ArrayList<Row> serieList = new ArrayList<Row>();
                for(Row r : serie) {
                    serieList.add(r);
                }
                Collections.sort(serieList,new compareRows());

                // Check if something haooened during this minute
                if(serieList.size()>0) {
                    // Pre-find the positions indecies of columns using the first row
                    Row row_se = it_serie.next();
                    int timestamp_id = row_se.fieldIndex("timestamp");
                    int jobid_id =     row_se.fieldIndex("jobid");
                    int memory_id =    row_se.fieldIndex("memory");
                    int vcores_id =    row_se.fieldIndex("vcores");
                    int number_id =    row_se.fieldIndex("numgroup");
                    int action_id =    row_se.fieldIndex("action");
                    system_id =    row_se.fieldIndex("system");

                    // For each event in series (assign/request/release), update the current state and calculate
                    // wait categories
                    for(Row r : serieList) {
                        String jobid = r.getString(jobid_id);
                        long timestamp = r.getLong(timestamp_id);
                        int action = r.getInt(action_id);

                        JobStatus tmp = job_map.get(jobid);
                        if (tmp!=null) {

                            int memory = (int) r.getLong(memory_id);
                            int vcores = (int) r.getLong(vcores_id);
                            int number = (int) r.getLong(number_id);

                            double deltatime = (timestamp-tmp.last_transition_time)/1000.0;
                            double memory_waittime = deltatime*tmp.job_req_mem;

                            // job_req_mem should be zero for jobs which have no requested
                            // containers in the beginning of the minute

                            // Wait categories are attributed to last event
                            tmp.updateWaits(memory_waittime,deltatime,usrSet,queSet);
                            
                            // If job has waiting containers, update unaggregated wait time
                            tmp.updateUnaggWaitTime(deltatime);
                            tmp.update90ClusterTime(deltatime);

                            // Update the state of the cluster
                            // Some if statements are added since the data from container_time_series can give
                            // initial states which are not exact and memory can never be negative
                            // These actions assume every container have a requestedtime if allocated
                            switch (action) {
                                case 0: // All requestes adds to the requested state
                                    tmp.updateRequestedState(memory,vcores,number);
                                    tmp.maximumRequestedContainerSize = Math.max(memory,tmp.maximumRequestedContainerSize);
                                    break;
                                case 1: // All transitions from requested to allocated adds to running state and subtracts from requested
                                    if (tmp.job_req_mem-memory>=0) {
                                        tmp.updateRequestedState(-memory,-vcores,-number);
                                        tmp.maximumRequestedContainerSize = Math.max(memory,tmp.maximumRequestedContainerSize);
                                    }

                                    tmp.updateRunningState(memory,vcores,number);
                                    break;
                                case 2: // All running containers that are completed/killed/released/expired decrease running state
                                    tmp.updateRunningState(-memory,-vcores,-number);
                                    break;
                                case 3: // All reservations are counted as adding to running state of this job (system mem should tehcnically be omitted here)
                                    tmp.updateRunningState(memory,vcores,number);
                                    break;
                                case 4: // A reserved container that transitions to allocated state is already counted as running
                                        // However this container is also a requested container, which adds to the running state in action 1
                                        // To balance this update (+/-0), remove the correspodning memory from the running state 
                                    tmp.updateRequestedState(-memory,-vcores,-number);
                                    break;
                                case 5: // Reserved container that transitions directly to completed/exired/killed/released without allocation (independent of request)
                                    tmp.updateRunningState(-memory,-vcores,-number); 
                                    break;
                                case 6: // Requested containers that transitions directly to completed/expired/killed/released without allocation (independent of reservation)
                                    tmp.updateRequestedState(-memory,-vcores,-number); 
                                    break;
                                default:
                                    throw new RuntimeException("Unrecognized action: "+action+"\n"+
                                                                "0: request is made\n"+
                                                                "1: requested container is allocated\n"+
                                                                "2: running container is removed\n"+
                                                                "3: container is reserved\n"+
                                                                "4: reserved container is allocated\n"+
                                                                "5: a non allocated reserved container is removed\n"+
                                                                "6: a non allocated requested container is removed");
                            }

                            // Update instantaneous measurements after updating the state
                            // since first step is already calculated
                            tmp.updateInstantMeasures();
                            // Update last event time
                            tmp.last_transition_time = timestamp;
                        }
                        else {
                            // This should not happen....There is a job in the series
                            // but not in the state. Can happen if capacity_combined has missing values for instsance
                            //throw new RuntimeException("Missing state from series");
                        }
                    }
                }

                // Move every job to end of minute and add jobs ouput list 
                for(Map.Entry<String, JobStatus> job_entry : job_map.entrySet()) {
                    JobStatus tmp = job_entry.getValue();

                    int num_measures = tmp.num_measures;
                    double avg_waiting_containers = tmp.job_req_num;
                    double avg_waiting_memory = tmp.job_req_mem;
                    double avg_waiting_vcores = tmp.job_req_vcr;
                    if (num_measures>0) {
                        avg_waiting_containers = tmp.sum_waiting_containers/num_measures;
                        avg_waiting_memory = tmp.sum_waiting_memory/num_measures;
                        avg_waiting_vcores = tmp.sum_waiting_vcores/num_measures;
                    }

                    double deltatime = (minute_end_ms-tmp.last_transition_time)/1000.0;
                    double memory_waittime = deltatime*tmp.job_req_mem;
                    tmp.updateWaits(memory_waittime,deltatime,usrSet,queSet);
                    tmp.updateUnaggWaitTime(deltatime);
                    WaitReasons memReasons = tmp.memWaitReasons;
                    WaitReasons vcrReasons = tmp.vcrWaitReasons;


                    String otherUserName = "-";
                    double otherUserMem = 0.0;

                    // User limits check
                    for(Map.Entry<String, Double> user : tmp.otherUserWaits.entrySet()) {
                        double memory = user.getValue();
                        String name = user.getKey();
                        if (memory>0 && !name.equals("-")) {
                            if (memory>otherUserMem) {
                                otherUserMem=memory;
                                otherUserName=name;
                            }
                        }
                    }

                    // Similar but for queues
                    String otherQueueName = "-";
                    double otherQueueMem = 0.0;
                    for(Map.Entry<String, Double> queue : tmp.otherQueueWaits.entrySet()) {
                        double memory = queue.getValue();
                        String name = queue.getKey();
                        if (memory>otherQueueMem && !name.equals("-")) {
                            otherQueueMem=memory;
                            otherQueueName=name;
                        }
                    }


                    Row waitRow = RowFactory.create(
                        jobMapInfo.get(job_entry.getKey()),
                        memReasons.waitOnQueueLim,
                        memReasons.multiWaitOnQueuLim,
                        memReasons.waitOnUserLim,
                        memReasons.waitOnElasticUnfair,
                        memReasons.waitOnCompeteJob,
                        vcrReasons.waitOnQueueLim,
                        vcrReasons.multiWaitOnQueuLim,
                        vcrReasons.waitOnUserLim,
                        vcrReasons.waitOnElasticUnfair,
                        vcrReasons.waitOnCompeteJob,
                        tmp.waitTotalMem,
                        otherUserName,
                        otherUserMem,
                        otherQueueName,
                        otherQueueMem,
                        tmp.max_waiting_containers,
                        tmp.max_running_containers,
                        avg_waiting_containers,
                        tmp.max_waiting_memory,
                        tmp.max_running_memory,
                        avg_waiting_memory,
                        tmp.max_waiting_vcores,
                        tmp.max_running_vcores,
                        avg_waiting_vcores,
                        1, // for averages
                        theDate,
                        tmp.unaggWaitTime,
                        tmp.fullClusterTime,
                        tmp.slothour
                        );
                
                    // Add to output list
                    jobCategorized.add(new Tuple2(job_entry.getKey(),waitRow));
                }
            }
            // Ignore for now.
            // Only time program ends up here is if there has been an assing/release/request
            // during the current time interval but no state has been recorded.
            // This happens if for instance there is a release a few milliseconds after the
            // minute_start. In this caes the underlaying container_time_series might have rounded 'away'
            // the corresponding 'RUNNING' state.

            return jobCategorized;
        }
        }
    );
    System.out.println("[DONE]");


    System.out.print("Summing contributions from different minutes to each job...");
    JavaPairRDD<String,Row> summedJobWaits = jobWaitCategories.reduceByKey(
        new Function2<Row,Row,Row>() {
        public Row call(Row r1,Row r2) {
           
            String user;
            double user_mem;
            if (r1.getDouble(13)>r2.getDouble(13)){
                user=r1.getString(12);
                user_mem=r1.getDouble(13);
            }
            else {
                user=r2.getString(12);
                user_mem=r2.getDouble(13);
            }

            String queue;
            double queue_mem;
            if (r1.getDouble(15)>r2.getDouble(15)) {
                queue=r1.getString(14);
                queue_mem=r1.getDouble(15);
            }
            else {
                queue=r2.getString(14);
                queue_mem=r2.getDouble(15);
            }

            String date1=r1.getString(26);
            String date2=r2.getString(26);

            int intdate1 = Integer.parseInt(date1.replace("-",""));
            int intdate2 = Integer.parseInt(date2.replace("-",""));

            String maxdate=date1;
            if (intdate2>intdate1)
                maxdate = date2;

            return RowFactory.create(
                r1.getString(0),
                r1.getDouble(1)+r2.getDouble(1),
                r1.getDouble(2)+r2.getDouble(2),
                r1.getDouble(3)+r2.getDouble(3),
                r1.getDouble(4)+r2.getDouble(4),
                r1.getDouble(5)+r2.getDouble(5),
                r1.getDouble(6)+r2.getDouble(6),
                r1.getDouble(7)+r2.getDouble(7),
                r1.getDouble(8)+r2.getDouble(8),
                r1.getDouble(9)+r2.getDouble(9),
                r1.getDouble(10)+r2.getDouble(10),
                r1.getDouble(11)+r2.getDouble(11),
                
                user,
                user_mem,
                queue,
                queue_mem,

                r1.getLong(16)+r2.getLong(16),
                r1.getLong(17)+r2.getLong(17),
                r1.getDouble(18)+r2.getDouble(18),
                r1.getLong(19)+r2.getLong(19),
                r1.getLong(20)+r2.getLong(20),
                r1.getDouble(21)+r2.getDouble(21),
                r1.getLong(22)+r2.getLong(22),
                r1.getLong(23)+r2.getLong(23),
                r1.getDouble(24)+r2.getDouble(24),
                
                r1.getInt(25)+r2.getInt(25),
                maxdate,
                r1.getDouble(27)+r2.getDouble(27),
                r1.getDouble(28)+r2.getDouble(28),
                r1.getDouble(29)+r2.getDouble(29));
        }
        }
    );
    System.out.println("[DONE]");

    if (debugPrint>0) {
        Map<String, Row> localResults = summedJobWaits.collectAsMap();
        for(Map.Entry<String, Row> entry : localResults.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }


    System.out.print("Combining key value pairs to a single row...");
    JavaRDD<Row> jobWaitToHive = summedJobWaits.map(
        new Function<Tuple2<String,Row>,Row>() {
            public Row call(Tuple2<String, Row> tpl) {
                String jobid = tpl._1();
                Row r = tpl._2();
                String system = r.getString(0);

                // Maximum category
                double cats[] = {r.getDouble(1),
                                 r.getDouble(2),
                                 r.getDouble(3),
                                 r.getDouble(4),
                                 r.getDouble(5),
                                 r.getDouble(6),
                                 r.getDouble(7),
                                 r.getDouble(8),
                                 r.getDouble(9),
                                 r.getDouble(10)};

                double maxcat=0;
                int maxid=-1;
                for (int i=0;i<10;i++) {
                    if (cats[i]>maxcat) {
                        maxcat=cats[i];
                        maxid=i;
                    }
                }

                String catstr = "";
                switch (maxid) {
                    case -1: catstr="no_category_detected";
                        break;
                    case 0: catstr="singlejob_max_capacity_memory_delayed";
                        break;
                    case 1: catstr="multijob_max_capacity_memory_delayed";
                        break;
                    case 2: catstr="userlimit_memory_delayed";
                        break;
                    case 3: catstr="elastic_unfariness_memory_delayed";
                        break;
                    case 4: catstr="competing_jobs_memory_delayed";
                        break;
                    case 5: catstr="singlejob_max_capacity_vcores_delayed";
                        break;
                    case 6: catstr="multijob_max_capacity_vcores_delayed";
                        break;
                    case 7: catstr="user_limit_vcores_delayed";
                        break;
                    case 8: catstr="elastic_unfariness_vcores_delayed";
                        break;
                    case 9: catstr="competing_jobs_vcores_delayed";
                        break;
                    default: catstr="BAD CATEGORY: SPARK PROGRAM ERROR";
                        break;
                }

                return RowFactory.create(
                        jobid
                        ,system
                        ,r.getDouble(1)
                        ,r.getDouble(2)
                        ,r.getDouble(3)
                        ,r.getDouble(4)
                        ,r.getDouble(5)
                        ,r.getDouble(6)
                        ,r.getDouble(7)
                        ,r.getDouble(8)
                        ,r.getDouble(9)
                        ,r.getDouble(10)
                        ,r.getDouble(11)
                        ,catstr

                        ,r.getString(12)
                        ,r.getString(14)

                        ,r.getLong(16) 
                        ,r.getLong(17) 
                        ,r.getDouble(18)/r.getInt(25)
                        ,r.getLong(19) 
                        ,r.getLong(20) 
                        ,r.getDouble(21)/r.getInt(25)
                        ,r.getLong(22) 
                        ,r.getLong(23) 
                        ,r.getDouble(24)/r.getInt(25)
                        ,r.getString(26) //date
                        ,r.getDouble(27)
                        ,r.getDouble(28)
                        ,r.getDouble(29)
                        );
            }
        }
    );
    System.out.println("[DONE]");

    System.out.print("Creating internal temporary tables for hive...");
    // Generate the schema based on the string of schema
    ArrayList<StructField> fields = new ArrayList<StructField>();

    fields.add(DataTypes.createStructField("job_id", DataTypes.StringType, true));
    fields.add(DataTypes.createStructField("system", DataTypes.StringType, true));
    fields.add(DataTypes.createStructField("singlejob_max_capacity_memory_delayed", DataTypes.DoubleType, true));
    fields.add(DataTypes.createStructField("multijob_max_capacity_memory_delayed", DataTypes.DoubleType, true));
    fields.add(DataTypes.createStructField("userlimit_memory_delayed", DataTypes.DoubleType, true));
    fields.add(DataTypes.createStructField("elastic_unfariness_memory_delayed", DataTypes.DoubleType, true));
    fields.add(DataTypes.createStructField("competing_jobs_memory_delayed", DataTypes.DoubleType, true));
    fields.add(DataTypes.createStructField("singlejob_max_capacity_vcores_delayed", DataTypes.DoubleType, true));
    fields.add(DataTypes.createStructField("multijob_max_capacity_vcores_delayed", DataTypes.DoubleType, true));
    fields.add(DataTypes.createStructField("user_limit_vcores_delayed", DataTypes.DoubleType, true));
    fields.add(DataTypes.createStructField("elastic_unfariness_vcores_delayed", DataTypes.DoubleType, true));
    fields.add(DataTypes.createStructField("competing_jobs_vcores_delayed", DataTypes.DoubleType, true));
    fields.add(DataTypes.createStructField("total_memory_delayed", DataTypes.DoubleType, true));
    fields.add(DataTypes.createStructField("max_category_name", DataTypes.StringType, true));

    fields.add(DataTypes.createStructField("most_competing_user_name", DataTypes.StringType, true));
    fields.add(DataTypes.createStructField("most_competing_queue_name", DataTypes.StringType, true));

    fields.add(DataTypes.createStructField("max_waiting_containers", DataTypes.LongType, true));
    fields.add(DataTypes.createStructField("max_running_containers", DataTypes.LongType, true));
    fields.add(DataTypes.createStructField("avg_waiting_containers", DataTypes.DoubleType, true));

    fields.add(DataTypes.createStructField("max_waiting_memory", DataTypes.LongType, true));
    fields.add(DataTypes.createStructField("max_running_memory", DataTypes.LongType, true));
    fields.add(DataTypes.createStructField("avg_waiting_memory", DataTypes.DoubleType, true));

    fields.add(DataTypes.createStructField("max_waiting_vcores", DataTypes.LongType, true));
    fields.add(DataTypes.createStructField("max_running_vcores", DataTypes.LongType, true));
    fields.add(DataTypes.createStructField("avg_waiting_vcores", DataTypes.DoubleType, true));
    fields.add(DataTypes.createStructField("date", DataTypes.StringType, true));
    fields.add(DataTypes.createStructField("unagg_jobwait", DataTypes.DoubleType, true));
    fields.add(DataTypes.createStructField("fullclustertime", DataTypes.DoubleType, true));
    fields.add(DataTypes.createStructField("slothours", DataTypes.DoubleType, true));
    

    StructType schema = DataTypes.createStructType(fields);

    // Apply the schema to the RDD.
    DataFrame jobWaitDataFrame = hc.createDataFrame(jobWaitToHive, schema);

    // Register the DataFrame as a table.
    jobWaitDataFrame.registerTempTable("jobwait");
    System.out.println("[DONE]");

    // SQL can be run over RDDs that have been registered as tables.
    //DataFrame results = sqlContext.sql("SELECT * FROM jobWaitDataFrame");

    System.out.print("Creating hive table "+args[2]+"...");
    hc.sql("DROP TABLE IF EXISTS "+args[2]);
    hc.sql(""+
    "CREATE TABLE "+
        args[2]+
            "("+
            "job_id                                     String"+
            ",system                                     String"+
            ",singlejob_max_capacity_memory_delayed      Double"+
            ",multijob_max_capacity_memory_delayed       Double"+
            ",userlimit_memory_delayed                   Double"+
            ",elastic_unfariness_memory_delayed          Double"+
            ",competing_jobs_memory_delayed              Double"+
            ",singlejob_max_capacity_vcores_delayed      Double"+
            ",multijob_max_capacity_vcores_delayed       Double"+
            ",user_limit_vcores_delayed                  Double"+
            ",elastic_unfariness_vcores_delayed          Double"+
            ",competing_jobs_vcores_delayed              Double"+
            ",total_memory_delayed                       Double"+
            ",max_category_name                          String"+

            ",most_competing_user_name                   String"+
            ",most_competing_queue_name                  String"+

            ",max_waiting_containers                     Bigint"+
            ",max_running_containers                     Bigint"+
            ",avg_waiting_containers                     Double"+

            ",max_waiting_memory                         Bigint"+
            ",max_running_memory                         Bigint"+
            ",avg_waiting_memory                         Double"+

            ",max_waiting_vcores                         Bigint"+
            ",max_running_vcores                         Bigint"+
            ",avg_waiting_vcores                         Double"+
            ",date                                       String"+
            ",unagg_jobwait                              Double"+
            ",fullclustertime                            Double"+
            ",slothours                                  Double"+
            ")"
            );
    hc.sql("INSERT INTO TABLE "+args[2]+" select "+
        "job_id"+
        ",system"+
        ",singlejob_max_capacity_memory_delayed"+
        ",multijob_max_capacity_memory_delayed"+
        ",userlimit_memory_delayed"+
        ",elastic_unfariness_memory_delayed"+
        ",competing_jobs_memory_delayed"+
        ",singlejob_max_capacity_vcores_delayed"+
        ",multijob_max_capacity_vcores_delayed"+
        ",user_limit_vcores_delayed"+
        ",elastic_unfariness_vcores_delayed "+
        ",competing_jobs_vcores_delayed"+
        ",total_memory_delayed"+
        ",max_category_name"+

        ",most_competing_user_name"+
        ",most_competing_queue_name"+

        ",max_waiting_containers"+
        ",max_running_containers"+
        ",avg_waiting_containers"+

        ",max_waiting_memory"+
        ",max_running_memory"+
        ",avg_waiting_memory"+

        ",max_waiting_vcores"+
        ",max_running_vcores"+
        ",avg_waiting_vcores"+
        ",date"+
        ",unagg_jobwait"+
        ",fullclustertime"+
        ",slothours"+
        " from jobwait");
    System.out.println("[DONE]");

    }
}

