import java.io.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.Map.Entry;
import java.sql.Timestamp;
import com.google.gson.*;
import com.google.gson.reflect.TypeToken;

class Image
{
    String job_id; //Stores the job id of the service
    long timestamp; //Stores the timestamp of the service
    long task_index; //Stores the task index of the service
    double CPU; // Stores the CPU requirement of the service
    double RAM; //Stores the RAM requirement of the service
    double Runtime_disk; //Stores the runtime disk requirement of the service
    double Storage; // Stores the amount of storage required by service to be cloned in cache

    public Image(String job_id, long timestamp, long task_index, double cpu, double ram, double runtime_disk, double storage)
    {   //Constructor to initialize instance of Image class
        this.job_id = job_id;
        this.timestamp = timestamp;
        this.task_index = task_index;
        this.CPU = cpu;
        this.RAM = ram;
        this.Runtime_disk = runtime_disk;
        this.Storage = storage;
    }
}

class Cache
{
    int no_services; //no of services currently in the Edge Cache
    int no_services_max; //max no of services to be processed
    double CPU_max; // max CPU available in cache(scaled to 1)
    double RAM_max; //max RAM available in cache(scaled to 1)
    double Storage_max; //max storage available in cache(scaled to 1)
    double Runtime_disk_max; //max runtime disk available(scaled to 1)
    double Disk_storage_max; //max sum of runtime disk and storage
    double CPU_curr; //Current usage of CPU in Edge Server
    double RAM_curr;//Current usage of RAM in Edge Server
    double Runtime_disk_curr;//Current usage of Runtime disk in Edge Server
    double Storage_curr; //Current usage of Storage in cache 
    HashMap<String,Image> Running; //Contains images of all running services and their service id
    HashMap<String,Image> Free; //Contains images of all free services and their service id

    public Cache(String drivers) throws FileNotFoundException
    {
        File driver = new File(drivers);
        Scanner in = new Scanner(driver);
        String line = in.nextLine();
        String tokens[] = line.split(",");
        no_services_max = Integer.parseInt(tokens[0]);
        no_services = 0;
        CPU_max = Double.parseDouble(tokens[1]);
        RAM_max = Double.parseDouble(tokens[2]);
        Runtime_disk_max = Double.parseDouble(tokens[3]);
        Storage_max = Double.parseDouble(tokens[4]);
        Disk_storage_max = Runtime_disk_max + Storage_max;
        CPU_curr = 0.00;
        RAM_curr = 0.00;
        Runtime_disk_curr = 0.00;
        Storage_curr = 0.00;
        Running = new HashMap<String,Image>();
        Free = new HashMap<String,Image>();
        in.close();
    }

    String min_timestamp(HashMap<String,Image> hm)
    { // Function to find service id that has minimum timestamp from the set of images
        String min_ts = "";
        long ts = Long.MAX_VALUE;
        for(Entry<String,Image> entry : hm.entrySet()) // Iterating over whole set of images
        {
            if(entry.getValue().timestamp < ts)
            {
                ts = entry.getValue().timestamp;
                min_ts = entry.getKey();
            }
        }
        return min_ts;
    }

    String service_id_gen(String job_id, String task_index)
    { //Generates a string of tuple(task index, job id)
        return (task_index + "," + job_id);
    }

    void print_cache(Cache cache)
    {
        System.out.println("No of services: " + cache.no_services);
        System.out.println("Current Status: ");
        System.out.println("CPU: " + cache.CPU_curr + " RAM: "+ cache.RAM_curr + " Runtime Disk:" + cache.Runtime_disk_curr + " Storage: " + cache.Storage_curr);
        System.out.println("Running Services: ");
        for(Entry<String,Image> entry : cache.Running.entrySet())
        {
            Image image = entry.getValue();
            System.out.println("Service_id: " + entry.getKey() + " CPU: " + image.CPU + " RAM: " + image.RAM + " Runtime Disk: " + image.Runtime_disk + " Storage: " + image.Storage);
        }
        System.out.println("Free Services: ");
        for(Entry<String,Image> entry : cache.Free.entrySet())
        {
            Image image = entry.getValue();
            System.out.println("Service_id: " + entry.getKey() + " CPU: " + image.CPU + " RAM: " + image.RAM + " Runtime Disk: " + image.Runtime_disk + " Storage: " + image.Storage);
        }
    }
}

class FIFO_Cloud
{
    void FIFO_cache(Cache cache, String drivers, String requests) throws IOException
    {
        int cache_hit = 0; // NO of cache hit
        int count = 0;//Counts no of services requested
        File request = new File(requests); //File containing requests
        Scanner sc = new Scanner(request);
        String line = "";
        String tokens[] = new String[6];
        while(sc.hasNextLine() && (count < cache.no_services_max))
        {
            double progress = (double) count*100/cache.no_services_max;
            System.out.println("Progress: " + progress);
            Timestamp ts = new Timestamp(System.currentTimeMillis());
            line = sc.nextLine();
            tokens = line.split(","); //contains information in the request
            String service_id = tokens[0]; //generate service id for the service
            //System.out.println(service_id);
            double CPU_req = Double.parseDouble(tokens[3]), RAM_req = Double.parseDouble(tokens[4]), Runtime_disk_req = Double.parseDouble(tokens[5]); //service requirements
            double Storage_req = 0; //assuming for now

            if(cache.Running.containsKey(service_id)) //if service is cached
            {
                cache_hit++;
            }
            else if(cache.Free.containsKey(service_id))
            {
                Image replace = cache.Free.get(service_id);
                //ignoring storage req in the below condition.
                if(((CPU_req + cache.CPU_curr <= cache.CPU_max) && (RAM_req + cache.RAM_curr <= cache.RAM_max) && (Runtime_disk_req + cache.Runtime_disk_curr <= cache.Runtime_disk_max)))
                {
                    replace.timestamp = ts.getTime();
                    cache.Running.put(service_id, replace);
                    cache.CPU_curr += replace.CPU;
                    cache.RAM_curr += replace.RAM;
                    cache.Runtime_disk_curr += replace.Runtime_disk;
                    cache.Storage_curr += replace.Storage;
                    cache.Free.remove(service_id);
                }
            }
            else
            {
                //Creating deep copies of Hashmap: Free and Running.
                Gson gson = new Gson();
                String jsonString = gson.toJson(cache.Running);
                Type type = new TypeToken<HashMap<String,Image>>(){}.getType();
                HashMap<String,Image> Running_clone = gson.fromJson(jsonString, type);

                Gson gson1 = new Gson();
                String jsonString1 = gson1.toJson(cache.Free);
                Type type1 = new TypeToken<HashMap<String,Image>>(){}.getType();
                HashMap<String,Image> Free_clone = gson.fromJson(jsonString1, type1);

                double CPU_copy = cache.CPU_curr;
                double RAM_copy = cache.RAM_curr;
                double Runtime_copy = cache.Runtime_disk_curr;
                double Storage_copy = cache.Storage_curr;
                int no_services_copy = cache.no_services;
                double Disk_storage_curr = 0;
                double Disk_storage_req =  Runtime_disk_req; //+Storage_req;
                
                //while(There are no enough resources)
                while(!((CPU_req + CPU_copy <= cache.CPU_max) && (RAM_req + RAM_copy <= cache.RAM_max) && (Disk_storage_req + Disk_storage_curr <= cache.Disk_storage_max)))
                {
                    Disk_storage_curr = Runtime_copy;// + cache.Storage_curr;
                    if((CPU_req + cache.CPU_curr > cache.CPU_max) || (RAM_req + cache.RAM_curr > cache.RAM_max))
                    {
                        String to_remove = cache.min_timestamp(Running_clone); //finding service id with least time stamp
                        Image remove = Running_clone.get(to_remove);
                        Free_clone.put(to_remove, remove); //inserting its image to free set of images
                        CPU_copy -= remove.CPU;
                        RAM_copy -= remove.RAM;
                        Runtime_copy -= remove.Runtime_disk;
                        Storage_copy -= remove.Storage;
                        Running_clone.remove(to_remove); //removing its image from running set of images
                        no_services_copy--;
                    }
                    if(Disk_storage_req + Disk_storage_curr > cache.Disk_storage_max)
                    {
                        if(Free_clone.isEmpty()) //if there are no free images
                        {
                            String to_remove = cache.min_timestamp(Running_clone); //finding service id with minimum timestamp
                            Image remove = Running_clone.get(to_remove);
                            Free_clone.put(to_remove, Running_clone.get(to_remove)); //adding to free set of images
                            CPU_copy -= remove.CPU;
                            RAM_copy -= remove.RAM;
                            Runtime_copy -= remove.Runtime_disk;
                            Storage_copy -= remove.Storage;
                            Running_clone.remove(to_remove); //removing from running set of images
                            no_services_copy--;
                        }
                        else
                        {
                            String to_remove = cache.min_timestamp(Free_clone); //finding service with minimum timestamp
                            Image remove = Running_clone.get(to_remove);
                            Runtime_copy -= remove.Runtime_disk;
                            Storage_copy -= remove.Storage;
                            Free_clone.remove(to_remove); //removing service from cache
                            no_services_copy--;
                        }
                    }
                }
                //Deep copying from Free_clone and Running_clone to Free and Running respectively
                Gson gson2 = new Gson();
                String jsonString2 = gson2.toJson(Running_clone);
                Type type2 = new TypeToken<HashMap<String,Image>>(){}.getType();
                cache.Running = gson2.fromJson(jsonString2, type2);

                Gson gson3 = new Gson();
                String jsonString3 = gson3.toJson(Free_clone);
                Type type3 = new TypeToken<HashMap<String,Image>>(){}.getType();
                cache.Free = gson3.fromJson(jsonString3, type3);

                cache.CPU_curr = CPU_copy;
                cache.RAM_curr = RAM_copy;
                cache.Storage_curr = Storage_copy;
                cache.Runtime_disk_curr = Runtime_copy;
                cache.no_services = no_services_copy;

                //cloning new service -- adding new service to cache
                cache.Running.put(service_id, new Image(tokens[0], ts.getTime(), Long.parseLong(tokens[2]), Double.parseDouble(tokens[3]), Double.parseDouble(tokens[4]), Double.parseDouble(tokens[5]), Storage_req));
                if(cache.Free.containsKey(service_id))
                    cache.Free.remove(service_id);
                cache.CPU_curr += CPU_req;
                cache.RAM_curr += RAM_req;
                cache.Runtime_disk_curr += Runtime_disk_req;
                cache.Storage_curr += Storage_req;
                cache.no_services++;
            }
            count++;
        }
        double hit_rate = (double)100*cache_hit/count;
        FileWriter writer = new FileWriter("F:/Service Cache Implementation/Experiments/FIFO/1.4/Results.csv",true);
        StringBuilder sb = new StringBuilder();
        sb.append(drivers);
        sb.append(',');
        sb.append(count);
        sb.append(',');
        sb.append(cache_hit);
        sb.append(',');
        sb.append(hit_rate);
        sb.append('\n');

        writer.write(sb.toString());
        writer.close();        
        sc.close();
    }
}

public class FIFO
{
    public static void main(String[] args) throws IOException 
    {
        Scanner in = new Scanner(System.in);
        String drivers = "";
        String requests = "";
        System.out.println("Please enter the path of program driver file: ");
        drivers = in.nextLine();
        System.out.println("Please enter the path of requests file: ");
        requests = in.nextLine();
        Cache c = new Cache(drivers);
        FIFO_Cloud e = new FIFO_Cloud();
        e.FIFO_cache(c, drivers, requests);
        //c.print_cache(c);
        in.close();
    }
}