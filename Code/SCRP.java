import java.io.*;
import java.lang.reflect.*;
import java.util.*;
import java.lang.Math;
import java.util.Map.Entry;
import com.google.gson.*;
import com.google.gson.reflect.TypeToken;

class Image {
    String job_id; // Stores the job id of the service
    double frequency; // Stores the frequency of the service
    double stay; // stores the no of rounds service remains in the cache
    double SCRP; // stores the SCRP score of the service
    long task_index; // Stores the task index of the service
    double CPU; // Stores the CPU requirement of the service
    double RAM; // Stores the RAM requirement of the service
    double Runtime_disk; // Stores the runtime disk requirement of the service
    double Storage; // Stores the amount of storage required by service to be cloned in cache

    public Image(String job_id, double frequency, double stay, double SCRP, long task_index, double cpu, double ram,
            double runtime_disk, double storage) { // Constructor to initialize instance of Image class
        this.job_id = job_id;
        this.frequency = frequency;
        this.stay = stay;
        this.SCRP = SCRP;
        this.task_index = task_index;
        this.CPU = cpu;
        this.RAM = ram;
        this.Runtime_disk = runtime_disk;
        this.Storage = storage;
    }
}

class Cache {
    int no_services; // no of services currently in the Edge Cache
    int no_services_max; // max no of services to be processed
    double CPU_max; // max CPU available in cache(scaled to 1)
    double RAM_max; // max RAM available in cache(scaled to 1)
    double Storage_max; // max storage available in cache(scaled to 1)
    double Runtime_disk_max; // max runtime disk available(scaled to 1)
    double Disk_storage_max; // max sum of runtime disk and storage
    double CPU_curr; // Current usage of CPU in Edge Server
    double RAM_curr;// Current usage of RAM in Edge Server
    double Runtime_disk_curr;// Current usage of Runtime disk in Edge Server
    double Storage_curr; // Current usage of Storage in cache
    HashMap<String, Image> Running; // Contains images of all running services and their service id
    HashMap<String, Image> Free; // Contains images of all free services and their service id

    public Cache(String drivers) throws FileNotFoundException {
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
        Running = new HashMap<String, Image>();
        Free = new HashMap<String, Image>();
        in.close();
    }

    double SCRP_calc(Image service, double CPU_curr, double RAM_curr, double Disk_curr, double CPU_max, double RAM_max,
            double Disk_max) {
        double disk = service.Runtime_disk; // + service.Storage;
        double R = (CPU_curr / CPU_max) * service.CPU + (RAM_curr / RAM_max) * service.RAM
                + ((Disk_curr) / (Disk_max)) * disk;
        // System.out.println("R: " + R);
        if (R == 0) {
            return Double.MAX_VALUE;
        }
        return ((double) service.frequency / (service.stay * R));
    }

    String min_SCRP(HashMap<String, Image> hm, Cache cache, double CPU_curr, double RAM_curr, double Disk_curr,
            double CPU_max, double RAM_max, double Disk_max) { // Function to find service id that has minimum timestamp
                                                               // from the set of images
        String min_SCRP = "";
        double SCRP_min = Double.MAX_VALUE;
        for (Entry<String, Image> entry : hm.entrySet()) // Iterating over whole set of images
        {
            entry.getValue().SCRP = cache.SCRP_calc(entry.getValue(), CPU_curr, RAM_curr, Disk_curr, CPU_max, RAM_max,
                    Disk_max);
            if (entry.getValue().SCRP < SCRP_min) {
                SCRP_min = entry.getValue().frequency;
                min_SCRP = entry.getKey();
            }
        }
        return min_SCRP;
    }

    double priority_calc(double clock, double frequency, double size) {
        if (size == 0) {
            return Double.MAX_VALUE;
        }
        return (clock + (frequency / size));
    }

    String min_priority(double clock, HashMap<String, Image> hm, Cache cache) { // Function to find service id that has
                                                                                // minimum priority from the set of
                                                                                // images
        String min_pr = "";
        double priority_min = Double.MAX_VALUE;
        for (Entry<String, Image> entry : hm.entrySet()) // Iterating over whole set of images
        {
            double pr = cache.priority_calc(clock, entry.getValue().stay, entry.getValue().Runtime_disk);
            if (pr < priority_min) {
                priority_min = pr;
                min_pr = entry.getKey();
            }
        }
        return min_pr;
    }

    String service_id_gen(String job_id, String task_index) { // Generates a string of tuple(task index, job id)
        return (task_index + "," + job_id);
    }

    void print_cache(Cache cache) {
        System.out.println("No of services: " + cache.no_services);
        System.out.println("Current Status: ");
        System.out.println("CPU: " + cache.CPU_curr + " RAM: " + cache.RAM_curr + " Runtime Disk:"
                + cache.Runtime_disk_curr + " Storage: " + cache.Storage_curr);
        System.out.println("Running Services: ");
        for (Entry<String, Image> entry : cache.Running.entrySet()) {
            Image image = entry.getValue();
            System.out.println("Service_id: " + entry.getKey() + " CPU: " + image.CPU + " RAM: " + image.RAM
                    + " Runtime Disk: " + image.Runtime_disk + " Storage: " + image.Storage);
        }
        System.out.println("Free Services: ");
        for (Entry<String, Image> entry : cache.Free.entrySet()) {
            Image image = entry.getValue();
            System.out.println("Service_id: " + entry.getKey() + " CPU: " + image.CPU + " RAM: " + image.RAM
                    + " Runtime Disk: " + image.Runtime_disk + " Storage: " + image.Storage);
        }
    }
}

class SCRP_cloud {
    Cache replacement(Cache cache, Image new_service, double clock) {
        // Creating deep copies of Hashmap: Free and Running.
        Gson gson = new Gson();
        String jsonString = gson.toJson(cache.Running);
        Type type = new TypeToken<HashMap<String, Image>>() {
        }.getType();
        HashMap<String, Image> Running_clone = gson.fromJson(jsonString, type);

        Gson gson1 = new Gson();
        String jsonString1 = gson1.toJson(cache.Free);
        Type type1 = new TypeToken<HashMap<String, Image>>() {
        }.getType();
        HashMap<String, Image> Free_clone = gson.fromJson(jsonString1, type1);

        double CPU_copy = cache.CPU_curr;
        double RAM_copy = cache.RAM_curr;
        double Runtime_copy = cache.Runtime_disk_curr;
        double Storage_copy = cache.Storage_curr;
        int no_services_copy = cache.no_services;
        double Disk_storage_curr = 0;
        double Disk_storage_req = new_service.Runtime_disk; // +Storage_req;

        // while(There are no enough resources)
        while (!((new_service.CPU + CPU_copy <= cache.CPU_max) && (new_service.RAM + RAM_copy <= cache.RAM_max)
                && (Disk_storage_req + Disk_storage_curr <= cache.Disk_storage_max))) {

            Disk_storage_curr = Runtime_copy;// + cache.Storage_curr;
            if ((new_service.CPU + cache.CPU_curr > cache.CPU_max)
                    || (new_service.RAM + cache.RAM_curr > cache.RAM_max)) {
  
                String to_remove = cache.min_SCRP(Running_clone, cache, CPU_copy, RAM_copy, Runtime_copy, cache.CPU_max,
                        cache.RAM_max, cache.Runtime_disk_max); // finding service id with least time stamp
                Image remove = Running_clone.get(to_remove);
                remove.SCRP = cache.SCRP_calc(remove, CPU_copy, RAM_copy, Runtime_copy, cache.CPU_max, cache.RAM_max,
                        cache.Runtime_disk_max);
                if (new_service.SCRP > remove.SCRP) {
                    Free_clone.put(to_remove, remove); // inserting its image to free set of images
                    CPU_copy -= remove.CPU;
                    RAM_copy -= remove.RAM;
                    Runtime_copy -= remove.Runtime_disk;
                    Storage_copy -= remove.Storage;
                    Running_clone.remove(to_remove); // removing its image from running set of images
                    no_services_copy--;
                } else {
                    break;
                }
            }
            if (Disk_storage_req + Disk_storage_curr > cache.Disk_storage_max) {
                if (Free_clone.isEmpty()) // if there are no free images
                {

                    String to_remove = cache.min_SCRP(Running_clone, cache, CPU_copy, RAM_copy, Runtime_copy,
                            cache.CPU_max, cache.RAM_max, cache.Runtime_disk_max); // finding service id with minimum
                                                                                   // timestamp
                    Image remove = Running_clone.get(to_remove);
                    remove.SCRP = cache.SCRP_calc(remove, CPU_copy, RAM_copy, Runtime_copy, cache.CPU_max,
                            cache.RAM_max, cache.Runtime_disk_max);
                    if (new_service.SCRP > remove.SCRP) {
                        Free_clone.put(to_remove, Running_clone.get(to_remove)); // adding to free set of images
                        CPU_copy -= remove.CPU;
                        RAM_copy -= remove.RAM;
                        Runtime_copy -= remove.Runtime_disk;
                        Storage_copy -= remove.Storage;
                        Running_clone.remove(to_remove); // removing from running set of images
                        no_services_copy--;
                    } else {
                        break;
                    }
                } else {
                    String to_remove = cache.min_priority(clock, Free_clone, cache); // finding service with minimum
                                                                                     // timestamp
                    Image remove = Running_clone.get(to_remove);
                    Runtime_copy -= remove.Runtime_disk;
                    Storage_copy -= remove.Storage;
                    clock += cache.priority_calc(clock, remove.stay, remove.Runtime_disk);
                    Free_clone.remove(to_remove); // removing service from cache
                    no_services_copy--;
                }
            }
        }
        // Deep copying from Free_clone and Running_clone to Free and Running
        // respectively
        Gson gson2 = new Gson();
        String jsonString2 = gson2.toJson(Running_clone);
        Type type2 = new TypeToken<HashMap<String, Image>>() {
        }.getType();
        cache.Running = gson2.fromJson(jsonString2, type2);

        Gson gson3 = new Gson();
        String jsonString3 = gson3.toJson(Free_clone);
        Type type3 = new TypeToken<HashMap<String, Image>>() {
        }.getType();
        cache.Free = gson3.fromJson(jsonString3, type3);

        cache.CPU_curr = CPU_copy;
        cache.RAM_curr = RAM_copy;
        cache.Storage_curr = Storage_copy;
        cache.Runtime_disk_curr = Runtime_copy;
        cache.no_services = no_services_copy;

        // cloning new service -- adding new service to cache
        cache.Running.put(new_service.job_id, new_service);
        if (cache.Free.containsKey(new_service.job_id))
            cache.Free.remove(new_service.job_id);
        cache.CPU_curr += new_service.CPU;
        cache.RAM_curr += new_service.RAM;
        cache.Runtime_disk_curr += new_service.Runtime_disk;
        cache.Storage_curr += new_service.Storage;
        cache.no_services++;
        return cache;
    }

    void SCRP_cache(Cache cache, String drivers, String requests) throws IOException {
        int cache_hit = 0; // NO of cache hit
        int count = 0;// Counts no of services requested
        double clock = 0;
        File request = new File(requests); // File containing requests
        Scanner sc = new Scanner(request);
        String line = "";
        String tokens[] = new String[6];

        while (sc.hasNextLine() && (count < cache.no_services_max)) {
            for (Entry<String, Image> entry : cache.Running.entrySet()) {
                entry.getValue().stay += 1;
            }
            for (Entry<String, Image> entry : cache.Free.entrySet()) {
                entry.getValue().stay += 1;
            }

            double progress = (double) count * 100 / cache.no_services_max;
            System.out.println("Progress: " + progress);

            int min = 1, max = 5;
            int rand = (int) (Math.random() * (max - min + 1) + min);
            if (rand != 1) {
                sc.nextLine();
                continue;
            }

            line = sc.nextLine();
            tokens = line.split(","); // contains information in the request
            String service_id = tokens[0]; // generate service id for the service
            // System.out.println(service_id);

            double CPU_req = Double.parseDouble(tokens[3]), RAM_req = Double.parseDouble(tokens[4]),
                    Runtime_disk_req = Double.parseDouble(tokens[5]); // service requirements
            double Storage_req = 0; // assuming for now

            Image new_service = new Image(service_id, 1, 1, 0.00, Long.parseLong(tokens[2]), CPU_req, RAM_req,
                    Storage_req, Storage_req);

            new_service.SCRP = cache.SCRP_calc(new_service, cache.CPU_curr, cache.RAM_curr, cache.Runtime_disk_curr,
                    cache.CPU_max, cache.RAM_max, cache.Runtime_disk_max);

            if (cache.Running.containsKey(service_id)) // if service is cached
            {
                cache_hit++;
                Image to_update = cache.Running.get(service_id);
                to_update.frequency += 1; // update its frequency
                cache.Running.put(service_id, to_update);
            } else if (cache.Free.containsKey(service_id)) {
                Image replace = cache.Free.get(service_id);
                replace.frequency = 1;
                // ignoring storage req in the below condition.
                if (((CPU_req + cache.CPU_curr <= cache.CPU_max) && (RAM_req + cache.RAM_curr <= cache.RAM_max)
                        && (Runtime_disk_req + cache.Runtime_disk_curr <= cache.Runtime_disk_max))) {
                    cache.Running.put(service_id, replace);
                    cache.CPU_curr += replace.CPU;
                    cache.RAM_curr += replace.RAM;
                    cache.Runtime_disk_curr += replace.Runtime_disk;
                    cache.Storage_curr += replace.Storage;
                    cache.Free.remove(service_id);
                } else {
                    double cl[] = new double[1];
                    cl[0] = clock;
                    cache = this.replacement(cache, new_service, cl[0]);
                    clock = cl[0];
                }
            } else {
                double cl[] = new double[1];
                cl[0] = clock;
                cache = this.replacement(cache, new_service, cl[0]);
                clock = cl[0];
            }
            count++;
        }
        double hit_rate = (double) 100 * cache_hit / count;
        FileWriter writer = new FileWriter("F:/Try/Results.csv", true);
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

public class SCRP {
    public static void main(String[] args) throws IOException {
        Scanner in = new Scanner(System.in);
        String drivers = "";
        String requests = "";
        System.out.println("Please enter the path of program driver file: ");
        drivers = in.nextLine();
        System.out.println("Please enter the path of requests file: ");
        requests = in.nextLine();

        Cache c = new Cache(drivers);
        SCRP_cloud e = new SCRP_cloud();
        e.SCRP_cache(c, drivers, requests);

        // c.print_cache(c);
        in.close();
    }
}