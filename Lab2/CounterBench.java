package lab;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.map.IMap;

import java.util.Arrays;
import java.util.concurrent.*;

public class CounterBench {

    static class Settings {
        int threads = 10;
        int perThread = 10_000;
        int cpBatch = 1; 
        String clusterName = "counter-cluster";
        String[] addresses = {"127.0.0.1:5701","127.0.0.1:5702","127.0.0.1:5703"};
        String mapName = "counter-map";
        String key = "counter";
    }

    public static void main(String[] args) throws Exception {
        Settings s = parseArgs(args);
        long expected = (long) s.threads * s.perThread;

        System.out.println("Settings: threads=" + s.threads + ", perThread=" + s.perThread +
                ", expected=" + expected + ", cpBatch=" + s.cpBatch);
        System.out.println("Cluster: " + s.clusterName + " @ " + Arrays.toString(s.addresses));

        HazelcastInstance client = connect(s);

        try {
            IMap<String, Long> map = client.getMap(s.mapName);

            runScenario("1) Map / no locks", expected, () -> mapNoLocks(map, s));
            runScenario("2) Map / pessimistic lock()", expected, () -> mapPessimistic(map, s));
            runScenario("3) Map / optimistic CAS replace()", expected, () -> mapOptimistic(map, s));
            runScenario("4) CP / IAtomicLong", expected, () -> cpAtomic(client, s));

        } finally {
            client.shutdown();
        }
    }

static HazelcastInstance connect(Settings s) {
    ClientConfig cfg = new ClientConfig();
    cfg.setClusterName(s.clusterName);

    var net = cfg.getNetworkConfig();
    net.addAddress(s.addresses);

    net.setSmartRouting(false);

    return HazelcastClient.newHazelcastClient(cfg);
}


    static void runScenario(String name, long expected, Callable<Long> scenario) throws Exception {
        long t0 = System.nanoTime();
        long value = scenario.call();
        double secs = (System.nanoTime() - t0) / 1_000_000_000.0;
        String ok = (value == expected) ? "OK" : "BAD";
        System.out.printf("%-38s time=%8.3fs  value=%d  expected=%d  => %s%n",
                name, secs, value, expected, ok);
    }

    static long mapNoLocks(IMap<String, Long> map, Settings s) throws Exception {
        map.put(s.key, 0L);
        runThreads(s.threads, () -> {
            for (int i = 0; i < s.perThread; i++) {
                Long v = map.get(s.key);
                map.put(s.key, v + 1);
            }
        });
        return map.get(s.key);
    }

    static long mapPessimistic(IMap<String, Long> map, Settings s) throws Exception {
        map.put(s.key, 0L);
        runThreads(s.threads, () -> {
            for (int i = 0; i < s.perThread; i++) {
                map.lock(s.key);
                try {
                    Long v = map.get(s.key);
                    map.put(s.key, v + 1);
                } finally {
                    map.unlock(s.key);
                }
            }
        });
        return map.get(s.key);
    }

    static long mapOptimistic(IMap<String, Long> map, Settings s) throws Exception {
        map.put(s.key, 0L);
        runThreads(s.threads, () -> {
            for (int i = 0; i < s.perThread; i++) {
                while (true) {
                    Long oldVal = map.get(s.key);
                    if (map.replace(s.key, oldVal, oldVal + 1)) break;
                }
            }
        });
        return map.get(s.key);
    }

    static long cpAtomic(HazelcastInstance client, Settings s) throws Exception {
        IAtomicLong al = client.getCPSubsystem().getAtomicLong("counter");
        al.set(0L);

        if (s.cpBatch <= 1) {
            runThreads(s.threads, () -> {
                for (int i = 0; i < s.perThread; i++) {
                    al.incrementAndGet();
                }
            });
        } else {
            int batch = s.cpBatch;
            int fullBatches = s.perThread / batch;
            int rem = s.perThread % batch;

            runThreads(s.threads, () -> {
                for (int i = 0; i < fullBatches; i++) {
                    al.addAndGet(batch);
                }
                if (rem > 0) al.addAndGet(rem);
            });
        }

        return al.get();
    }

    static void runThreads(int threads, Runnable worker) throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try { worker.run(); }
                finally { latch.countDown(); }
            });
        }

        latch.await();
        pool.shutdown();
    }

    static Settings parseArgs(String[] args) {
        Settings s = new Settings();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--threads" -> s.threads = Integer.parseInt(args[++i]);
                case "--perThread" -> s.perThread = Integer.parseInt(args[++i]);
                case "--cpBatch" -> s.cpBatch = Integer.parseInt(args[++i]);
                default -> { }
            }
        }
        return s;
    }
}
