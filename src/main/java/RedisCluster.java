import javafx.util.Pair;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RedisCluster {
    private final static int port = 6379;
    private static MessageDigest messageDigest;

    static {
        try {
            messageDigest = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    private ArrayList<Pair<Integer, Jedis>> servers;

    private RedisCluster() {
    }

    public static Builder getBuilder() {
        return new Builder();
    }

    /**
     * Uses SHA-1 hash
     *
     * @param s String to be hashed
     * @return Abs. value of an integer hash
     */
    private static int getHash(String s) {
        byte[] hashSHA = messageDigest.digest(s.getBytes());
        int hash = hashSHA[0] + hashSHA[1] * 8 + hashSHA[2] * 64 + hashSHA[3] * 512;
        hash = Math.abs(hash);
        return hash;
    }

    public Map<String, String> getMap(String hashKey) {
        return new RedisMapImpl(hashKey);
    }

    /**
     * @param id Server id
     * @return Jedis server
     */
    private Jedis getServerById(Integer id) {
        return servers.get(id).getValue();
    }

    /**
     * @return Stream of all cluster jedis servers
     */
    private Stream<Jedis> getJedisStream() {
        return servers.stream()
                .map(Pair::getValue);
    }

    /**
     * Builder class of a RedisCluster
     */
    public static class Builder {
        private ArrayList<Pair<Integer, Jedis>> servers = new ArrayList<>();

        private Builder() {
        }

        /**
         * Uses standard port (6379)
         *
         * @param host String that contains domain name or ip address
         * @return Builder with new state
         */
        public Builder addServer(String host) {
            return addServer(host, port);
        }

        /**
         * @param host String that contains domain name or ip address
         * @param port Server port
         * @return Builder with new state
         */
        public Builder addServer(String host, int port) {
            String s = host + ":" + port;
            int hash = getHash(s);

            if (servers.stream().map(Pair::getKey).anyMatch(x -> x == hash)) {
                return this;
            }

            Jedis server = new Jedis(host, port);

            servers.add(new Pair<>(hash, server));

            servers.sort(Comparator.comparing(Pair::getKey));

            return this;
        }

        /**
         * @return Built RedisCluster
         */
        public RedisCluster build() {
            RedisCluster cluster = new RedisCluster();
            cluster.servers = this.servers;

            return cluster;
        }
    }

    private class RedisMapImpl implements Map<String, String> {
        private final String hashKey;

        RedisMapImpl(String hashKey) {
            this.hashKey = hashKey;
        }

        public String getHashKey() {
            return hashKey;
        }

        /**
         * @param key String key
         * @return jedis server
         */
        private Jedis getServer(String key) {
            int serverId = getServerId(key);
            return getServerById(serverId);
        }

        /**
         * @param key String key
         * @return index of server
         */
        private int getServerId(String key) {
            int hash = getHash(key);

            return hash % servers.size();
        }

        @Override
        public int size() {
            return getJedisStream()
                    .mapToInt(jedis -> {
                        long len = jedis.hlen(hashKey);
                        return Math.toIntExact(len);
                    })
                    .sum();
        }

        @Override
        public boolean isEmpty() {
            return (0 == size());
        }

        @Override
        public boolean containsKey(Object field) {
            Jedis jedis = getServerByField(field);
            return jedis.hexists(hashKey, (String) field);
        }

        @Override
        public boolean containsValue(Object value) {
            return getJedisStream()
                    .flatMap(jedis -> jedis.hvals(hashKey).stream())
                    .anyMatch(s -> s.equals(value));
        }

        @Override
        public String get(Object field) {
            Jedis jedis = getServerByField(field);
            return jedis.hget(hashKey, (String) field);
        }

        private Jedis getServerByField(Object field) {
            return getServer(field + "_" + hashKey);
        }

        @Override
        public String put(String field, String value) {
            Jedis jedis = getServerByField(field);

            Transaction t = jedis.multi();
            Response<String> prev = t.hget(hashKey, field);
            t.hset(hashKey, field, value);
            t.exec();

            return prev.get();
        }

        @Override
        public String remove(Object field) {
            Jedis jedis = getServer(field);

            Transaction t = jedis.multi();
            Response<String> prev = t.hget(hashKey, (String) field);
            t.hdel(hashKey, (String) field);
            t.exec();

            return prev.get();
        }

        private Jedis getServer(Object field) {
            return getServerByField(field);
        }

        @Override
        public void putAll(Map<? extends String, ? extends String> map) {
            map.entrySet().stream()
                    .collect(Collectors.groupingBy(this::getServerIdByEntry))
                    .forEach((id, entryList) -> {
                        Jedis jedis = getServerById(id);

                        Transaction t = jedis.multi();
                        entryList.forEach(entry -> {
                            t.hset(hashKey, entry.getKey(), entry.getValue());
                        });
                        t.exec();
                    });
        }

        private int getServerIdByEntry(Entry<? extends String, ? extends String> entry) {
            return getServerId(entry.getKey() + "_" + hashKey);
        }

        @Override
        public void clear() {
            getJedisStream()
                    .forEach(jedis -> jedis.del(hashKey));
        }

        @Override
        public Set<String> keySet() {
            return getJedisStream()
                    .flatMap(jedis -> jedis.hkeys(hashKey).stream())
                    .collect(Collectors.toSet());
        }

        @Override
        public Collection<String> values() {
            return getJedisStream()
                    .flatMap(jedis -> jedis.hvals(hashKey).stream())
                    .collect(Collectors.toSet());
        }

        @Override
        public Set<Entry<String, String>> entrySet() {
            return getJedisStream()
                    .flatMap(jedis -> jedis.hgetAll(hashKey).entrySet().stream())
                    .collect(Collectors.toSet());
        }

    }
}
