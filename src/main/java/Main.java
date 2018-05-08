import java.util.Map;

public class Main {
    public static void main(String[] args) {
        RedisCluster redisCluster = RedisCluster.getBuilder()
                .addServer("localhost")
                .build();

        Map<String, String> map = redisCluster.getMap("key");
        Map<String, String> map1 = redisCluster.getMap("key1");

        map.clear();
        map1.clear();

        for (int i = 0; i < 2000; i++) {
            map.put("field" + i, "value" + i);
        }

        System.out.println(map.size());
        System.out.println(map1.size());

        map1.putAll(map);

        System.out.println(map.size());
        System.out.println(map1.size());
    }
}
