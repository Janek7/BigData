package hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.Map;

public class ServerDemo {

    public static void main(String[] args) {

        HazelcastInstance hzi = Hazelcast.newHazelcastInstance();

        Map<Long, String> map = hzi.getMap("test");

        map.put(1234L, "value1");

        System.out.println(map.get(1234L));

    }

}
