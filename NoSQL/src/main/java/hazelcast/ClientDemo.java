package hazelcast;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;

import java.util.Map;

public class ClientDemo {

    public static void main(String[] args) {

        ClientConfig config = new ClientConfig();
        GroupConfig groupConfig = config.getGroupConfig();
        groupConfig.setName("dev");
        groupConfig.setPassword("dev-pass");
        HazelcastInstance hzc = HazelcastClient.newHazelcastClient(config);

        Map<Long, String> map = hzc.getMap("test");
        System.out.println(map.get(1234L));
    }

}
