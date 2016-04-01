package hazelcast_distribution;

import com.hazelcast.config.Config;
import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.core.Hazelcast;
import org.rhea_core.network.Machine;

/**
 * @author Orestis Melkonian
 */
public class HazelcastMain {

    public static void init(Machine machine) {
        Config config = new Config();

        // Setup skills
        MemberAttributeConfig memberConfig = new MemberAttributeConfig();
        memberConfig.setIntAttribute("cores", machine.cores());
        for (String skill : machine.skills())
            memberConfig.setBooleanAttribute(skill, true);
        memberConfig.setStringAttribute("hostname", machine.hostname());
        memberConfig.setStringAttribute("ip", machine.ip());

        config.setMemberAttributeConfig(memberConfig);

        // Setup network configuration
        /*List<String> addresses = machines.stream().map(::hostname).collect(Collectors.toList());
        Config cfg = new Config();
        NetworkConfig network = cfg.getNetworkConfig();
        network.setReuseAddress(true);

        JoinConfig join = network.getJoin();
        join.getMulticastConfig().setEnabled(false);
        TcpIpConfig ipConfig = join.getTcpIpConfig().setEnabled(true);
        for (String address : addresses)
            ipConfig = ipConfig.addMember(address);
        InterfacesConfig interfaces = network.getInterfaces().setEnabled(true);
        for (String address : addresses)
            interfaces = interfaces.addInterface(address);*/

        Hazelcast.newHazelcastInstance(config);
    }
}
