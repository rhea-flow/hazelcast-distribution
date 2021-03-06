import hazelcast_distribution.HazelcastDistributionStrategy;
import org.junit.Test;
import org.rhea_core.Stream;
import rx_eval.RxjavaEvaluationStrategy;
import test_data.utilities.Threads;

import java.util.Collections;

/**
 * @author Orestis Melkonian
 */
public class Adhoc {

    @Test
    public void adhoc() {
        Stream.distributionStrategy = new HazelcastDistributionStrategy(Collections.singletonList(RxjavaEvaluationStrategy::new));

        Stream.nat().print();

        Threads.sleep();
    }

}
