package hazelcast_distribution;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import org.rhea_core.Stream;
import org.rhea_core.distribution.annotations.RequiredSkills;
import org.rhea_core.evaluation.EvaluationStrategy;
import org.rhea_core.internal.expressions.Transformer;
import org.rhea_core.internal.output.Output;
import org.rhea_core.util.functions.Func0;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.util.*;

public class HazelcastTask implements Runnable, Serializable, HazelcastInstanceAware {

    private HazelcastInstance hazelcast;

    protected Func0<EvaluationStrategy> strategyGenerator;
    protected Stream stream;
    protected Output output;
    private Set<String> requiredSkills;

    public HazelcastTask(Func0<EvaluationStrategy> strategyGenerator, Stream stream, Output output) {
        this.strategyGenerator = strategyGenerator;
        this.stream = stream;
        this.output = output;
    }

    public Set<String> getRequiredSkills() {
        Set<String> skills = new HashSet<>();

        for (Transformer node : stream.getGraph().vertices()) {
            Class clazz = node.getClass();
            if (clazz.isAnnotationPresent(RequiredSkills.class)) {
                RequiredSkills annotation = (RequiredSkills) clazz.getAnnotation(RequiredSkills.class);
                Collections.addAll(skills, annotation.requiredSkills());
            }
        }

        return skills;
    }

    public Output getOutput() {
        return output;
    }

    public Stream getStream() {
        return stream;
    }

    public Func0<EvaluationStrategy> getStrategyGenerator() {
        return strategyGenerator;
    }

    public HazelcastInstance getHazelcast() {
        return hazelcast;
    }

    @Override
    public void run() {
        if (Stream.DEBUG) System.out.println(this);

        for (HazelcastTopic topic : HazelcastTopic.extract(stream, output))
            topic.setClient(hazelcast);

        strategyGenerator.call().evaluate(stream, output);
    }

    @Override
    public String toString() {
        return "\n\n======================== "
                + instanceIP()
//                + ManagementFactory.getRuntimeMXBean().getName() + "@" + Thread.currentThread().getId()
                + " ========================"
                + "\n" + stream.getGraph()
                + "\n\t===>\t" + output + "\n"
                + "\n==================================================\"\n\n";
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcast = hazelcastInstance;
    }

    private String instanceIP() {
        String str = hazelcast.toString();
        return (str.substring(str.indexOf("Address") + 7, str.length() - 1)).replace("[","").replace("]","");
    }
}
