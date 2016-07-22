package hazelcast_distribution;

import com.hazelcast.core.*;
import org.javatuples.Pair;
import org.jgrapht.traverse.BreadthFirstIterator;
import org.rhea_core.Stream;
import org.rhea_core.annotations.PlacementConstraint;
import org.rhea_core.annotations.StrategyInfo;
import org.rhea_core.distribution.DistributionStrategy;
import org.rhea_core.distribution.graph.DistributedGraph;
import org.rhea_core.distribution.graph.TopicEdge;
import org.rhea_core.evaluation.EvaluationStrategy;
import org.rhea_core.internal.expressions.MultipleInputExpr;
import org.rhea_core.internal.expressions.NoInputExpr;
import org.rhea_core.internal.expressions.SingleInputExpr;
import org.rhea_core.internal.expressions.Transformer;
import org.rhea_core.internal.expressions.creation.FromSource;
import org.rhea_core.internal.graph.FlowGraph;
import org.rhea_core.internal.output.MultipleOutput;
import org.rhea_core.internal.output.Output;
import org.rhea_core.internal.output.SinkOutput;
import org.rhea_core.network.Machine;
import org.rhea_core.util.functions.Func0;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Function;
import java.util.stream.Collectors;


public class HazelcastDistributionStrategy implements DistributionStrategy {
    private final HazelcastInstance hazelcast;
    private final List<Func0<EvaluationStrategy>> strategies;

    // for testing purposes
    public HazelcastDistributionStrategy(List<Func0<EvaluationStrategy>> strategies) {
        this.strategies = strategies;
        hazelcast = Hazelcast.newHazelcastInstance();
    }

    public HazelcastDistributionStrategy(HazelcastInstance hazelcast, List<Machine> machines, List<Func0<EvaluationStrategy>> strategies) {
        this.strategies = strategies;
        this.hazelcast = hazelcast;
    }

    @Override
    public void distribute(Stream stream, Output output) {

        // TODO Task Fusion { iff (stream.getGraph().size() > desiredGranularity) }

        DistributedGraph graph = new DistributedGraph(stream.getGraph(), this::newTopic);

        Queue<HazelcastTask> tasks = new LinkedList<>();

        // Run output node first
        HazelcastTopic result = newTopic();
        tasks.add(createTask(Stream.from(result), output));

        // Then run each graph vertex as an individual node (reverse BFS)
        Set<Transformer> checked = new HashSet<>();
        Stack<Transformer> stack = new Stack<>();
        for (Transformer root : graph.getRoots())
            new BreadthFirstIterator<>(graph, root).forEachRemaining(stack::push);
        while (!stack.empty()) {
            Transformer toExecute = stack.pop();
            if (checked.contains(toExecute)) continue;

            Set<TopicEdge> inputs = graph.incomingEdgesOf(toExecute);

            FlowGraph innerGraph = new FlowGraph();
            if (toExecute instanceof NoInputExpr) {
                assert inputs.isEmpty();
                // 0 input
                innerGraph.addConnectVertex(toExecute);
            } else if (toExecute instanceof SingleInputExpr) {
                assert inputs.size() == 1;
                // 1 input
                HazelcastTopic input = (HazelcastTopic) inputs.iterator().next().getTopic();
                Transformer toAdd = new FromSource(input.clone());
                innerGraph.addConnectVertex(toAdd);
                innerGraph.attach(toExecute);
            } else if (toExecute instanceof MultipleInputExpr) {
                assert inputs.size() > 1;
                // N inputs
                innerGraph.setConnectNodes(inputs.stream()
                        .map(edge -> new FromSource(edge.getTopic()/*.clone()*/))
                        .collect(Collectors.toList()));
                innerGraph.attachMulti(toExecute);
            }

            // Set outputs according to graph connections
            Set<TopicEdge> outputs = graph.outgoingEdgesOf(toExecute);
            List<Output> list = new ArrayList<>();
            if (toExecute.equals(graph.toConnect))
                list.add(new SinkOutput(result.clone()));
            list.addAll(outputs.stream()
                    .map(TopicEdge::getTopic)
                    .map(t -> (HazelcastTopic) t)
                    .map((Function<HazelcastTopic, SinkOutput<Object>>) sink -> new SinkOutput(sink))
                    .collect(Collectors.toList()));
            Output outputToExecute = (list.size() == 1) ? list.get(0) : new MultipleOutput(list);

            // Schedule for execution
            tasks.add(createTask(new Stream(innerGraph), outputToExecute));

            checked.add(toExecute);
        }

        // Execute
        submit(tasks);
    }

    private void executeOn(Runnable task, String ip) {
        for (Member member : hazelcast.getCluster().getMembers()) {
            String host = member.getAddress().getHost(); // TODO find proper IP
            if (host.equals(ip))
                hazelcast.getExecutorService("ex").executeOnMember(task, member);
        }
    }

    private void execute(HazelcastTask task, IExecutorService executorService) {
        executorService.execute(task, member -> {
            for (String skill : task.getRequiredSkills()) {
                System.out.println("Skill: " + skill);
                if (!member.getAttributes().containsKey(skill))
                    return false;
            }
            return true;
        });
    }

    /**
     * Executes the given {@link HazelcastTask}s on the current cluster.
     * @param tasks the {@link HazelcastTask}s to execute
     */
    private void submit(Queue<? extends HazelcastTask> tasks) {

        IExecutorService executorService = hazelcast.getExecutorService("ex");
        Set<Member> members = hazelcast.getCluster().getMembers();

        // Print each machine of the cluster
        members.stream().forEach(System.out::println);

        // TODO Placement optimization
        // Profile network cost (and operator cost) to determine optimal placement.

        // Execute tasks
        HazelcastTask task;
        while ((task = tasks.poll()) != null)
            execute(task, executorService);
    }

    private HazelcastTask createTask(Stream stream, Output output) {
        Set<String> constraints = new HashSet<>();
        for (Transformer node : stream.getGraph().vertices()) {
            Class<?> clazz = node.getClass();
            if (clazz.isAnnotationPresent(PlacementConstraint.class)) {
                PlacementConstraint annotation = clazz.getAnnotation(PlacementConstraint.class);
                constraints.add(annotation.constraint());
            }
        }

        assert constraints.size() <= 1;
        String constaint = constraints.isEmpty() ? null : constraints.iterator().next();

        SortedSet<Pair<StrategyInfo, Func0<EvaluationStrategy>>> strategiesImpl = new ConcurrentSkipListSet<>((e1, e2) -> {
            int p1 = e1.getValue0().priority();
            int p2 = e2.getValue0().priority();
            return (p1 == p2) ? 0 : ((p1 > p2) ? -1 : 1);
        });

        for (Func0<EvaluationStrategy> s : strategies) {
            StrategyInfo info = s.call().getClass().getAnnotation(StrategyInfo.class);
            String name = info.name();
            if ((constaint == null) | name.equals(constaint))
                strategiesImpl.add(new Pair<>(info, s));
        }

        return new HazelcastTask(strategiesImpl.first().getValue1(), stream, output);
    }

    /**
     * Generators
     */
    private String nodePrefix = "t";
    private int topicCounter = 0;
    private HazelcastTopic newTopic() {
        return new HazelcastTopic(nodePrefix + "/" + Integer.toString(topicCounter++));
    }
}

