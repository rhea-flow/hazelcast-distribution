package hazelcast_distribution;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.topic.ReliableMessageListener;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.rhea_core.Stream;
import org.rhea_core.internal.Notification;
import org.rhea_core.serialization.DefaultSerializer;
import org.rhea_core.internal.output.Output;
import org.rhea_core.io.AbstractTopic;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Orestis Melkonian
 */
public class HazelcastTopic<T> extends AbstractTopic<T, byte[], HazelcastInstance> {

    private ITopic<byte[]> topic;

    public HazelcastTopic(String name) {
        super(name, new DefaultSerializer());
    }

    @Override
    public void setClient(HazelcastInstance client) {
        this.client = client;
        topic = client.getReliableTopic(name);
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        topic.addMessageListener(new ReliableMessageListener<byte[]>() {
            long sequence = 0;

            @Override
            public long retrieveInitialSequence() {
                return (sequence == 0) ? 0 : sequence + 1;
            }

            @Override
            public void storeSequence(long sequence) {
                this.sequence = sequence;
            }

            @Override
            public boolean isLossTolerant() {
                return false;
            }

            @Override
            public boolean isTerminal(Throwable failure) {
                return false;
            }

            @Override
            public void onMessage(Message<byte[]> message) {
                byte[] msg = message.getMessageObject();
                Notification<T> notification = serializer.deserialize(msg);
                switch (notification.getKind()) {
                    case OnNext:
                        if (Stream.DEBUG)
                            System.out.println(name() + ": Recv\t" + notification.getValue());
                        s.onNext(notification.getValue());
                        break;
                    case OnError:
                        s.onError(notification.getThrowable());
                        break;
                    case OnCompleted:
                        if (Stream.DEBUG)
                            System.out.println(name() + ": Recv\tComplete");
                        s.onComplete();
                        break;
                    default:
                }
            }
        });
    }

    @Override
    public void onSubscribe(Subscription s) {
        s.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(T t) {
        if (Stream.DEBUG) System.out.println(name() + ": Send\t" + t);
        publish(Notification.createOnNext(t));
    }

    @Override
    public void onError(Throwable t) {
        publish(Notification.createOnError(t));
    }

    @Override
    public void onComplete() {
        if (Stream.DEBUG) System.out.println(name() + ": Send\tComplete");
        publish(Notification.createOnCompleted());
    }

    private void publish(Notification notification) {
        topic.publish(serializer.serialize(notification));
    }

    @Override
    public HazelcastTopic<T> clone() {
        return new HazelcastTopic<>(name);
    }

    public static List<HazelcastTopic> extract(Stream stream, Output output) {
        return AbstractTopic.extractAll(stream, output)
                .stream()
                .filter(topic -> topic instanceof HazelcastTopic)
                .map(topic -> ((HazelcastTopic) topic))
                .collect(Collectors.toList());
    }}
