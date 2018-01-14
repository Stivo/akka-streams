package rxjava;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.parallel.ParallelFlowable;
import io.reactivex.schedulers.Schedulers;

import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

public class RxjavaTest {
    public static void main(String[] args) throws Exception {
        System.setProperty("rx2.computation-threads", "8");
        Iterable<Integer> naturals = IntStream.iterate(0, i -> i + 1)::iterator;
        File file = new File("backup/metadata.json");
        Flowable<ByteBuffer> generate = Flowable.generate(() -> new FileInputStream(file), (fis, emitter) -> {
            if (fis.available() > 0) {
                byte[] buffer = new byte[1000];
                int read = fis.read(buffer);
                emitter.onNext(ByteBuffer.wrap(buffer, 0, read));
            } else {
                fis.close();
                emitter.onComplete();
            }
        });

        CompletableFuture<Boolean> future = new CompletableFuture<>();

        ParallelFlowable<State> flow = generate.zipWith(naturals, (s, i) -> {
            State state = new State();
            state.byteBuffer = s;
            state.number = i;
            return state;
        }).flatMap(x -> Observable.just(x).toFlowable(BackpressureStrategy.BUFFER))
                .parallel(10)
                .runOn(Schedulers.io(), 100);
        flow = flow
                .map(state -> {
                    MessageDigest md5 = MessageDigest.getInstance("MD5");
                    System.out.println("Started computing hash " + state.number);
                    if (state.number % 2 == 0) {
                        Thread.sleep(2000);
                    }
                    System.out.println("Computing hash for " + state.number);
                    int position = state.byteBuffer.position();
                    md5.update(state.byteBuffer);
                    state.byteBuffer.position(position);
                    byte[] hash = md5.digest();
                    state.hash = hash;
                    return state;
                });

        flow.toSortedList(Comparator.comparingInt(e -> e.number)).subscribe(e -> System.out.println(e), e -> e.printStackTrace(), () -> future.complete(true));
        future.join();

    }

    private static Integer intenseCalculation(Integer i) {
        System.out.println("Hello from "+i);
        return i;
    }
}

class State {
    public ByteBuffer byteBuffer;
    public int number;
    public byte[] hash;

    @Override
    public String toString() {
        return "State{" +
                ", number=" + number +
                ", hash=" + Arrays.toString(hash) +
                '}';
    }
}
