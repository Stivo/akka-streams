package rxjava;

import akka.util.ByteString;
import backupper.BackupStreams;
import backupper.model.Block;
import backupper.model.BlockId;
import backupper.model.FileDescription;
import com.github.davidmoten.rx2.Bytes;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.flowables.ConnectableFlowable;
import io.reactivex.parallel.ParallelFlowable;
import io.reactivex.schedulers.Schedulers;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.io.File;
import java.security.MessageDigest;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

public class RxjavaTest {
    public static void main(String[] args) throws Exception {
        System.setProperty("rx2.computation-threads", "8");
        Iterable<Integer> naturals = IntStream.iterate(0, i -> i + 1)::iterator;
        File file = new File("backup/metadata.json");
        Flowable<byte[]> from = Bytes.from(file, 1000);

        CompletableFuture<Boolean> future1 = new CompletableFuture<>();
        CompletableFuture<Boolean> future2 = new CompletableFuture<>();

        Flowable<Block> blockFlowable = from.zipWith(naturals, (s, i) -> {
            FileDescription fileDescription = new FileDescription(file);
            BlockId blockId = new BlockId(fileDescription, i);
            Block block = new Block(blockId, ByteString.fromArray(s), null);
//            State state = new State();
//            state.byteBuffer = ByteBuffer.wrap(s);
//            state.number = i;
//            state.file = file;
//            return state;
            return block;
        });
        ParallelFlowable<Block> parallelBlocks = blockFlowable.flatMap(x -> Observable.just(x).toFlowable(BackpressureStrategy.BUFFER))
                .parallel(10)
                .runOn(Schedulers.computation(), 100);

        LoggingSubscriber<Block> blockLoggingSubscriber = new LoggingSubscriber<>();
        Subscriber[] subscribers = {blockLoggingSubscriber};
//        parallelBlocks.subscribe(subscribers);
//        parallelBlocks.sequential().forEach(e -> System.out.println(e));
        ConnectableFlowable<Block> md51 = parallelBlocks
                .flatMap(block -> {
                    MessageDigest md5 = MessageDigest.getInstance("MD5");
                    System.out.println("Computing hash for " + block.blockId());
                    byte[] digest = md5.digest(block.content().toArray());
                    ByteString byteString = ByteString.fromArray(digest);
                    Block block1 = new Block(block.blockId(), block.content(), byteString);
                    Future<Block> booleanFuture = BackupStreams.blockStorageActor().hasAlreadyJava(block1);

                    return Observable.fromFuture(booleanFuture).toFlowable(BackpressureStrategy.BUFFER);
                }).map(e -> {
                    if (e.isAlreadySaved()) {
                        e.content_$eq(ByteString.empty());
                    }
                    return e;
                }).sequential(100).publish();
//        md51.sequential().forEach()
//        ParallelFlowable<Block> md511 = md51;
//        md511.subscribe(subscribers);
//
        md51.toSortedList(Comparator.comparingInt(b -> b.blockId().blockNr()))
                .subscribe(e -> System.out.println("sorted " + e));
        md51.subscribe(e -> System.out.println("asdfasdf" + e));
        md51.connect();
//        md51.sequential().forEach(e -> System.out.println(e));
//        hashedBlocks.parallel().filter(e -> e.isAlreadySaved()).map(e -> {
//                    System.out.println("Should save " + e.blockId());
//                    return null;
//                }
//        ).reduce(() -> 1, (e, f) -> e).runOn(Schedulers.computation()).sequential().forEach( e-> future1.complete(true));

//        Flowable<List<Block>> map = hashedBlocks.parallel().toSortedList(Comparator.comparingInt(e -> e.blockId().blockNr()))
//                .map(e -> {
//                    System.out.println("Got hashlist for " + e.size() + " blocks");
//                    future2.complete(true);
//                    return e;
//                });
//
//        flowToSave.runOn(Schedulers.computation())
//                .sequential()
//                .subscribe(e -> System.out.println(e+""), e -> e.printStackTrace(), () -> future1.complete(true));
//        map
//                .subscribe(e -> System.out.println(e+""), e -> e.printStackTrace(), () -> future2.complete(true));

        System.out.println("Waiting for future 1");
        future1.join();
        System.out.println("Waiting for future 2");
//        future2.join();

    }

    private static Integer intenseCalculation(Integer i) {
        System.out.println("Hello from " + i);
        return i;
    }
}
//
//class State {
//    public File file;
//    public ByteBuffer byteBuffer;
//    public int number;
//    public byte[] hash;
//
//    @Override
//    public String toString() {
//        return "State{" +
//                ", number=" + number +
//                ", hash=" + Arrays.toString(hash) +
//                '}';
//    }
//}

class LoggingSubscriber<T> implements Subscriber<T> {

    @Override
    public void onSubscribe(Subscription s) {

    }

    @Override
    public void onNext(T o) {
        System.out.println(o);
    }

    @Override
    public void onError(Throwable t) {

    }

    @Override
    public void onComplete() {

    }
}