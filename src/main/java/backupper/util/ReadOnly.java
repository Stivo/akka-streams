package backupper.util;

import org.bouncycastle.crypto.digests.Blake2bDigest;

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

public class ReadOnly {

    static ThreadLocal<byte[]> buffers = new ThreadLocal<>();
    static ThreadLocal<Blake2bDigest> digests = new ThreadLocal<>();

    static AtomicLong readBytes = new AtomicLong();
    static AtomicLong readFiles = new AtomicLong();
    static AtomicLong byteArray = new AtomicLong();

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        Files.walk(Paths.get(args[0]))
                .parallel()
                .filter(Files::isRegularFile)
                .filter(e -> !e.toAbsolutePath().toString().contains("/.git/"))
                .forEach(e -> {
                    Blake2bDigest blake2bDigest = digests.get();
//                    if (blake2bDigest == null) {
//                        blake2bDigest = new Blake2bDigest(160);
//                        digests.set(blake2bDigest);
//                    }
//                    blake2bDigest.reset();
                    MessageDigest md = null;
                    try {
                        md = MessageDigest.getInstance("MD5");
                    } catch (NoSuchAlgorithmException e1) {
                        e1.printStackTrace();
                    }
                    ArrayList<byte[]> bytes1 = new ArrayList<>();
//                    try {
//                        RandomAccessFile r = new RandomAccessFile(e.toFile(), "r");
//                        MappedByteBuffer map = r.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, e.toFile().length());
//                        int limit = Math.min(64 * 1024, map.remaining());
//                        byte[] bytes = new byte[limit];
//                        map.get(bytes);
//                        md.update(bytes);
//                    } catch (IOException e1) {
//                        e1.printStackTrace();
//                    }
                    try (FileInputStream fileInputStream = new FileInputStream(e.toFile())) {
//                        byte[] bytes = buffers.get();
//                        if (bytes == null) {
//                            bytes = new byte[4 * 1024];
//                            buffers.set(bytes);
//                        }
                        while (fileInputStream.available() > 0) {
                            byte[] bytes = new byte[4 * 1024];
                            int read = fileInputStream.read(bytes);
                            md.update(bytes, 0, read);
                            readBytes.addAndGet(read);
                            bytes1.add(bytes);
                        }
                        byteArray.addAndGet(bytes1.size());
//                        System.out.println("Got " + bytes1.size() + " byte arrays");
                    } catch (Exception e1) {
                        e1.printStackTrace();
                    }
                    byte[] bytes = new byte[512];
                    md.digest();
                    readFiles.incrementAndGet();
                });
        long stop = System.currentTimeMillis();
        System.out.println("Took " + (stop - start) + " ms to read " + (readBytes.get()) + " bytes in " + (readFiles.get()) + " files and allocated " + (byteArray.get()) + " byte arrays");
    }
}
