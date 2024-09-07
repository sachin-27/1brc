package dev.morling.onebrc;

import java.io.*;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.Map;
import java.util.DoubleSummaryStatistics;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public class CalculateAverageOptimised {

    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws InterruptedException, IOException {
        var clockStart = System.currentTimeMillis();
        File file = new File(FILE);
        long length = file.length();
        int chunkCount = Runtime.getRuntime().availableProcessors() * 2;
        var results = new Measurement[chunkCount][];
        var chunkOffsets = new long[chunkCount];
        try (var raf = new RandomAccessFile(file, "r")) {

            for(int i=1; i<chunkOffsets.length; i++) {
                var start = (length * i) / chunkOffsets.length;
                raf.seek(start);
                while(raf.read() != (byte)'\n') {
                    // do nothing
                }
                start = raf.getFilePointer();
                chunkOffsets[i] = start;
            }

            var mappedFile = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, length, Arena.global());
            var threads = new Thread[chunkCount];
            for(int i=0; i<chunkOffsets.length; i++) {
                long chunkStart = chunkOffsets[i];
                long chunkEnd = (i != chunkOffsets.length-1) ? chunkOffsets[i+1] : length;
                threads[i] = new Thread(new ChunkProcessor(mappedFile.asSlice(chunkStart, chunkEnd-chunkStart), results, i));
            }

            for(var thread : threads) {
                thread.start();
            }
            for(var thread : threads) {
                thread.join();
            }
        }

        var finalMap = new TreeMap<String, Measurement>();
        for(var measurementArray : results) {
            for(var measurement : measurementArray) {
                finalMap.merge(measurement.station, measurement, (v1, v2) -> {
                   v1.count += v2.count;
                   v1.sum += v2.sum;
                   v1.max = Math.max(v1.max, v2.max);
                   v1.min = Math.min(v1.min, v2.min);
                   return v1;
                });
            }
        }
        System.out.println(finalMap);

        System.err.format("Took %,d ms\n", System.currentTimeMillis() - clockStart);
    }
    
}

class ChunkProcessor implements Runnable {

    private MemorySegment chunk;
    private Measurement[][] results;
    private int myIndex;
    private Map<String, Measurement> map;

    public ChunkProcessor(MemorySegment chunk, Measurement[][] results, int index) {
        this.chunk = chunk;
        this.results = results;
        this.myIndex = index;
        this.map = new HashMap<>();
    }

    @Override
    public void run() {
        for(var cursor=0L; cursor<chunk.byteSize();) {
            var semiColonPos = findByte(cursor, ';');
            String station = stringAt(cursor, semiColonPos);

            var newLinePos = findByte(semiColonPos+1, '\n');
            var temp = parseTemp(semiColonPos+1, newLinePos);
            var stats = map.computeIfAbsent(station, k -> new Measurement(k));
            stats.max = Math.max(temp, stats.max);
            stats.min = Math.min(temp, stats.min);
            stats.sum = stats.sum + temp;
            stats.count = stats.count + 1;
            cursor = newLinePos+1;
        }

        results[myIndex] = map.values().toArray(Measurement[]::new);
    }

    private int parseTemp(long start, long end) {
        int sign = 1;
        if(chunk.get(JAVA_BYTE, start) == '-') {
            sign = -1;
            start++;
        }

        int total = 0;
        while(start != end) {
            if(chunk.get(JAVA_BYTE, start) == '.') {
                start++;
                continue;
            }

            total *= 10;
            total += (chunk.get(JAVA_BYTE, start) - '0');
            start++;
        }

        return total * sign;
    }

    private long findByte(long cursor, int b) {
        for(var i=cursor; i<chunk.byteSize(); i++) {
            if(chunk.get(JAVA_BYTE, i) == b) {
                return i;
            }
        }
        throw new RuntimeException(((char)b) + " not found");
    }

    private String stringAt(long start, long end) {
        return new String(chunk.asSlice(start, end-start).toArray(JAVA_BYTE), StandardCharsets.UTF_8);
    }
}

class Measurement {
    String station;
    long sum;
    int count;
    int max;
    int min;

    public Measurement(String station) {
        this.station = station;
    }

    public String toString() {
        return String.format("%.1f/%.1f/%.1f", min/10.0, (sum/(count * 10.0)), max/10.0);
    }
}
