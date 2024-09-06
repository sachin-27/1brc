package dev.morling.onebrc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.Map;
import java.util.DoubleSummaryStatistics;

public class CalculateAverageOptimised {

    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(FILE));
        var allStats = br.lines().parallel().collect(Collectors.groupingBy(line -> line.substring(0, line.indexOf(';')),
        Collectors.summarizingDouble(line -> Double.parseDouble(line.substring(line.indexOf(';')+1, line.length())))));

        TreeMap<String, String> map = new TreeMap<>();
        for(Map.Entry<String, DoubleSummaryStatistics> e : allStats.entrySet()) {
            var stats = e.getValue();
            map.put(e.getKey(), String.format("%.1f/%.1f/%.1f", stats.getMin(), stats.getAverage(), stats.getMax()));
        }

        System.out.println(map);
    }
    
}
