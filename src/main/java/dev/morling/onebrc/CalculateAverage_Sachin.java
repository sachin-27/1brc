package dev.morling.onebrc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;

public class CalculateAverage_Sachin {

    private static final String FILE = "./measurements.txt";
    public static void main(String[] args) throws IOException {
        Map<String, Measurement> map = new HashMap<>();
        BufferedReader br = new BufferedReader(new FileReader(FILE));
        String line;
        while((line = br.readLine()) != null) {
            String values[] = line.split(";");
            double tempVal = Float.parseFloat(values[1]);
            if(!map.containsKey(values[0])) {
                map.put(values[0], new Measurement(tempVal));
            } else {
                Measurement m = map.get(values[0]);
                m.count++;
                m.maxTemp = Math.max(tempVal, m.maxTemp);
                m.minTemp = Math.min(tempVal, m.minTemp);
                m.avg += tempVal;
            }
        }

        List<Pair> l = new ArrayList<>();
        for(Map.Entry<String, Measurement> e : map.entrySet()) {
            l.add(new Pair(e.getKey(), e.getValue()));
        }

        Collections.sort(l);

        StringBuilder s = new StringBuilder();
        s.append("{");
        for(Pair p : l) {
            p.m.avg /= p.m.count;
            s.append(String.format("%s=%.1f/%.1f/%.1f, ", p.location, p.m.minTemp, p.m.avg, p.m.maxTemp));
        }

        s.deleteCharAt(s.length()-1);
        s.deleteCharAt(s.length()-1);
        s.append("}");
        System.out.println(s);
    }

}

class Measurements {
    long count;
    double avg;
    double minTemp;
    double maxTemp;

    public Measurements() {
        count = 0;
        avg = 0;
        minTemp = 1000;
        maxTemp = -1000;
    }

    public Measurements(double temp) {
        count = 1;
        avg = temp;
        minTemp = temp;
        maxTemp = temp;
    }
}

class Pair implements Comparable<Pair> {
    Measurement m;
    String location;

    public Pair(String location, Measurement m) {
        this.location = location;
        this.m = m;
    }

    public int compareTo(Pair p) {
        return this.location.compareTo(p.location);
    }
}
