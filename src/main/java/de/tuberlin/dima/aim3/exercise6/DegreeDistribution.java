/**
 * AIM3 - Scalable Data Mining -  course work
 * Copyright (C) 2018  Pandu Wicaksono
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package de.tuberlin.dima.aim3.exercise6;

import com.google.common.collect.Iterables;
import org.apache.flink.api.common.accumulators.AverageAccumulator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.directed.VertexInDegree;
import org.apache.flink.types.NullValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.regex.Pattern;

public class DegreeDistribution {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        /* Vertices */
        DataSet<Tuple2<Long, NullValue>> vertices = env.readTextFile(Config.pathToAllVertices())
                .flatMap(new VertexReader());

        /* Edges */
        DataSet<Tuple3<Long, Long, Boolean>> edges = env.readTextFile(Config.pathToSlashdotZoo())
                .flatMap(new EdgeReader());

        /* Graph */
        Graph<Long, NullValue, Boolean> graph = Graph.fromTupleDataSet(vertices,edges,env);

        DataSet<Tuple2<Long,LongValue>> degrees = graph.getDegrees();

        // Calculates the total number of vertices (to be used below)
        DataSet<Long> totVertices = graph.getVertices().reduceGroup(new CountVertices());

        // Calculates the degree distribution
        DataSet<Tuple2<Long, Double>> degreeDistribution =
                degrees.groupBy(1).reduceGroup(new DistributionElement())
                        .withBroadcastSet(totVertices, "totVertices");

        /* Write to file */
        degreeDistribution.writeAsCsv(Config.outputPath()+ "degree_dist.csv", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

    	/* Calculate the average degree and write to file */
        graph.getDegrees()
                .reduceGroup(new CalculateAverage())
                .writeAsCsv(Config.outputPath()+"avg_degree.csv", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

		/*Calculate the max degree and write to file */
        graph.getDegrees()
                .maxBy(1)
                .writeAsCsv(Config.outputPath()+"max_degree.csv", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        env.execute();
    }

    public static class VertexReader implements FlatMapFunction<String, Tuple2<Long, NullValue>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<Long, NullValue>> collector) throws Exception {
            collector.collect(new Tuple2<Long, NullValue>(Long.parseLong(s), new NullValue()));
        }
    }

    public static class EdgeReader implements FlatMapFunction<String, Tuple3<Long, Long, Boolean>> {
        private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

        @Override
        public void flatMap(String s, Collector<Tuple3<Long, Long, Boolean>> collector) throws Exception {
            if (!s.startsWith("%")) {
                String[] tokens = SEPARATOR.split(s);

                long source = Long.parseLong(tokens[0]);
                long target = Long.parseLong(tokens[1]);
                boolean isFriend = "+1".equals(tokens[2]);

                collector.collect(new Tuple3<Long, Long, Boolean>(source, target, isFriend));
            }
        }
    }

    public static class CountVertices implements GroupReduceFunction<Vertex<Long,NullValue>, Long> {
        @Override
        public void reduce(Iterable<Vertex<Long,NullValue>> vertices, Collector<Long> collector) throws Exception {
            collector.collect(new Long(Iterables.size(vertices)));
        }
    }

    public static class DistributionElement extends RichGroupReduceFunction<Tuple2<Long, LongValue>, Tuple2<Long, Double>> {
        private long totVertices;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            totVertices = getRuntimeContext().<Long>getBroadcastVariable("totVertices").get(0);
//            System.out.println("totVertices: " + totVertices);
        }

        @Override
        public void reduce(Iterable<Tuple2<Long, LongValue>> verticesWithDegree, Collector<Tuple2<Long, Double>> collector) throws Exception {
            Iterator<Tuple2<Long, LongValue>> iterator = verticesWithDegree.iterator();
            Long degree = iterator.next().f1.getValue();
            long count = 1L;
            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }
            collector.collect(new Tuple2<Long, Double>(degree, (double) count / totVertices));
        }
    }

    private static class CalculateAverage implements GroupReduceFunction<Tuple2<Long, LongValue>, Tuple1<Double>> {

        @Override
        public void reduce(Iterable<Tuple2<Long, LongValue>> iterable, Collector<Tuple1<Double>> collector) throws Exception {
            long count = 0L;
            long sum = 0L;
            for (Tuple2<Long, LongValue> rec : iterable){
                count++;
                sum = sum + rec.f1.getValue();
            }
            collector.collect(new Tuple1<>( new Double(sum/count)));
        }
    }

}
