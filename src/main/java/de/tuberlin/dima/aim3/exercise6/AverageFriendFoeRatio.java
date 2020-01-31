package de.tuberlin.dima.aim3.exercise6;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

/**
 * compute the average ratio of friends to foes per vertex in the network.
 * Save the result as avg_ratio.txt. Note: Ignore vertices that only have friends or foes.
 */
public class AverageFriendFoeRatio {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Long, NullValue>> vertices = env.readTextFile(Config.pathToAllVertices())
                .flatMap(new DegreeDistribution.VertexReader());

        DataSet<Tuple3<Long, Long, Boolean>> edges = env.readTextFile(Config.pathToSlashdotZoo())
                .flatMap(new DegreeDistribution.EdgeReader());

        Graph<Long, NullValue, Boolean> graph = Graph.fromTupleDataSet(vertices,edges,env);

        // Average Friends to Foes ratio
        graph.getEdgesAsTuple3()
                .groupBy(0)
                .reduceGroup(new CalcAverageFriendFoeRatio())
                .reduceGroup(new CalcFinalAverage())
                .writeAsCsv(Config.outputPath()+"avg_ratio.csv", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        env.execute("average ratio of friends to foes per vertex in the network");
    }


    private static class CalcAverageFriendFoeRatio extends RichGroupReduceFunction<Tuple3<Long, Long, Boolean>, Tuple1<Double>> {

        @Override
        public void reduce(Iterable<Tuple3<Long, Long, Boolean>> iterable, Collector<Tuple1<Double>> collector) throws Exception {
            long friends=0;
            long foes=0;
            for (Tuple3<Long, Long, Boolean> rec : iterable){
                if(rec.f2)
                    friends++;
                else
                    foes++;
            }
            if(friends != 0 && foes != 0) { // ignore nodes that do not have either friends or foes
                Double ratio = (double) friends / foes;
                collector.collect(new Tuple1<>(ratio));
            }
        }
    }

    private static class CalcFinalAverage implements GroupReduceFunction<Tuple1<Double>, Tuple1<Double>> {
        @Override
        public void reduce(Iterable<Tuple1<Double>> iterable, Collector<Tuple1<Double>> collector) throws Exception {
            int count= 0;
            double sum = 0;
            for (Tuple1<Double> rec : iterable){
                count++;
                sum +=rec.f0;
            }
            collector.collect(new Tuple1<>(sum/count));
        }
    }
}
