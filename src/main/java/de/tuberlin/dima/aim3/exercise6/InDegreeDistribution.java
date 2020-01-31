package de.tuberlin.dima.aim3.exercise6;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;

/**
 * compute the in-degree distribution.
 * Save  the  output  as in-degree_dist.csv.
 */

public class InDegreeDistribution {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Long, NullValue>> vertices = env.readTextFile(Config.pathToAllVertices())
                .flatMap(new DegreeDistribution.VertexReader());

        DataSet<Tuple3<Long, Long, Boolean>> edges = env.readTextFile(Config.pathToSlashdotZoo())
                .flatMap(new DegreeDistribution.EdgeReader());

        Graph<Long, NullValue, Boolean> graph = Graph.fromTupleDataSet(vertices,edges,env);

        DataSet<Long> totVertices = graph.getVertices().reduceGroup(new DegreeDistribution.CountVertices());

        //Calculate in-degrees distribution
        graph.inDegrees()
                .groupBy(1)
                .reduceGroup(new DegreeDistribution.DistributionElement())
                .withBroadcastSet(totVertices, "totVertices")
                .writeAsCsv(Config.outputPath()+"in-degree_dist.csv", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        env.execute("in-degree distribution");
    }
}
