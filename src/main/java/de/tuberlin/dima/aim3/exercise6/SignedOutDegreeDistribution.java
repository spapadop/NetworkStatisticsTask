package de.tuberlin.dima.aim3.exercise6;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;

public class SignedOutDegreeDistribution {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Long, NullValue>> vertices = env.readTextFile(Config.pathToAllVertices())
                .flatMap(new DegreeDistribution.VertexReader());

        DataSet<Tuple3<Long, Long, Boolean>> edges = env.readTextFile(Config.pathToSlashdotZoo())
                .flatMap(new DegreeDistribution.EdgeReader());

        Graph<Long, NullValue, Boolean> graph = Graph.fromTupleDataSet(vertices,edges,env);

        DataSet<Long> totVertices = graph.getVertices().reduceGroup(new DegreeDistribution.CountVertices());

        // Compute the out-degree distributions for friend & foe nodes,individually.
        // Friends
        graph.filterOnEdges(t-> t.f2)
                .outDegrees()
                .groupBy(1)
                .reduceGroup(new DegreeDistribution.DistributionElement())
                .withBroadcastSet(totVertices, "totVertices")
                .writeAsCsv(Config.outputPath()+"out-degree_friend_dist.csv", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        // Foes
        graph.filterOnEdges(t-> !t.f2)
                .outDegrees()
                .groupBy(1)
                .reduceGroup(new DegreeDistribution.DistributionElement())
                .withBroadcastSet(totVertices, "totVertices")
                .writeAsCsv(Config.outputPath()+"out-degree_foe_dist.csv", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        env.execute("out-degree distributions for friend and foes nodes individually");
    }
}
