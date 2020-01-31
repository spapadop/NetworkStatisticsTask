package de.tuberlin.dima.aim3.exercise6;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;

/**
 * dentify the vertex with the most friends and   the   most   foes.
 * Save the result as vertex_max_friend.txt and vertex_max_foe.txt, respectively
 */

public class VertexQuery {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Long, NullValue>> vertices = env.readTextFile(Config.pathToAllVertices())
                .flatMap(new DegreeDistribution.VertexReader());

        DataSet<Tuple3<Long, Long, Boolean>> edges = env.readTextFile(Config.pathToSlashdotZoo())
                .flatMap(new DegreeDistribution.EdgeReader());

        Graph<Long, NullValue, Boolean> graph = Graph.fromTupleDataSet(vertices,edges,env);

        // Vertex with max friends
        graph.filterOnEdges( t-> t.f2)
                .outDegrees()
                .maxBy(1)
                .writeAsCsv(Config.outputPath()+"vertex_max_friend.csv", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        // Vertex with max foes
        graph.filterOnEdges( t-> !t.f2)
                .outDegrees()
                .maxBy(1)
                .writeAsCsv(Config.outputPath()+"vertex_max_foe.csv", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        env.execute("Vertex with max friends and vertex with max foes");
    }
}
