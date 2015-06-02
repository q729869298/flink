package org.apache.flink.graph.library;

import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.graph.utils.Hits;

import java.io.Serializable;


/**
 *
 * This class implements the HITS algorithm by using flink Gelly API
 *Hyperlink-Induced Topic Search (HITS; also known as hubs and authorities) is a link analysis algorithm that rates Web pages,
 *developed by Jon Kleinberg.
 *
 * The algorithm performs a series of iterations, each consisting of two basic steps:
 *
 * Authority Update: Update each node's Authority score to be equal to the sum of the Hub Scores of each node that
 * points to it.
 * That is, a node is given a high authority score by being linked from pages that are recognized as Hubs for information.
 * Hub Update: Update each node's Hub Score to be equal to the sum of the Authority Scores of each node that it
 * points to.
 * That is, a node is given a high hub score by linking to nodes that are considered to be authorities on the subject.
 *
 * The Hub score and Authority score for a node is calculated with the following algorithm:
 *  *Start with each node having a hub score and authority score of 1.
 *  *Run the Authority Update Rule
 *  *Run the Hub Update Rule
 *  *Normalize the values by dividing each Hub score by square root of the sum of the squares of all Hub scores, and
 *   dividing each Authority score by square root of the sum of the squares of all Authority scores.
 *  *Repeat from the second step as necessary.
 *
 * http://en.wikipedia.org/wiki/HITS_algorithm
 *
 */

public class HITS <K extends Comparable<K> & Serializable>implements GraphAlgorithm<K, Double, String> {

    private Hits HubAuthority;
    private int maxIterations;

    public HITS(Hits choice, int maxIter){

        this.HubAuthority=choice;

        if(this.HubAuthority.equals(Hits.AUTHORITY)) {

            this.maxIterations=(maxIter*4)-2;

        }else {

            this.maxIterations=maxIter*4;

        }

    }

    /**
     * this method will get a graph and process for Hub and Authority and it will return a graph( Hub or Authority
     * values).
     * @param HitsGraph
     * @return Graph
     * @throws Exception
     */

    @Override
    public Graph<K, Double, String> run(Graph<K, Double, String> HitsGraph) throws Exception {

        HitsGraph=(HitsGraph.mapEdges(new MapFunction<Edge<K,String>, String>() {

            @Override
            public String map(Edge<K, String> value) throws Exception { return "H"; } })).

                union(HitsGraph.reverse().mapEdges(new MapFunction<Edge<K, String>, String>() {

                    @Override
                    public String map(Edge<K, String> value) throws Exception {
                        return "A";
                    }
                }));

            return HitsGraph.runVertexCentricIteration(new VertexHitsUpdater<K>(),new HitsMessenger<K>(),maxIterations);
        }
                /**
                 * Function that updates in odd superStep Iteration either the Hub or Authority values of a vertex by summing up
                 * the partial Hits or Authority values from all incoming messages and then applying the normalization process on
                 * even superstep for either Hub or Authority values.
                 */
        @SuppressWarnings("serial")
    public static final class VertexHitsUpdater<K> extends VertexUpdateFunction<K, Double, Double> {

        double sum;
        LongSumAggregator aggregator=new LongSumAggregator();
        public void preSuperstep() {
        // retrieve the Aggregator
            sum = getIterationAggregator("sum");
            
            if(getSuperstepNumber()%4==3)
                sum=0.0;
        }
        @Override
        public void updateVertex(Vertex<K, Double> vertex, MessageIterator<Double> inMessages) {

            double num=0.0;
            switch (getSuperstepNumber()%4){
                case 1:
                    for(double m:inMessages)
                        num+=m;
                    sum+=Math.pow(num,2);
                    break;

                case 2:
                    setNewVertexValue(vertex.f1/(Math.sqrt(sum)));
                    break;

                case 3:
                    for(double m:inMessages)
                        num+=m;
                    sum+=Math.pow(num,2);
                    break;

                case 4:
                    setNewVertexValue(vertex.f1/(Math.sqrt(sum)));
                    break;
            }
        }
    }

    /**
     *Distributes in even superstep iteration the normalization value to the target vertices.
     */
    @SuppressWarnings("serial")
    public static final class HitsMessenger<K> extends MessagingFunction<K, Double, Double, String> {

        @Override
        public void sendMessages(Vertex<K, Double> vertex) {

            switch (getSuperstepNumber()%4) {
                case 2:
                    for(Edge<K,String> ed: getEdges()){
                        if (ed.f1.equals("A"))
                            sendMessageTo(ed.getTarget(),vertex.f1);
                    }
                    break;

                case 4:
                    for(Edge<K,String> ed: getEdges()){
                        if (ed.f1.equals("H"))
                            sendMessageTo(ed.getTarget(),vertex.f1);
                    }
                    break;
            }
        }
    }
}

