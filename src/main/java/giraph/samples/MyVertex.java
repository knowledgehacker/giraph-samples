package giraph.samples;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.giraph.graph.Vertex;
//import org.apache.giraph.edge.Edge;

public class MyVertex extends Vertex<LongWritable, DoubleWritable, DoubleWritable, DoubleWritable> {

	/* 
	 * Q: when compute is called? and when messages from other vertexes are received?
	 * I guess in each superstep, the messages are assigned to workers before compute method of vertexes on the workers are called.
	 * Thus if in some superstep no messages are passed from the previous superstep, then the messages passed as argument will be empty.
	 */
	
	/*
	 * in superstep 0, each vertex distribute its rank value to other vertexes.
	 * in superstep > 0, each vertex aggregates rank values delivered from all the vertexes(including the vertex itself) in the graph.
	 */
	public void compute(Iterable<DoubleWritable> messages) throws IOException {
		long superStep = getSuperstep();
		if(superStep > 0) {
			double rankValueSum = 0.0;
			for(DoubleWritable message: messages)
				rankValueSum += message.get();
			setValue(new DoubleWritable(rankValueSum));
		}
		
		DoubleWritable distributedRankValue = new DoubleWritable();
		//distributedRankValue.set((superStep == 0 ? 1.0 : getValue().get()) / getNumEdges());
		distributedRankValue.set(getValue().get() / getNumEdges());
		/*
		for(Edge<LongWritable, DoubleWritable> edge: getEdges())
			sendMessage(edge.getTargetVertexId(), distributedRankValue);
		*/
		sendMessageToAllEdges(distributedRankValue);
		
		voteToHalt();
	}
}