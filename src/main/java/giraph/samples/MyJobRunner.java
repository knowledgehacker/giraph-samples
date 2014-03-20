package giraph.samples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.job.GiraphJob;
import org.apache.giraph.edge.LongDoubleArrayEdges;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyJobRunner {
	private static final Logger LOG = LoggerFactory.getLogger(MyJobRunner.class);
	
	public void run() throws IOException {
		Configuration conf = new Configuration();
		GiraphConfiguration gConf = new GiraphConfiguration(conf);
		gConf.setVertexInputFormatClass(MyTextVertexInputFormat.class);
		gConf.setVertexClass(MyVertex.class);
		// The following setting is needed only when input OutEdges class is different from the ones used during computation
		//gConf.setInputOutEdgesClass(LongDoubleArrayEdges.class);
		gConf.setOutEdgesClass(LongDoubleArrayEdges.class);
		
		GiraphJob gJob = new GiraphJob(gConf, "Page Rank");
		try {
			gJob.run(true);
		} catch (ClassNotFoundException cnfe) {
			LOG.error(cnfe.toString());
		} catch (InterruptedException ie) {
			LOG.error(ie.toString());
		}
	}
	
	public static void main(String[] args) {
		MyJobRunner jobRunner = new MyJobRunner();
		try {
			jobRunner.run();
		}  catch (IOException ioe) {
			LOG.error(ioe.toString());
		}
	}
}
