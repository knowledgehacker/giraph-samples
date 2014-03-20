package giraph.samples;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.DefaultEdge;
import org.apache.giraph.edge.LongDoubleArrayEdges;
import org.apache.giraph.io.formats.TextVertexInputFormat;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class MyTextVertexInputFormat extends TextVertexInputFormat<LongWritable, DoubleWritable, DoubleWritable> {
	private static final Logger LOG = LoggerFactory.getLogger(MyTextVertexInputFormat.class);
	
	private final LongWritable _vertexId = new LongWritable(-1);
	private final DoubleWritable _vertexValue = new DoubleWritable(-1.0);
	private final LongDoubleArrayEdges _outEdges = new LongDoubleArrayEdges();
	private final DoubleWritable _dummyEdgeValue = new DoubleWritable(0.0);
	
	public TextVertexReader createVertexReader(InputSplit split,
		      TaskAttemptContext context) throws IOException {
		MyTextVertexReader vertexReader = new MyTextVertexReader();
		try {
			vertexReader.initialize(split, context);
		} catch (InterruptedException ie) {
			LOG.warn(ie.toString());
		}
		
		return vertexReader;
	}
	
	/*
    protected class MyTextVertexReader extends TextVertexReaderFromEachLine {
    	
		protected LongWritable getId(Text line) throws IOException {
			String lineStr = line.toString();
			int idx = lineStr.indexOf('\t');
			if(idx == -1) {				
				LOG.error("Corruptted line: missing separator \t when finding vertex id - " + line);
				
				return _vertexId;
			}
			
			int sepIdx = lineStr.lastIndexOf(':', idx);
			if(sepIdx == -1) {
				LOG.error("Corruptted line: missing separator : when finding vertex id - " + line);
				
				return _vertexId;
			}
			
			Long vertexId = null;
			try {
				vertexId = Long.parseLong(lineStr.substring(0, sepIdx));
			} catch (NumberFormatException nfe) {
				LOG.error(nfe.toString());
			}
			if(vertexId != null)
				_vertexId.set(vertexId);
			
			return _vertexId;
		}
		
		protected DoubleWritable getValue(Text line) throws IOException {
			String lineStr = line.toString();
			int idx = lineStr.indexOf('\t');
			if(idx == -1) {
				LOG.error("Corruptted line: missing separator \t when finding vertex value - " + line);

				return _vertexValue;
			}
			
			int sepIdx = lineStr.lastIndexOf(':', idx);
			if(sepIdx == -1) {
				LOG.error("Corruptted line: missing separator : when finding vertex value - " + line);

				return _vertexValue;
			}
			
			Double vertexValue = null;
			try {
				vertexValue = Double.parseDouble(lineStr.substring(sepIdx+1, idx));
			} catch (NumberFormatException nfe) {
				LOG.error(nfe.toString());
			}
			if(vertexValue != null)
				_vertexValue.set(vertexValue);
			
			return _vertexValue;
		}
		
		protected Iterable<Edge<LongWritable, DoubleWritable>> getEdges(Text line) throws IOException {
			String lineStr = line.toString();
			int idx = lineStr.indexOf('\t');
			if(idx == -1) {
				LOG.error("Corruptted line: missing the first separator \t when finding outgoing edges - " + line);
				
				return _outEdges;
			}
			
			int start = idx+1;
			int end = lineStr.indexOf('\t', start);
			LongWritable neighbourVertexId = new LongWritable(-1);
			DefaultEdge<LongWritable, DoubleWritable> outEdge = new DefaultEdge<LongWritable, DoubleWritable>();
			while(end != -1) {
				neighbourVertexId.set(Long.parseLong(lineStr.substring(start, end)));
				outEdge.setTargetVertexId(neighbourVertexId);
				outEdge.setValue(_dummyEdgeValue);
				_outEdges.add(outEdge);
				
				start = end+1;
				end = lineStr.indexOf('\t', start);
			}
			neighbourVertexId.set(Long.parseLong(lineStr.substring(start)));
			outEdge.setTargetVertexId(neighbourVertexId);
			outEdge.setValue(_dummyEdgeValue);
			_outEdges.add(outEdge);
			
			return _outEdges;
		}
	}
	*/
	
	protected class MyTextVertexReader extends TextVertexReaderFromEachLineProcessed<List<String>> {

		private static final char SEPARATOR = '\t';
		
		protected List<String> preprocessLine(Text line) throws IOException {
			List<String> fields = new ArrayList<String>();
			
			String lineStr = line.toString();
			int start = 0;
			int end = lineStr.indexOf(SEPARATOR, start);
			String field = null;
			while(end != -1) {
				field = lineStr.substring(start, end);
				fields.add(field);
				
				start = end+1;
				end = lineStr.indexOf(SEPARATOR, start);
			}
			field = lineStr.substring(start);
			fields.add(field);
			
			return fields;
		}
		
		protected LongWritable getId(List<String> fields) throws IOException {
			String vertex = fields.get(0);
			int idx = vertex.indexOf(':');
			if(idx == -1) {
				LOG.error("Corrupted field: missing separator : when finding vertex id - " + vertex);
			} else {
				Long vertexId = null;
				try {
					vertexId = Long.parseLong(vertex.substring(0, idx));
				} catch(NumberFormatException nfe) {
					LOG.error(nfe.toString());
				}
				if(vertexId != null)
					_vertexId.set(vertexId);
			}
			
			return _vertexId;
		}
		
		protected DoubleWritable getValue(List<String> fields) throws IOException {
			String vertex = fields.get(0);
			int idx = vertex.indexOf(':');
			if(idx == -1) {
				LOG.error("Corrupted field: missing separator : when finding vertex value - " + vertex);
			} else {
				Double vertexValue = null;
				try {
					vertexValue = Double.parseDouble(vertex.substring(idx+1));
				} catch(NumberFormatException nfe) {
					LOG.error(nfe.toString());
				}
				if(vertexValue != null)
					_vertexValue.set(vertexValue);
			}
			
			return _vertexValue;
		}
		
		protected Iterable<Edge<LongWritable, DoubleWritable>> getEdges(List<String> fields) throws IOException {
			LongWritable neighbourVertexId = new LongWritable();
			DefaultEdge<LongWritable, DoubleWritable> outEdge = new DefaultEdge<LongWritable, DoubleWritable>();
			for(int i = 1; i < fields.size(); ++i) {
				Long field = null;
				try {
					field = Long.parseLong(fields.get(i));
				} catch(NumberFormatException nfe) {
					LOG.error(nfe.toString());
				}
				if(field != null) {
					neighbourVertexId.set(field);
				
					outEdge.setTargetVertexId(neighbourVertexId);
					outEdge.setValue(_dummyEdgeValue);
					_outEdges.add(outEdge);
				}
			}
			
			return _outEdges;
		}
	}
}
