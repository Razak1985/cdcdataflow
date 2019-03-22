package com.juniper.dataflow.streaming;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import com.google.api.services.bigquery.Bigquery.Tabledata.InsertAll;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.dataflow.DataflowScopes;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.juniper.dataflow.common.options.DataflowPipelineOptions;
import com.juniper.dataflow.common.transform.TableRowCreatorJson;
public class DryRunPgm {

  private static final String SCHEMA_FILE = "schema.json";

  private static final int WINDOW_SIZE_SECONDS = 30;

  private static  int record_count = 0;
  private static  String file_name= "";

  public static void main(String... args) throws IOException {
	  
	  args = new String[8];
	  	args[0] = "--project=cdcdataflow";
		args[1] = "--bucketUrl=gs://cdctest/test/";
		args[2] = "--bqTableName=cdcdataflow:abc.TRANSACTIONS_MASTER";
		//args[3] = "--runner=DirectRunner";
		args[3] = "--region=us-central1";
		args[4] = "--stagingLocation=gs://cdctest/test/";
		args[5] = "--output=gs://cdctest/test/";
		args[6] = "--tempLocation=gs://cdctest/test/";
		args[7] = "--runner=DirectRunner";
		
		//create scope list with DataFlow's scopes
	    Set<String> scopeList = new HashSet<String>();
	    scopeList.addAll(DataflowScopes.all());

	    //create GoogleCredentials object with Json credential file & the scope collection prepared above
	    GoogleCredentials credential = GoogleCredentials
	                                        // .fromStream(new FileInputStream("gcpsiduk-eb63a9b3b14e.json"))
	                                         .fromStream(new FileInputStream("cdcdataflow-13a0000e7525.json"))
	                                         .createScoped(scopeList);
		
    //BigQueryImportOptions options =
    //    PipelineOptionsFactory.fromArgs(args).as(BigQueryImportOptions.class);
	DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DataflowPipelineOptions.class);
    // Configure AWS specific options
    //AwsOptionsParser.formatOptions(options);
	options.setGcpCredential(credential);
    runPipeline(options);
  }

  static class extractmessageloactionfn extends DoFn<PubsubMessage,String>{
		@ProcessElement
		public void processElement(ProcessContext c) throws Exception
		{
			System.out.println("Inside processor..");
			PubsubMessage message = c.element();
			String location = message.getAttribute("loaction");
			c.output(location);
		}}
  
  private static void runPipeline(DataflowPipelineOptions options) throws IOException {
	  try {
		String TablName = "";
	  	Pipeline p = Pipeline.create(options);
	  	int i=0;
    	PCollection<String> streamData = null;
		streamData = p.apply("ReadPubSub",PubsubIO.readStrings().fromTopic(String.format("projects/%1$s/topics/%2$s","cdcdataflow","testcdc")));
		PCollection<String> inputpath = streamData.apply("ToTableRow",ParDo.of(new DoFn<String, String>() {
				@ProcessElement
				public void processElement(ProcessContext c) { 
					Gson gson = new GsonBuilder().create();
					HashMap<String, Object> parsedMap = gson.fromJson(c.element().toString(), HashMap.class);
					System.out.println();
					file_name = parsedMap.get("BucketName").toString();
					System.out.println("filename---->"+file_name);
					c.output(parsedMap.get("BucketName").toString());
					}
				}));
	
		PCollection<TableRow> rows = inputpath.apply("MetaDataReader", TextIO.readAll())
			// Convert each CSV row to a transfer record object
	    .apply("JsonToRowObject",ParDo.of(new DoFn<String, TableRow>() {
	    @ProcessElement
	   	  public void processElement(@Element String json, OutputReceiver<TableRow> receiver) {
	   	  TableRow row;
	   	  	
	   	   // Parse the JSON into a {@link TableRow} object.
	   	   try (InputStream inputStream =
	   	       new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
	   		   	row = TableRowJsonCoder.of().decode(inputStream, Context.OUTER);
	   	     	record_count++;
	   	  		System.out.println("record_count-->"+record_count);
	   	  		receiver.output(row);
	   	   } catch (Exception e) {
	   	     throw new RuntimeException("Failed to serialize json to table row: " + json, e);
	   	   }
	    }
	    }));
		
	    // Write the result to BigQuery.
		WriteResult writeResult = null;
		try {
			 writeResult = rows.apply(
	            "WriteToBigQuery",
	            BigQueryIO.writeTableRows()
	                .to(options.getBqTableName())
	               // .withSchema(schema.getTableSchema())
	                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
	                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
		
			rows.apply(Window.into(FixedWindows.of(new Duration(10))))
		    .apply("SumInsertedCounts", Combine.globally(Count.<TableRow>combineFn()).withoutDefaults())
		    .apply("CountsMessage", ParDo.of(new DoFn<Long, PubsubMessage>() {
		        @ProcessElement
		        public void processElement(ProcessContext c) {
		            String messagePayload = "pipeline_completed";
		            Map<String, String> attributes = new HashMap<>();
		            attributes.put("rows_written", c.element().toString());
		            PubsubMessage message = new PubsubMessage(messagePayload.getBytes(), attributes);
		            System.out.println("rows_written>>>>>>>>>>>>>>>>>>>>>>"+c.element().toString());
		            c.output(message);
		        }
		    }))
		    .apply("PublishCompletionMessage", PubsubIO.writeMessages().to(String.format("projects/%1$s/topics/%2$s","cdcdataflow","response")));
			//	PipelineResult pipelineResult=
			p.run();
					
		}catch(Exception e) {
			e.printStackTrace();
			System.out.println(""+e.getCause());
		}
			//State state=pipelineResult.waitUntilFinish();
			
			//jobId = ((DataflowPipelineJob) pipelineResult).getJobId();
			//State state = pipelineResult.getState();
		//	System.out.println("result------->"+state.name());
			
			System.out.println("#####################################################################################################################################");
			//System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>"+state.values());
			System.out.println("#####################################################################################################################################");
			//System.exit(1);
	  }catch(Exception e) {
		  e.printStackTrace();
	  }
  }
  
  public static class SumInts implements SerializableFunction<Iterable<Integer>, Integer> {
	  @Override
	  public Integer apply(Iterable<Integer> input) {
	    int sum = 0;
	    for (int item : input) {
	      sum += item;
	    }
	    return sum;
	  }
	}

}
