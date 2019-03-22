package com.juniper.dataflow.common.transform;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.transforms.DoFn;

import com.google.api.services.bigquery.model.TableRow;

public class TableRowCreatorJson extends DoFn<String, TableRow> {

	
	  @ProcessElement
	  public void processElement(@Element String json, OutputReceiver<TableRow> receiver) {
	  TableRow row;
	   // Parse the JSON into a {@link TableRow} object.
	   try (InputStream inputStream =
	       new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
	     row = TableRowJsonCoder.of().decode(inputStream, Context.OUTER);
	     receiver.output(row);
	   } catch (Exception e) {
	     throw new RuntimeException("Failed to serialize json to table row: " + json, e);
	   }

	}
	  
}