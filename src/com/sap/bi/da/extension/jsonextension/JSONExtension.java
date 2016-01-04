/*
Copyright 2015, SAP SE

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
    
       http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

package com.sap.bi.da.extension.jsonextension;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sap.bi.da.extension.sdk.DAEWorkflow;
import com.sap.bi.da.extension.sdk.DAException;
import com.sap.bi.da.extension.sdk.IDAEAcquisitionJobContext;
import com.sap.bi.da.extension.sdk.IDAEAcquisitionState;
import com.sap.bi.da.extension.sdk.IDAEClientRequestJob;
import com.sap.bi.da.extension.sdk.IDAEDataAcquisitionJob;
import com.sap.bi.da.extension.sdk.IDAEEnvironment;
import com.sap.bi.da.extension.sdk.IDAEMetadataAcquisitionJob;
import com.sap.bi.da.extension.sdk.IDAEProgress;
import com.sap.bi.da.extension.sdk.IDAExtension;

public class JSONExtension implements IDAExtension {
	
	static public final String EXTENSION_ID = "com.sap.bi.da.extension.jsonextension";
	private IDAEEnvironment environment;

    public JSONExtension() {
    }

    @Override
    public void initialize(IDAEEnvironment environment) {
    	this.environment = environment;
    	// This function will be called when the extension is initially loaded
    	// This gives the extension to perform initialization steps, according to the provided environment
    }

    @Override
    public IDAEAcquisitionJobContext getDataAcquisitionJobContext (IDAEAcquisitionState acquisitionState) {
        return new JSONExtensionAcquisitionJobContext(environment, acquisitionState);
    }

    @Override
    public IDAEClientRequestJob getClientRequestJob(String request) {
        return new JSONExtensionClientRequestJob(request);
    }

    private static class JSONExtensionAcquisitionJobContext implements IDAEAcquisitionJobContext {

        private IDAEAcquisitionState acquisitionState;
        private IDAEEnvironment environment;

        JSONExtensionAcquisitionJobContext(IDAEEnvironment environment, IDAEAcquisitionState acquisitionState) {
            this.acquisitionState = acquisitionState;
            this.environment = environment;
        }

        @Override
        public IDAEMetadataAcquisitionJob getMetadataAcquisitionJob() {
            return new JSONExtensionMetadataRequestJob(environment, acquisitionState);
        }

        @Override
        public IDAEDataAcquisitionJob getDataAcquisitionJob() {
            return new JSONExtensionDataRequestJob(environment, acquisitionState);
        }

        @Override
        public void cleanup() {
        	// Called once acquisition is complete
        	// Provides the job the opportunity to perform cleanup, if needed
        	// Will be called after both job.cleanup()'s are called
        }
    }
    
    public static Reader newReader(InputStream input) {
		return newReader(input, (Charset) null);
	}

	public static Reader newReader(InputStream input, String encoding) {
		return newReader(input, Charset.forName(encoding));
	}

	public static Reader newReader(InputStream input, Charset encoding) {
		if (encoding != null) {
			return new InputStreamReader(input, encoding);
		} else {
			return new InputStreamReader(input);
		}
	}
    
    public static Reader newReader(File file) {
		return newReader(file, (Charset) null);
	}

	public static Reader newReader(File file, String encoding) {
		return newReader(file, Charset.forName(encoding));
	}

	public static Reader newReader(File file, Charset encoding) {
		FileInputStream input;
		try {
			input = new FileInputStream(file);
		} catch (FileNotFoundException e) {
			throw new IllegalArgumentException(e);
		}

		return newReader(input, encoding);
	}
    
    private static class JSONExtensionDataRequestJob implements IDAEDataAcquisitionJob
    {
        IDAEAcquisitionState acquisitionState;
        IDAEEnvironment environment;

        JSONExtensionDataRequestJob (IDAEEnvironment environment, IDAEAcquisitionState acquisitionState) {
            this.acquisitionState = acquisitionState;
            this.environment = environment;
        }

        @Override
        public File execute(IDAEProgress callback) throws DAException {
        	
            try {
            	File dataFile = JSONExtensionMetadataRequestJob.dataFile;
            	dataFile.deleteOnExit();

            	return dataFile;
            } catch (Exception e) {
                throw new DAException("JSON Extension acquisition failed" + e.toString(), e);
            }
        }

        @Override
        public void cancel() {
        	// Cancel is currently not supported
        }

        @Override
        public void cleanup() {
        	// Called once acquisition is complete
        }
    }

    private static class JSONExtensionMetadataRequestJob implements IDAEMetadataAcquisitionJob {
    	public static File dataFile;
    	
        IDAEAcquisitionState acquisitionState;
        IDAEEnvironment environment;
        
        JSONExtensionMetadataRequestJob (IDAEEnvironment environment, IDAEAcquisitionState acquisitionState) {
            this.acquisitionState = acquisitionState;
            this.environment = environment;
        }
        public String csvMetadata = "";
        
        private ArrayList<String> tableHeader = new ArrayList<>();
        private HashMap<String, String> tableHeaderDataTypes = new HashMap<>();
        private ArrayList<String> tableRow = new ArrayList<>();
        private int tableRows = 0;
        private File csvFile;
        private BufferedWriter csvBw;

        @Override
        public String execute(IDAEProgress callback) throws DAException {
            try {
                JSONObject infoJSON = new JSONObject(acquisitionState.getInfo());
                String JSONFilePath = infoJSON.getString("jsonfilepath");
            	
                File jsonFile = new File(JSONFilePath);
            	dataFile = File.createTempFile(JSONExtension.EXTENSION_ID, ".csv", environment.getTemporaryDirectory());
            	
            	parseJsonToCsv(jsonFile);

            	return csvMetadata;
            } catch (Exception e) {
                throw new DAException("JSON Extension acquisition failed", e);
            }
        }
        
        private void parseJsonToCsv(File jsonFile) {
            csvMetadata = "";
            tableHeader = new ArrayList<>();
            tableHeaderDataTypes = new HashMap<>();
            tableRows = 0;

            try
            {
                // TODO - replace file reference with how DAE does this.

                // Parse a large (or small) JSON file using the Jackson parser.
                // Do that by streaming through it instead of loading it in one go.
                // Based on: Parsing a large JSON file efficiently and easily
                // See:      http://www.ngdata.com/parsing-a-large-json-file-efficiently-and-easily/

                // Open the JSON file.
                JsonFactory f = new MappingJsonFactory();
                JsonParser jp = f.createJsonParser(jsonFile);
                JsonToken token = jp.nextToken();
                if (token != JsonToken.START_OBJECT) {
                    throw new Exception("JSON root should be an object");
                }

                // Open temporary CSV file.
                csvBw = new BufferedWriter(new FileWriter(dataFile));

                // Parse the JSON - use streaming in case this is a big file.
                // When streaming, for each Array item that should be small so we
                // can turn that Array item into a tree.
                while (jp.nextToken() != JsonToken.END_OBJECT) {
                    String fieldName = jp.getCurrentName();

                    // Move from field name to field value.
                    token = jp.nextToken();
                    if (token == JsonToken.START_ARRAY) {
                        // For each item in the array.
                        while (jp.nextToken() != JsonToken.END_ARRAY) {
                            // New row.
                            tableRows++;
                            tableRow = new ArrayList<>();

                            // Read the array item as a tree.
                            JsonNode node = jp.readValueAsTree();
                            parseJsonObject2Csv(node);

                            // Append to the CSV.
                            addTableRowToCsv();
                        }
                    } else {
                        // Not an array token so skip its children.
                        jp.skipChildren();
                    }
                }

                // Build the CSV meta data based on the header data types.
                csvMetadata = createMetadata();
                System.out.println(csvMetadata);
                System.out.println("Cols: " + tableHeader.size());
                System.out.println("Rows: " + tableRows);
                System.out.println("File: " + csvFile.getAbsolutePath());
            } catch (Exception e) {
                System.err.println("Reading JSON failed");
                System.err.println(e.getMessage());
                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw));
                String stackTraceStr = sw.toString();
                System.err.println(stackTraceStr);

                // TODO: throw new DAException("Reading JSON failed", e);
            } finally {
                // Close CSV file.
                try {
                    csvBw.close();
                } catch (Exception e) {
                    // Ignore.
                }
            }
        }

        private void parseJsonArray2Csv(JsonNode arr) throws IOException {
            ArrayList<String> baseRow = copyList(tableRow);

            for (int i=0; i < arr.size(); i++) {
                JsonNode v = arr.get(i);
                if (i == 0) {
                    // First row - use as normal.
                    parseJsonObject2Csv(v);
                } else {
                    // More then one row in this array.
                    // Add the current row to the CSV.
                    // Then start a new row based on the "baseRow".

                    // Write out the current line to the CSV file.
                    addTableRowToCsv();

                    // Start a new row based on the base row.
                    tableRow = copyList(baseRow);
                    tableRows++;
                    parseJsonObject2Csv(v);
                }
            }
        }

        private void parseJsonObject2Csv(JsonNode obj) throws IOException {
            ArrayList<String> arrKeys = new ArrayList<>();
            String v = "";

            // Fields.
            Iterator<String> fieldIter = obj.fieldNames();
            while (fieldIter.hasNext()) {
                String key = fieldIter.next();
                JsonNode field = obj.get(key);
                if (field.isArray()) {
                    // Need to finish the fields first, save the array till later.
                    arrKeys.add(key);
                } else if (field.isNumber()) {
                    // Number.
                    v = field.asText();
                    addTableValue(v, key, true);
                } else if (field.isBoolean()) {
                    // Boolean.
                    Boolean b = field.asBoolean();
                    v = (b ? "TRUE" : "FALSE");
                    addTableValue("\"" + v + "\"", key, false);
                } else if (field.isTextual()) {
                    // String.
                    v = field.asText();
                    v = v.replaceAll("\"", "\"\"");  // Escape double quotes.
                    addTableValue("\"" + v + "\"", key, false);
                } else if (field.isObject()) {
                    // Convert JSON object.
                    parseJsonObject2Csv(field);
                }
            }

            // Arrays.
            Iterator<String> arrKeyIter = arrKeys.iterator();
            while (arrKeyIter.hasNext()) {
                String key = arrKeyIter.next();
                JsonNode field = obj.get(key);

                // Convert JSON array.
                parseJsonArray2Csv(field);
            }
        }

        private void addTableValue(String value, String colName, Boolean isNumber) {
            String myColName = "\"" + colName + "\"";

            // Header.
            if (tableRows == 1) {
                // As we are flattening nested JSON make sure the column names we use are unique when flattened.
                if (tableHeader.contains(myColName)) {
                    // Column name is already used - so need to make a unique flattened column name.
                    int counter = 2;
                    myColName = "\"" + colName + " " + (counter++) + "\"";
                    while (tableHeader.contains(myColName)) {
                        myColName = "\"" + colName + " " + (counter++) + "\"";
                    }
                }

                // Add the unique column name to the header.
                tableHeader.add(myColName);
                tableHeaderDataTypes.put(myColName, (isNumber ? "Number" : "String"));
            }

            // Find out where to update the row in case the JSON fields
            // are ina  different order in each row.
            int idx = tableHeader.indexOf(myColName);
            if (idx > -1 && idx < tableHeader.size()) {
                while (tableRow.size() < (idx + 1)) {
                    tableRow.add("");
                }
                tableRow.set(idx, value);

                // If not a number keep track of the String data type.
                if (!isNumber) {
                    if (tableHeaderDataTypes.get(myColName) == "Number") {
                        // Not a number so change to String.
                        tableHeaderDataTypes.put(myColName, "String");
                    }
                }
            }
        }

        private ArrayList<String> copyList(ArrayList<String> arrStr) {
            return new ArrayList<String>(arrStr);
        }

        private void addTableRowToCsv() throws IOException {
            String rowStr = StringUtils.join(tableRow, ",");
            csvBw.write(rowStr);
            csvBw.newLine();
        }

        private String createMetadata() {
            ObjectMapper mapper = new ObjectMapper();

            // Version.
            JsonNode rootNode = mapper.createObjectNode();
            ((ObjectNode) rootNode).put("version", "1.0");
            JsonNode columns = mapper.createArrayNode();

            // Columns.
            ((ObjectNode) rootNode).put("columns", columns);
            Iterator<String> columnIter = tableHeader.iterator();
            Pattern patternId = Pattern.compile("[^a-zA-Z0-9_]+", Pattern.CASE_INSENSITIVE);
            while (columnIter.hasNext()) {
                // Define the column metadata.
                String colName = columnIter.next();
                String myColName = colName.replaceAll("\"", ""); // Remove double quotes.
                String colDataType = tableHeaderDataTypes.get(colName);
                if (colDataType != null && colDataType.length() > 0) {
                    Boolean isMeasure = colDataType == "Number";
                    String analyticalType = (isMeasure ? "measure" : "dimension");
                    String id = myColName.replaceAll(patternId.pattern(), "_");
                    
                    JsonNode colNode = mapper.createObjectNode();
                    ((ObjectNode) colNode).put("name", myColName);
                    ((ObjectNode) colNode).put("id", id);
                    ((ObjectNode) colNode).put("type", colDataType);
                    ((ObjectNode) colNode).put("analyticalType", analyticalType);
                    if (isMeasure) {
                        ((ObjectNode) colNode).put("aggregationFunction", "NONE");
                    }

                    // Add the column metadata.
                    ((ArrayNode) columns).add(colNode);
                }
            }

            // Return JSON metadata.
            return rootNode.toString();
        }

        @Override
        public void cancel() {
        	// Cancel is currently not supported
        }

        @Override
        public void cleanup() {
        	// Called once acquisition is complete
        }
        
        
    }

    private class JSONExtensionClientRequestJob implements IDAEClientRequestJob {

        String request;

        JSONExtensionClientRequestJob(String request) {
            this.request = request;
        }

        @Override
        public String execute(IDAEProgress callback) throws DAException {
            if ("ping".equals(request)) {
                return "pong";
            }
            return null;
        }

        @Override
        public void cancel() {
        	// Cancel is currently not supported
        }

        @Override
        public void cleanup() {
        	// This function is NOT called
        }

    }

    @Override
    public Set<DAEWorkflow> getEnabledWorkflows(IDAEAcquisitionState acquisitionState) {
    	// If the extension is incompatible with the current environment, it may disable itself using this function
    	// return EnumSet.allOf(DAEWorkflow.class) to enable the extension
    	// return EnumSet.noneOf(DAEWorkflow.class) to disable the extension
    	// Partial enabling is not currently supported
        return EnumSet.allOf(DAEWorkflow.class);
    }

}