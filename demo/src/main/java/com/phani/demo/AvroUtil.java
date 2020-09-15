package com.phani.demo;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class AvroUtil {

    public static Map<String, List<List<String>>> readFromAvroFile(File file) throws IOException {
        Map<String, List<List<String>>> dataSourceKeyToMessages = new HashMap<>();
        GenericDatumReader datum = new GenericDatumReader();
        DataFileReader reader = new DataFileReader(file, datum);

        GenericData.Record record = new GenericData.Record(reader.getSchema());
        while (reader.hasNext()) {
            reader.next(record);
            //System.out.println("Key " + record.get("key") +" :: ");
            List<Utf8> content = (List<Utf8>) record.get("content");
            //System.out.println(content);
            List<List<String>> contents = dataSourceKeyToMessages.computeIfAbsent(record.get("key").toString(), k -> new ArrayList<>());
            List<String> conStr = content.stream().map(x->x.toString()).collect(Collectors.toList());
            contents.add(conStr);
            String[] outputRow = new String[content.size()];
            for(int i=0; i< conStr.size();i++){
                outputRow[i] = conStr.get(i);
            }


        }
        reader.close();
        return dataSourceKeyToMessages;
    }

    public static File writeToAVROFile(String fileName, List<GenericData.Record> recordList) throws Exception {
        final File target = new File(fileName);
        Schema schema = parseSchema();

        try (DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {

            writer.setCodec(CodecFactory.deflateCodec(9));
            if(target.exists()) {
                writer.appendTo(target);
            }else{
                writer.create(schema, target);
            }
            for (GenericData.Record rec:recordList) {
                writer.append(rec);
                writer.flush();
            }
        }

        return target;
    }

    public static List<GenericData.Record> getGenRec(Schema schema, String key, List<String[]> values) {
        List<GenericData.Record> records = new ArrayList<>();
        for(String[] value:values){
            GenericData.Record avroRec = new GenericData.Record(schema);
            avroRec.put("key", key);
            avroRec.put("content", Arrays.asList(value));
            records.add(avroRec);
        }
        return records;
    }



    public static Schema parseSchema() {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = null;
        try {
            // pass path to schema
            schema = parser.parse("{\n" +
                    " \"type\": \"record\",\n" +
                    " \"name\": \"dataRecords\",\n" +
                    " \"doc\": \"Records\",\n" +
                    " \"fields\": \n" +
                    "  [{\n" +
                    "   \"name\": \"key\", \n" +
                    "   \"type\": \"string\"\n" +
                    "   \n" +
                    "  }, \n" +
                    "  {\n" +
                    "   \"name\": \"content\",\n" +
                    "   \"type\": { \n" +
                    "      \"type\": \"array\",\n" +
                    "      \"items\": \"string\"\n" +
                    "   } \n" +
                    "  }\n" +
                    " ]\n" +
                    "}");




        } catch (Exception e) {
            e.printStackTrace();
        }
        return schema;

    }

    public int generateHashCode(int code) {

        return new HashCodeBuilder(17, 37).
                append(code).
                toHashCode();
    }
}
