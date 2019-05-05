package com.junsen.data;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;

import java.util.ArrayList;
import java.util.List;

import com.junsen.data.entity.RecordEntity;

public class App {
    private static final Logger LOGGER = LoggerFactory.getLogger(App.class);

    private static final Schema SCHEMA;
    private static final String SCHEMA_LOCATION = "/table1.avsc";
    private static final Path OUT_PATH = new Path("table1.parquet");
    static {
        try (InputStream inStream = App.class.getResourceAsStream(SCHEMA_LOCATION)) {
            SCHEMA = new Schema.Parser().parse(IOUtils.toString(inStream, "UTF-8"));
        } catch (Exception e) {
            LOGGER.error("Can't read SCHEMA file from {}", SCHEMA_LOCATION);
            throw new RuntimeException("Can't read SCHEMA file from" + SCHEMA_LOCATION, e);
        }
    }

    private static List<RecordEntity> readRecordFromCsv(String fileName){
        List<RecordEntity> records=new ArrayList<>();
        java.nio.file.Path pathToFile = java.nio.file.Paths.get(fileName);
        try(BufferedReader br =java.nio.file.Files.newBufferedReader(pathToFile,StandardCharsets.UTF_8)){
          String line=br.readLine();
          while (line !=null){
              String[] attributes=line.split(",");
              RecordEntity recordEntity=createRecord(attributes);
              records.add(recordEntity);
              line=br.readLine();
          }
        }
        catch (IOException ioe){
            ioe.printStackTrace();
        }
        return records;
    }

    private static RecordEntity createRecord(String[] metadata){

        String patentId=metadata[0];
        String ansName=metadata[1];
        String apno=metadata[2];
        Integer pbdt=Integer.parseInt(metadata[3]);
        String patentType=metadata[4];
        return new RecordEntity(patentId,ansName,apno,pbdt,patentType);


    }
    private static GenericData.Record genRecord(RecordEntity recordEntity){
        GenericData.Record record = new GenericData.Record(SCHEMA);
        record.put("patent_id",recordEntity.getPatentId());
        record.put("ans_name",recordEntity.getAnsName().length()==0?null:recordEntity.getAnsName());
        record.put("apno",recordEntity.getApno().length()==0?null:recordEntity.getApno());
        record.put("pbdt",recordEntity.getPbdt()==0?null:recordEntity.getPbdt());
        record.put("patent_type",recordEntity.getPatentType().length()==0?null:recordEntity.getPatentType());
        return record;
    }
    public static void main(String[] args) throws IOException {
        if (args.length!=1){
            System.out.println("please specify csv data file path!");
            return;
        }
        List<GenericData.Record> sampleData = new ArrayList<>();


        List<RecordEntity> recordEntities=new ArrayList<>();
        recordEntities=readRecordFromCsv(args[0]);
        recordEntities.forEach(
                rec->sampleData.add(genRecord(rec))
        );

        App writerReader = new App();
        writerReader.writeToParquet(sampleData, OUT_PATH);
        writerReader.readFromParquet(OUT_PATH);
    }

    @SuppressWarnings("unchecked")
    public void readFromParquet(Path filePathToRead) throws IOException {
        try (ParquetReader<GenericData.Record> reader = AvroParquetReader
                .<GenericData.Record>builder(filePathToRead)
                .withConf(new Configuration())
                .build()) {

            GenericData.Record record;
            while ((record = reader.read()) != null) {
                System.out.println(record);
            }
        }
    }

    public void writeToParquet(List<GenericData.Record> recordsToWrite, Path fileToWrite) throws IOException {
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter
                .<GenericData.Record>builder(fileToWrite)
                .withSchema(SCHEMA)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {

            for (GenericData.Record record : recordsToWrite) {
                writer.write(record);
            }
        }
    }
}
