package com.bayee.petition.service.impl;

import com.bayee.petition.domain.BoatShipData;
import com.bayee.petition.domain.BoatShipHistoryData;
import com.bayee.petition.domain.TutorialData;
import com.bayee.petition.mapper.CommandLineDataStore;
import com.bayee.petition.service.BoatShipService;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.geotools.data.*;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory;
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.Filter;
import org.opengis.filter.sort.SortBy;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * @author antuo
 * @since 2021/7/6 10:01
 */
@Service
public class BoatShipServiceImpl implements BoatShipService {

    private  Map<String, String> params;
    private TutorialData data;
    private  boolean cleanup;
    private  boolean readOnly;

    public Map<String,Object> query1(String ecql) throws ParseException {
        String[] arg ={"--hbase.catalog", "zs:yhh", "--hbase.zookeepers", "manager1:2181"};

        Options options = createOptions(new HBaseDataStoreFactory().getParametersInfo());
        CommandLine command = CommandLineDataStore.parseArgs(getClass(), options, arg);
        this.params = CommandLineDataStore.getDataStoreParams(command, options);
        this.cleanup = command.hasOption("cleanup");
        this.data = new BoatShipData();
        this.readOnly = true;
        initializeFromOptions(command);
        DataStore datastore = null;
        try {
            datastore = createDataStore();
            System.out.println(datastore);
            if (readOnly) {
                ensureSchema(datastore, data);
            }
            List<Query> queries = getTestQueries(data,ecql);

            return queryFeatures(datastore, queries);
        } catch (Exception e) {
            throw new RuntimeException("Error running quickstart:", e);
        } finally {
            cleanup(datastore, data.getTypeName());
        }
    }

    @Override
    public Map<String,Object> query(String points,String startDate,String endDate,String attributes) throws ParseException {
        String[] arg ={"--hbase.catalog", "zs:yhh", "--hbase.zookeepers", "manager1:2181"};

        Options options = createOptions(new HBaseDataStoreFactory().getParametersInfo());
        CommandLine command = CommandLineDataStore.parseArgs(getClass(), options, arg);
        this.params = CommandLineDataStore.getDataStoreParams(command, options);
        this.cleanup = command.hasOption("cleanup");
        this.data = new BoatShipData();
        this.readOnly = true;
        initializeFromOptions(command);
        DataStore datastore = null;
        try {
            datastore = createDataStore();
            System.out.println(datastore);
            if (readOnly) {
                ensureSchema(datastore, data);
            }
            String ecql;
            DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.CHINESE);
            if(startDate != null && endDate !=null && attributes!=null) {
                startDate = LocalDateTime.parse(startDate, dateFormat).toInstant(ZoneOffset.ofHours(8)).toString();
                endDate = LocalDateTime.parse(endDate, dateFormat).toInstant(ZoneOffset.ofHours(8)).toString();
                String[] split = attributes.split(",");
                String attribute = "";
                for (int i=0; i<split.length-1; i++) {
                    attribute = attribute+split[i]+" AND ";
                }
                attribute = attribute+split[split.length-1];
                ecql = "contains('POLYGON ((" + points + "))', location_point) AND update_time DURING "+startDate+"/"+endDate+" AND "+attribute;
            } else if(startDate != null && endDate !=null) {
                startDate = LocalDateTime.parse(startDate, dateFormat).toInstant(ZoneOffset.ofHours(8)).toString();
                endDate = LocalDateTime.parse(endDate, dateFormat).toInstant(ZoneOffset.ofHours(8)).toString();
                ecql = "contains('POLYGON ((" + points + "))', location_point) AND update_time DURING "+startDate+"/"+endDate;
            } else if(startDate==null && endDate==null && attributes!=null){
                String[] split = attributes.split(",");
                String attribute = "";
                for (int i=0; i<split.length-1; i++) {
                    attribute = attribute+split[i]+" AND ";
                }
                attribute = attribute+split[split.length-1];
                ecql = "contains('POLYGON ((" + points + "))', location_point) AND "+attribute;
            } else {
                ecql = "contains('POLYGON ((" + points + "))', location_point)";
            }
            List<Query> queries = getTestQueries(data,ecql);

            return queryFeatures(datastore, queries);
        } catch (Exception e) {
            throw new RuntimeException("Error running quickstart:", e);
        } finally {
            cleanup(datastore, data.getTypeName());
        }
    }

    @Override
    public Map<String,Object> queryAisPoint(String points,String startDate,String endDate,String attributes) throws ParseException {
        String[] arg ={"--hbase.catalog", "zs:yhh", "--hbase.zookeepers", "manager1:2181"};

        Options options = createOptions(new HBaseDataStoreFactory().getParametersInfo());
        CommandLine command = CommandLineDataStore.parseArgs(getClass(), options, arg);
        this.params = CommandLineDataStore.getDataStoreParams(command, options);
        this.cleanup = command.hasOption("cleanup");
        this.data = new BoatShipData();
        this.readOnly = true;
        initializeFromOptions(command);
        DataStore datastore = null;
        try {
            datastore = createDataStore();
            System.out.println(datastore);
            if (readOnly) {
                ensureSchema(datastore, data);
            }

            String ecql;
            DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.CHINESE);
            if(startDate != null && endDate !=null && attributes!=null) {
                startDate = LocalDateTime.parse(startDate, dateFormat).toInstant(ZoneOffset.ofHours(8)).toString();
                endDate = LocalDateTime.parse(endDate, dateFormat).toInstant(ZoneOffset.ofHours(8)).toString();
                String[] split = attributes.split(",");
                String attribute = "";
                for (int i=0; i<split.length-1; i++) {
                    attribute = attribute+split[i]+" AND ";
                }
                attribute = attribute+split[split.length-1];
                ecql = "contains('POLYGON ((" + points + "))', location_point) AND update_time DURING "+startDate+"/"+endDate+" AND "+attribute;
            } else if(startDate != null && endDate !=null) {
                startDate = LocalDateTime.parse(startDate, dateFormat).toInstant(ZoneOffset.ofHours(8)).toString();
                endDate = LocalDateTime.parse(endDate, dateFormat).toInstant(ZoneOffset.ofHours(8)).toString();
                ecql = "contains('POLYGON ((" + points + "))', location_point) AND update_time DURING "+startDate+"/"+endDate;
            } else if(startDate==null && endDate==null && attributes!=null){
                String[] split = attributes.split(",");
                String attribute = "";
                for (int i=0; i<split.length-1; i++) {
                    attribute = attribute+split[i]+" AND ";
                }
                attribute = attribute+split[split.length-1];
                ecql = "contains('POLYGON ((" + points + "))', location_point) AND "+attribute;
            } else {
                ecql = "contains('POLYGON ((" + points + "))', location_point)";
            }
            List<Query> queries = getTestQueries(data,ecql);

            return queryAisPoint(datastore, queries);
        } catch (Exception e) {
            throw new RuntimeException("Error running quickstart:", e);
        } finally {
            cleanup(datastore, data.getTypeName());
        }
    }

    @Override
    public Map<String,Object> queryHistoryAisPoint(String points,String startDate,String endDate,String attributes) throws ParseException {
        String[] arg ={"--hbase.catalog", "zs:yhh", "--hbase.zookeepers", "manager1:2181"};

        Options options = createOptions(new HBaseDataStoreFactory().getParametersInfo());
        CommandLine command = CommandLineDataStore.parseArgs(getClass(), options, arg);
        this.params = CommandLineDataStore.getDataStoreParams(command, options);
        this.cleanup = command.hasOption("cleanup");
        this.data = new BoatShipHistoryData();
        this.readOnly = true;
        initializeFromOptions(command);
        DataStore datastore = null;
        try {
            datastore = createDataStore();
            System.out.println(datastore);
            if (readOnly) {
                ensureSchema(datastore, data);
            }

            String ecql;
            DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.CHINESE);
            if(startDate != null && endDate !=null && attributes!=null) {
                startDate = LocalDateTime.parse(startDate, dateFormat).toInstant(ZoneOffset.ofHours(8)).toString();
                endDate = LocalDateTime.parse(endDate, dateFormat).toInstant(ZoneOffset.ofHours(8)).toString();
                String[] split = attributes.split(",");
                String attribute = "";
                for (int i=0; i<split.length-1; i++) {
                    attribute = attribute+split[i]+" AND ";
                }
                attribute = attribute+split[split.length-1];
                ecql = "contains('POLYGON ((" + points + "))', location_point) AND update_time DURING "+startDate+"/"+endDate+" AND "+attribute;
            } else if(startDate != null && endDate !=null) {
                startDate = LocalDateTime.parse(startDate, dateFormat).toInstant(ZoneOffset.ofHours(8)).toString();
                endDate = LocalDateTime.parse(endDate, dateFormat).toInstant(ZoneOffset.ofHours(8)).toString();
                ecql = "contains('POLYGON ((" + points + "))', location_point) AND update_time DURING "+startDate+"/"+endDate;
            } else if(startDate==null && endDate==null && attributes!=null){
                String[] split = attributes.split(",");
                String attribute = "";
                for (int i=0; i<split.length-1; i++) {
                    attribute = attribute+split[i]+" AND ";
                }
                attribute = attribute+split[split.length-1];
                ecql = "contains('POLYGON ((" + points + "))', location_point) AND "+attribute;
            } else {
                ecql = "contains('POLYGON ((" + points + "))', location_point)";
            }
            List<Query> queries = getTestQueries(data,ecql);

            return queryHistoryAisPoint(datastore, queries);
        } catch (Exception e) {
            throw new RuntimeException("Error running quickstart:", e);
        } finally {
            cleanup(datastore, data.getTypeName());
        }
    }

    public List<Query> getTestQueries(TutorialData data,String ecql) {
        return data.getTestQueries(ecql);
    }

    public Map<String,Object> queryFeatures(DataStore datastore, List<Query> queries) throws IOException {
        Map<String,Object> jsonData = new TreeMap<>();
        Map<String,Object> data = new HashMap<>();
        boolean flag = true;
        for (Query query : queries) {

            if (query.getPropertyNames() != null) {
                System.out.println("Returning attributes " + Arrays.asList(query.getPropertyNames()));
            }
            if (query.getSortBy() != null) {
                SortBy sort = query.getSortBy()[0];
                System.out.println("Sorting by " + sort.getPropertyName() + " " + sort.getSortOrder());
            }

            try (FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                         datastore.getFeatureReader(query, Transaction.AUTO_COMMIT)) {

                while (reader.hasNext()) {
                    Map<String,Object> map = new HashMap<>();
                    SimpleFeature feature = reader.next();
                    String mmsi = null;
                    List<AttributeDescriptor> attributeDescriptors = feature.getType().getAttributeDescriptors();
                    List<Object> attributes = feature.getAttributes();
                    for (int i = 0; i < feature.getAttributeCount(); i++) {
                        String field = attributeDescriptors.get(i).getLocalName();
                        if(!"location_point".equals(field)) {
                            map.put(attributeDescriptors.get(i).getLocalName(), attributes.get(i));
                        }
                        if("mmsi".equals(field)) {
                            mmsi = attributes.get(i).toString();
                        }
                    }
                    if (data.containsKey(mmsi)) {
                        long update_time = ((Date)(((Map<String,Object>)data.get(mmsi)).get("update_time"))).getTime();
                        long update_time1 = ((Date)map.get("update_time")).getTime();
                        if(update_time1<=update_time) {
                            flag = false;
                        }
                    } else {
                        flag = true;
                    }
                    if (flag) {
                        data.put(mmsi,map);
                    }
                }
            }
        }
        jsonData.put("count",data.size());
        jsonData.put("data",data);
        return jsonData;
    }

    public Map<String,Object> queryAisPoint(DataStore datastore, List<Query> queries) throws IOException {
        Map<String,Object> jsonData = new TreeMap<>();

        Map<String,Object> data = new HashMap<>();

        boolean flag = true;

        for (Query query : queries) {
            if (query.getPropertyNames() != null) {
                System.out.println("Returning attributes " + Arrays.asList(query.getPropertyNames()));
            }
            if (query.getSortBy() != null) {
                SortBy sort = query.getSortBy()[0];
                System.out.println("Sorting by " + sort.getPropertyName() + " " + sort.getSortOrder());
            }

            try (FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                         datastore.getFeatureReader(query, Transaction.AUTO_COMMIT)) {

                while (reader.hasNext()) {
                    Map<String,Object> map = new HashMap<>();
                    SimpleFeature feature = reader.next();
                    String mmsi = null;
                    List<AttributeDescriptor> attributeDescriptors = feature.getType().getAttributeDescriptors();
                    List<Object> attributes = feature.getAttributes();
                    for (int i = 0; i < feature.getAttributeCount(); i++) {
                        String field = attributeDescriptors.get(i).getLocalName();
                        if("longitude".equals(field)) {
                            map.put(attributeDescriptors.get(i).getLocalName(), attributes.get(i));
                        }
                        if("latitude".equals(field)) {
                            map.put(attributeDescriptors.get(i).getLocalName(), attributes.get(i));
                        }
                        if("mmsi".equals(field)) {
                            map.put(attributeDescriptors.get(i).getLocalName(), attributes.get(i));
                        }
                        if("update_time".equals(field)) {
                            map.put(attributeDescriptors.get(i).getLocalName(), attributes.get(i));
                        }
                        if("mmsi".equals(field)) {
                            mmsi = attributes.get(i).toString();
                        }
                    }
                    if (data.containsKey(mmsi)) {
                        long update_time = ((Date)(((Map<String,Object>)data.get(mmsi)).get("update_time"))).getTime();
                        long update_time1 = ((Date)map.get("update_time")).getTime();
                        if(update_time1<=update_time) {
                            flag = false;
                        }
                    } else {
                        flag = true;
                    }
                    if (flag) {
                        data.put(mmsi,map);
                    }
                }
            }
        }
        Set<String> set = data.keySet();
        for (String s : set) {
            ((Map<String,Object>)data.get(s)).remove("update_time");
            ((Map<String,Object>)data.get(s)).remove("mmsi");
        }
        jsonData.put("count",data.size());
        jsonData.put("data",data);
        return jsonData;
    }

    public Map<String,Object> queryHistoryAisPoint(DataStore datastore, List<Query> queries) throws IOException {
        Map<String,Object> jsonData = new TreeMap<>();

        Set<Object> data = new HashSet<>();
        for (Query query : queries) {
            if (query.getPropertyNames() != null) {
                System.out.println("Returning attributes " + Arrays.asList(query.getPropertyNames()));
            }
            if (query.getSortBy() != null) {
                SortBy sort = query.getSortBy()[0];
                System.out.println("Sorting by " + sort.getPropertyName() + " " + sort.getSortOrder());
            }
            try (FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                         datastore.getFeatureReader(query, Transaction.AUTO_COMMIT)) {
                while (reader.hasNext()) {
                    Map<String,Object> map = new HashMap<>();
                    SimpleFeature feature = reader.next();
                    List<AttributeDescriptor> attributeDescriptors = feature.getType().getAttributeDescriptors();
                    List<Object> attributes = feature.getAttributes();
                    for (int i = 0; i < feature.getAttributeCount(); i++) {
                        String field = attributeDescriptors.get(i).getLocalName();
                        if("longitude".equals(field)) {
                            map.put(attributeDescriptors.get(i).getLocalName(), attributes.get(i));
                        }
                        if("latitude".equals(field)) {
                            map.put(attributeDescriptors.get(i).getLocalName(), attributes.get(i));
                        }
                    }
                    data.add(map);
                }
            }
        }
        jsonData.put("count",data.size());
        jsonData.put("data",data);
        return jsonData;
    }

    public void cleanup(DataStore datastore, String typeName) {
        if (datastore != null) {
            try {
                if (cleanup) {
                    System.out.println("Cleaning up test data");
                    if (datastore instanceof GeoMesaDataStore) {
                        ((GeoMesaDataStore) datastore).delete();
                    } else {
                        ((SimpleFeatureStore) datastore.getFeatureSource(typeName)).removeFeatures(Filter.INCLUDE);
                        datastore.removeSchema(typeName);
                    }
                }
            } catch (Exception e) {
                System.err.println("Exception cleaning up test data: " + e.toString());
            } finally {
                // make sure that we dispose of the datastore when we're done with it
                datastore.dispose();
            }
        }
    }

    public DataStore createDataStore() throws IOException {

        // use geotools service loading to get a datastore instance
        DataStore datastore = DataStoreFinder.getDataStore(params);
        if (datastore == null) {
            throw new RuntimeException("Could not create data store with provided parameters");
        }
        return datastore;
    }

    public Options createOptions(DataAccessFactory.Param[] parameters) {
        // parse the data store parameters from the command line
        Options options = CommandLineDataStore.createOptions(parameters);
        if (!readOnly) {
            options.addOption(Option.builder().longOpt("cleanup").desc("Delete tables after running").build());
        }
        return options;
    }

    public void initializeFromOptions(CommandLine command) {
    }

    public void ensureSchema(DataStore datastore, TutorialData data) throws IOException {
        SimpleFeatureType sft = datastore.getSchema(data.getTypeName());
        if (sft == null) {
            throw new IllegalStateException("Schema '" + data.getTypeName() + "' does not exist. " +
                    "Please run the associated QuickStart to generate the test data.");
        }
    }
}
