package com.bayee.petition.service.impl;

import com.bayee.petition.domain.BoatShipData;
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

    @Override
    public Map<String,Object> query(String ecql) throws ParseException {
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

    public List<SimpleFeature> getTestFeatures(TutorialData data, List<String> jsons) {
        List<SimpleFeature> features = data.getTestData(jsons);
        return features;
    }

    public List<Query> getTestQueries(TutorialData data,String ecql) {
        return data.getTestQueries(ecql);
    }

    public Map<String,Object> queryFeatures(DataStore datastore, List<Query> queries) throws IOException {
        Map<String,Object> jsonData = new TreeMap<>();

        List<Object> data = new ArrayList<>();
        for (Query query : queries) {
            System.out.println("Running query " + ECQL.toCQL(query.getFilter()));
            if (query.getPropertyNames() != null) {
                System.out.println("Returning attributes " + Arrays.asList(query.getPropertyNames()));
            }
            if (query.getSortBy() != null) {
                SortBy sort = query.getSortBy()[0];
                System.out.println("Sorting by " + sort.getPropertyName() + " " + sort.getSortOrder());
            }
            // submit the query, and get back an iterator over matching features
            // use try-with-resources to ensure the reader is closed
            try (FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                         datastore.getFeatureReader(query, Transaction.AUTO_COMMIT)) {
                // loop through all results, only print out the first 10
                while (reader.hasNext()) {
                    Map<String,Object> map = new HashMap<>();
                    SimpleFeature feature = reader.next();

                    List<AttributeDescriptor> attributeDescriptors = feature.getType().getAttributeDescriptors();
                    List<Object> attributes = feature.getAttributes();
                    for (int i = 0; i < feature.getAttributeCount(); i++) {
                        String field = attributeDescriptors.get(i).getLocalName();
                        if(!"location_point".equals(field)) {
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
        System.out.println("Loading datastore");

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

    public SimpleFeatureType getSimpleFeatureType(TutorialData data) {
        return data.getSimpleFeatureType();
    }


    public void createSchema(DataStore datastore, SimpleFeatureType sft) throws IOException {
        System.out.println("Creating schema: " + DataUtilities.encodeType(sft));
        // we only need to do the once - however, calling it repeatedly is a no-op
        sft.getUserData().put("override.reserved.words",true);
        datastore.createSchema(sft);
        System.out.println();
    }
}
