/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.worldfonecc.access;

import com.google.gson.Gson;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSDownloadStream;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.util.JSON;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author MitsuyoRai
 */
public class MongoUtils {

    final static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("CrmEvent");

    private String db_host = "127.0.0.1";
    private int db_port = 27017;
    private String db_user = "admin";
    private String db_pwd = "admin";
    private String db_authdb = "admin";
    private String db_name = "sipcloud";

    private MongoClient mongoClient = null;
    private MongoDatabase db = null;

    private Document searchBuilder = new Document();
    private Document whereBuilder = new Document();
    private Document sortBuilder = new Document();
    private Document updateBuilder = new Document();
    private Document groupBuilder = new Document();
    private Integer skipBuilder = 0;
    private Integer limitBuilder = 1000;

    public MongoUtils() {
        try {
            File file1 = new File(System.getProperty("user.dir"));
            URL[] urls = {file1.toURI().toURL()};
            ClassLoader loader = new URLClassLoader(urls);
            ResourceBundle rb = ResourceBundle.getBundle("PBX", Locale.getDefault(), loader);
            if (rb.containsKey("mongodb.host")) {
                this.db_host = rb.getString("mongodb.host");
            } else {
                logger.error("Missing mongodb.host");
            }
            if (rb.containsKey("mongodb.port")) {
                this.db_port = Integer.parseInt(rb.getString("mongodb.port"));
            } else {
                logger.error("Missing mongodb.port");
            }
            if (rb.containsKey("mongodb.user")) {
                this.db_user = rb.getString("mongodb.user");
            } else {
                logger.error("Missing mongodb.user");
            }
            if (rb.containsKey("mongodb.pass")) {
                this.db_pwd = rb.getString("mongodb.pass");
            } else {
                logger.error("Missing mongodb.pass");
            }
            if (rb.containsKey("mongodb.authDB")) {
                this.db_authdb = rb.getString("mongodb.authDB");
            } else {
                logger.error("Missing mongodb.authDB");
            }
            if (rb.containsKey("mongodb.dbname")) {
                this.db_name = rb.getString("mongodb.dbname");
            } else {
                logger.error("Missing mongodb.dbname");
            }
            if (!"".equals(this.db_user) && !"".equals(this.db_pwd)) {
                MongoCredential credential = MongoCredential.createCredential(this.db_user, this.db_authdb, this.db_pwd.toCharArray());
                mongoClient = new MongoClient(new ServerAddress(this.db_host, this.db_port), Arrays.asList(credential));
            } else {
                mongoClient = new MongoClient(this.db_host, this.db_port);
            }
            // -------------------------------------------------------------------------------------------------
            db = mongoClient.getDatabase(this.db_name);
            // -------------------------------------------------------------------------------------------------
        } catch (MalformedURLException ex) {
            logger.error(ex);
        }
    }

    public void close() {
        try {
            if (mongoClient != null) {
                mongoClient.close();
            }
        } catch (Throwable ex) {
            logger.error("Mongo Throwable", ex);
        }
    }

    public void switch_db(String dbName) {
        db = mongoClient.getDatabase(dbName);
    }

    public MongoUtils select(Map<String, Object> select) {
        select.keySet().forEach(key -> {
            searchBuilder.put(key, select.get(key));
        });
        return this;
    }

    public MongoUtils group(String key, Object value) {
        groupBuilder.append(key, value);
        return this;
    }

    public MongoUtils group(Map<String, Object> groups) {
        groups.keySet().forEach(key -> {
            groupBuilder.append(key, groups.get(key));
        });
        return this;
    }

    public MongoUtils where(String key, Object value) {
        if ("_id".equalsIgnoreCase(key)) {
            whereBuilder.put("_id", new ObjectId(value.toString()));
        } else {
            whereBuilder.put(key, value);
        }
        return this;
    }

    public MongoUtils where(Map<String, Object> where) {
        where.keySet().forEach((key) -> {
            if ("_id".equalsIgnoreCase(key)) {
                whereBuilder.put("_id", new ObjectId(where.get(key).toString()));
            } else {
                whereBuilder.put(key, where.get(key));
            }
        });
        return this;
    }

    public MongoUtils order_by(String key, String value) {
        if ("asc".equalsIgnoreCase(value)) {
            sortBuilder.append(key, 1);
        } else if ("desc".equalsIgnoreCase(value)) {
            sortBuilder.append(key, -1);
        }
        return this;
    }

    public MongoUtils order_by(String key, Integer value) {
        sortBuilder.put(key, value);
        return this;
    }

    public MongoUtils order_by(Map<String, Integer> sort) {
        sort.keySet().forEach(key -> {
            sortBuilder.put(key, sort.get(key));
        });
        return this;
    }

    public MongoUtils offset(Integer offset) {
        skipBuilder = offset;
        return this;
    }

    public MongoUtils limit(Integer limit) {
        limitBuilder = limit;
        return this;
    }

    public MongoUtils set(String key, Object value) {
        updateBuilder.append("$set", new Document(key, value));
        return this;
    }

    public MongoUtils set(Map<String, Object> set) {
        set.keySet().forEach((key) -> {
            updateBuilder.append("$set", new Document(key, set.get(key)));
        });

        return this;
    }

    public MongoUtils inc(String key, Integer value) {
        updateBuilder.append("$inc", new Document(key, value));
        return this;
    }

    public MongoUtils inc(Map<String, Integer> inc) {
        inc.keySet().forEach((key) -> {
            updateBuilder.append("$inc", new Document(key, inc.get(key)));
        });
        return this;
    }

    public MongoUtils unset(String unset) {
        updateBuilder.append("$unset", new Document(unset, 1));
        return this;
    }

    public MongoUtils unset(List<String> unsets) {
        unsets.forEach(unset -> {
            updateBuilder.append("$unset", new Document(unset, 1));
        });
        return this;
    }

    public MongoUtils addToSet(String key, Object value) {
        updateBuilder.append("$addToSet", new Document(key, value));
        return this;
    }

    public MongoUtils addToSet(Map<String, Object> addtosets) {
        addtosets.keySet().forEach(key -> {
            updateBuilder.append("$addToSet", new Document(key, addtosets.get(key)));
        });
        return this;
    }

    public MongoUtils push(String field, String key, Object value) {
        updateBuilder.append("$push", new Document(field, new Document(key, value)));
        return this;
    }

    public MongoUtils push(String field, String[] keys, Object[] values) {
        for (int i = 0; i < keys.length; i++) {
            updateBuilder.append("$push", new Document(field, new Document(keys[i], values[i])));
        }
        return this;
    }

    public MongoUtils push(String field, String push) {
        updateBuilder.append("$push", new Document(field, push));
        return this;
    }

    public MongoUtils push(String field, Map<String, Object> push) {
        push.keySet().forEach(key -> {
            updateBuilder.append("$push", new Document(field, new Document(key, push.get(key))));
        });
        return this;
    }

    public MongoCursor<Object> distinct(String collection, String fieldName) {
        try {
            MongoCollection<Document> dept = db.getCollection(collection);
            // Get 1 Document
            MongoCursor<Object> c = dept.distinct(fieldName, Object.class).iterator();
            _clear();
            return c;
        } catch (Exception ex) {
            return null;
        }
    }

    public JSONObject getOne(String collection) {
        try {
            MongoCollection<Document> dept = db.getCollection(collection);
            // Get 1 Document
            Document doc = dept.find(whereBuilder).first();
            _clear();
            return new JSONObject(JSON.serialize(doc));
        } catch (JSONException ex) {
            return null;
        }
    }

    public List<JSONObject> get(String collection) {
        try {
            MongoCollection<Document> dept = db.getCollection(collection);
            // Get Document All
            MongoCursor<Document> docs = dept.find(whereBuilder).projection(searchBuilder).sort(sortBuilder).iterator();
            _clear();
            List<JSONObject> list = new ArrayList<>();
            while (docs.hasNext()) {
                list.add(new JSONObject(JSON.serialize(docs.next())));
            }
            return list;
        } catch (JSONException ex) {
            return null;
        }
    }

    public Document insert(String collection, Document doc) {
        try {
            MongoCollection<Document> dept = db.getCollection(collection);
            // Insert 1 Document
            dept.insertOne(doc);
            _clear();
            return doc;
        } catch (Exception ex) {
            return null;
        }
    }

    public Document insert(String collection, JSONObject jsonObj) {
        try {
            MongoCollection<Document> dept = db.getCollection(collection);
            // Insert 1 Document
            Document data = Document.parse(jsonObj.toString());
            dept.insertOne(data);
            _clear();
            return data;
        } catch (Exception ex) {
            return null;
        }
    }

    public List<Document> batch_insert(String collection, List<Document> docs) {
        try {
            MongoCollection<Document> dept = db.getCollection(collection);
            // Insert Many Document
            dept.insertMany(docs);
            _clear();
            return docs;
        } catch (Exception ex) {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public List<Document> batch_insert(String collection, JSONArray jsonArray) {
        try {
            MongoCollection<Document> dept = db.getCollection(collection);
            // Insert Many Document
            List<Document> list = (List<Document>) Document.parse(jsonArray.toString());
            dept.insertMany(list);
            _clear();
            return list;
        } catch (Exception ex) {
            return null;
        }
    }

    public String uploadFile(File file) {
        try {
            GridFSBucket gridFSFilesBucket = GridFSBuckets.create(db, "images");
            InputStream streamToUploadFrom = new FileInputStream(file);
            // Create some custom options
            GridFSUploadOptions options = new GridFSUploadOptions()
                    //		                                        .chunkSizeBytes(358400)
                    .metadata(new Document("type", "presentation"));

            ObjectId fileId = gridFSFilesBucket.uploadFromStream(file.getName(), streamToUploadFrom, options);
            return fileId.toString();
        } catch (FileNotFoundException e) {
            // handle exception
            return null;
        } catch (Exception ex) {
            return null;
        }
    }

    public byte[] getFile(String id) {
        try {
            GridFSBucket gridFSFilesBucket = GridFSBuckets.create(db, "images");
            byte[] bytesToWriteTo;
            try (GridFSDownloadStream downloadStream = gridFSFilesBucket.openDownloadStream(new ObjectId(id))) {
                int fileLength = (int) downloadStream.getGridFSFile().getLength();
                bytesToWriteTo = new byte[fileLength];
                downloadStream.read(bytesToWriteTo);
            }
            return bytesToWriteTo;
        } catch (Exception ex) {
            return null;
        }
    }

    public UpdateResult update(String collection) {
        try {
            MongoCollection<Document> dept = db.getCollection(collection);
            if (updateBuilder != null && !updateBuilder.isEmpty()) {
                // Update Document
                UpdateResult result = dept.updateOne(whereBuilder, updateBuilder);
                _clear();
                return result;
            }
            return null;
        } catch (Exception ex) {
            return null;
        }
    }

    public UpdateResult update_all(String collection) {
        try {
            MongoCollection<Document> dept = db.getCollection(collection);
            if (updateBuilder != null && !updateBuilder.isEmpty()) {
                // Update All Document
                UpdateResult result = dept.updateMany(whereBuilder, updateBuilder);
                _clear();
                return result;
            }
            return null;
        } catch (Exception ex) {
            return null;
        }
    }

    public DeleteResult delete(String collection) {
        try {
            MongoCollection<Document> dept = db.getCollection(collection);
            DeleteResult result = dept.deleteOne(whereBuilder);
            _clear();
            return result;
        } catch (Exception ex) {
            return null;
        }
    }

    public DeleteResult delete_all(String collection) {
        try {
            MongoCollection<Document> dept = db.getCollection(collection);
            DeleteResult result = dept.deleteMany(whereBuilder);
            _clear();
            return result;
        } catch (Exception ex) {
            return null;
        }
    }

    public void drop_db(String collection) {
        try {
            MongoCollection<Document> dept = db.getCollection(collection);
            dept.drop();
        } catch (Exception ex) {
        }
    }

    public void add_index(String collection, String name) {
        try {
            MongoCollection<Document> dept = db.getCollection(collection);
            dept.createIndex(new Document(collection, name));
        } catch (Exception ex) {
        }

    }

    public void remove_index(String collection, String name) {
        try {
            MongoCollection<Document> dept = db.getCollection(collection);
            dept.dropIndex(new Document(collection, name));
        } catch (Exception ex) {
        }
    }

    public void remove_all_index(String collection) {
        try {
            MongoCollection<Document> dept = db.getCollection(collection);
            dept.dropIndexes();
        } catch (Exception ex) {
        }
    }

    public JSONObject list_indexes(String collection) {
        try {
            MongoCollection<Document> dept = db.getCollection(collection);
            ListIndexesIterable<Document> lists = dept.listIndexes();
            return new JSONObject(new Gson().toJson(lists));
        } catch (JSONException ex) {
            return null;
        }
    }

    public List<JSONObject> aggregate(String collection) {
        try {
            MongoCollection<Document> dept = db.getCollection(collection);
            List<Document> pipeline = new ArrayList<>();
            if (!whereBuilder.isEmpty()) {
                Document withMatch = new Document();
                withMatch.put("$match", whereBuilder);
                pipeline.add(withMatch);
            }

            if (!groupBuilder.isEmpty()) {
                Document withGroup = new Document();
                withGroup.put("$group", groupBuilder);
                pipeline.add(withGroup);
            }

            if (!sortBuilder.isEmpty()) {
                Document withSort = new Document();
                withSort.put("$sort", sortBuilder);
                pipeline.add(withSort);
            }

            if (limitBuilder != null) {
                Document withLimit = new Document();
                withLimit.put("$limit", limitBuilder);
                pipeline.add(withLimit);
            }

            if (skipBuilder != null) {
                Document withSkip = new Document();
                withSkip.put("$skip", skipBuilder);
                pipeline.add(withSkip);
            }
            AggregateIterable<Document> results = dept.aggregate(pipeline);
            _clear();
            List<JSONObject> list = new ArrayList<>();
            for (Document obj : results) {
                list.add(new JSONObject(JSON.serialize(obj)));
            }
            return list;
        } catch (JSONException ex) {
            return null;
        }
    }

    public List<JSONObject> aggregate_pipeline(String collection, JSONArray jsonArray) {
        try {
            MongoCollection<Document> dept = db.getCollection(collection);
            List<Document> pipeline = new ArrayList<>();
            for (int i = 0; i < jsonArray.length(); i++) {
                Document key = (Document) JSON.parse(jsonArray.getJSONObject(i).toString());
                pipeline.add(key);
            }
            AggregateIterable<Document> results = dept.aggregate(pipeline);
            _clear();
            List<JSONObject> list = new ArrayList<>();
            for (Document obj : results) {
                list.add(new JSONObject(JSON.serialize(obj)));
            }
            return list;
        } catch (JSONException ex) {
            return null;
        }
    }

    public long count(String collection) {
        try {
            MongoCollection<Document> dept = db.getCollection(collection);
            long count = dept.count(whereBuilder);
            _clear();
            return count;
        } catch (Exception ex) {
            return 0;
        }
    }

    private void _clear() {
        searchBuilder = new Document();
        whereBuilder = new Document();
        sortBuilder = new Document();
        updateBuilder = new Document();
        groupBuilder = new Document();
        skipBuilder = 0;
        limitBuilder = 1000;
    }

}
