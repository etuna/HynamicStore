package com.kvs.hynamicstore.service;


import com.kvs.hynamicstore.model.Table;
import com.kvs.hynamicstore.model.Value;
import com.kvs.hynamicstore.util.Constant;
import org.apache.http.HttpEntity;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.hibernate.annotations.common.util.impl.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.stereotype.Service;

import java.io.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.nio.file.*;
import java.util.*;

@Service
public class DatabaseService {
    private Hashtable<String, Table> dbCache = new Hashtable<>();
    private Hashtable<String, Table> tmpDbHashTable = new Hashtable<>();
    private Table tmpTable = new Table();
    private Hashtable<String, Value> tmpHashTable = new Hashtable<>();


    private FileInputStream in = null;
    private ObjectInputStream ois = null;
    private FileOutputStream out = null;
    private ObjectOutputStream oos = null;
    private File file = new File(Constant.dbFile);
    private boolean uptodate = false;
    private static org.jboss.logging.Logger logger = LoggerFactory.logger(StorageService.class);


    public DatabaseService() throws IOException {
        tmpHashTable = new Hashtable<>();
        if (!file.exists()) {
            file.createNewFile();
        }
    }

    // Database
    public String createTable(String tableName) throws IOException, InterruptedException {
        Table table = new Table();
        table.setTableName(tableName);
        table.setReqCount(0);
        table.setTable(new Hashtable<String, Value>());
        int res = doCreateTable(tableName, table);
        syncWithMainDB();
        if (res == 0) {
            return String.format("Table %s succesfully created.", tableName);
        } else if (res == -1) {
            return String.format("Table %s is already available.", tableName);
        } else {
            return String.format("Error occured. Table: %s ", tableName);
        }
    }

    public int doCreateTable(String tableName, Table table) {
        try {
            in = new FileInputStream(file);
            if (in.getChannel().size() == 0) {
                tmpDbHashTable.put(tableName, table);
                out = new FileOutputStream(file);
                oos = new ObjectOutputStream(out);
                oos.writeObject(tmpDbHashTable);
                oos.close();
                out.close();
            } else {
                ois = new ObjectInputStream(in);
                tmpDbHashTable = (Hashtable) ois.readObject();
                tmpTable = tmpDbHashTable.get(tableName);
                in.close();
                ois.close();
                if (tmpTable == null) {
                    tmpDbHashTable.put(tableName, table);
                    out = new FileOutputStream(file);
                    oos = new ObjectOutputStream(out);
                    oos.writeObject(tmpDbHashTable);
                    oos.close();
                    out.close();
                } else {
                    return -1;
                }
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return 1;
        } catch (IOException e) {
            e.printStackTrace();
            return 1;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return 1;
        } finally {
            if (tmpDbHashTable != null) {
                tmpDbHashTable.clear();
            }
            if (tmpHashTable != null) {
                tmpHashTable.clear();
            }
        }
        return 0;
    }

    public String getTable(String tableName) {
        tmpTable = dbCache.get(tableName);
        if(tmpTable != null){
            try {
                in = new FileInputStream(file);
                if (in.getChannel().size() == 0) {
                    return "null";
                }
                ois = new ObjectInputStream(in);
                tmpDbHashTable = (Hashtable) ois.readObject();
                in.close();
                ois.close();
                out = new FileOutputStream(file);
                oos = new ObjectOutputStream(out);
                tmpTable.setReqCount(tmpTable.getReqCount() + 1);
                tmpDbHashTable.put(tableName, tmpTable);
                oos.writeObject(tmpDbHashTable);
                oos.close();
                out.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            syncCache();
            return "[CACHE]   "+tmpTable;
        }
        try {
            in = new FileInputStream(file);
            if (in.getChannel().size() == 0) {
                return null;
            }
            ois = new ObjectInputStream(in);
            tmpDbHashTable = (Hashtable) ois.readObject();
            tmpTable = tmpDbHashTable.get(tableName);
            in.close();
            ois.close();
            if (tmpTable == null) {
                return null;
            } else {
                out = new FileOutputStream(file);
                oos = new ObjectOutputStream(out);
                tmpTable.setReqCount(tmpTable.getReqCount() + 1);
                tmpDbHashTable.put(tableName, tmpTable);
                oos.writeObject(tmpDbHashTable);
                oos.close();
                out.close();
                syncCache();
                return "[DISK]   "+tmpTable;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String insertTable(String tableName, String key, String val) {
        try {
            in = new FileInputStream(file);
            if (in.getChannel().size() == 0) {
                return "null";
            }
            ois = new ObjectInputStream(in);
            tmpDbHashTable = (Hashtable) ois.readObject();
            tmpTable = tmpDbHashTable.get(tableName);
            if (tmpTable == null) {
                return "nul..";
            }
            tmpTable.getTable().put(key, new Value(val, 0));
            tmpDbHashTable.put(tableName, tmpTable);
            in.close();
            ois.close();

            out = new FileOutputStream(file);
            oos = new ObjectOutputStream(out);
            oos.writeObject(tmpDbHashTable);
            oos.close();
            out.close();
            syncWithMainDB();
            return "Success.";
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return "success";
    }


    public Hashtable<String, Table> getDbCache() {
        return dbCache;
    }

    public Hashtable<String, Value> getAll() {
        try {
            in = new FileInputStream(file);
            if (in.getChannel().size() == 0) {
                return null;
            }
            ois = new ObjectInputStream(in);
            tmpHashTable = (Hashtable) ois.readObject();
            in.close();
            ois.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            return tmpHashTable;
        }
    }

    public String getDB() {
        try {
            in = new FileInputStream(file);
            if (in.getChannel().size() == 0) {
                return "null";
            }
            ois = new ObjectInputStream(in);
            tmpDbHashTable = (Hashtable) ois.readObject();
            in.close();
            ois.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            return tmpDbHashTable.toString();
        }
    }


    public String get(String key) {
        Value v = dbCache.get(key).getTable().get(key);
        if (v == null) {
            try {
                in = new FileInputStream(Constant.dbFile);
                ois = new ObjectInputStream(in);
                tmpHashTable = (Hashtable) ois.readObject();
                v = tmpHashTable.get(key);
                in.close();
                ois.close();
                if (v == null) {
                    if(!uptodate){
                        fetchMainDB();
                        get(key);
                    }else {
                        return "Not Found with given Key :" + key;
                    }
                } else {
                    v.setReqCount(v.getReqCount() + 1);
                    tmpHashTable.put(key, v);
                    out = new FileOutputStream(Constant.dbFile);
                    oos = new ObjectOutputStream(out);
                    oos.writeObject(tmpHashTable);
                    oos.close();
                    out.close();
                    return "[DISK]" + v.getVal();
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } finally {
                tmpHashTable.clear();
                System.gc();
                syncCache();
            }
        } else {
            v.setReqCount(v.getReqCount() + 1);
            tmpHashTable.put(key, v);
            try {
                out = new FileOutputStream(Constant.dbFile);
                oos = new ObjectOutputStream(out);
                oos.writeObject(tmpHashTable);
                oos.close();
                out.close();
                return "[CACHE]" + v.getVal();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return v.getVal();
    }

    public String store(String key, String val) {
        try {
            in = new FileInputStream(file);
            if (in.getChannel().size() != 0) {
                ois = new ObjectInputStream(in);
                tmpHashTable = (Hashtable) ois.readObject();
                tmpHashTable.put(key, new Value(val, 0));
                in.close();
                ois.close();

                out = new FileOutputStream(file);
                oos = new ObjectOutputStream(out);
                oos.writeObject(tmpHashTable);
                oos.close();
                out.close();
            } else {
                tmpHashTable.put(key, new Value(val, 0));
                out = new FileOutputStream(file);
                oos = new ObjectOutputStream(out);
                oos.writeObject(tmpHashTable);
                oos.close();
                out.close();
            }


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            tmpHashTable.clear();
            System.gc();
            syncCache();
        }
        return "value";
    }

    public String del(String key, String val) {
        return "value";
    }

    public void start() throws IOException {
        fetchMainDB();
        populateCache();
    }

    public boolean populateCache() {
        try {
            in = new FileInputStream(file);
            ois = new ObjectInputStream(in);
            tmpDbHashTable = (Hashtable) ois.readObject();
            in.close();
            ois.close();
            fillCache(tmpDbHashTable);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return false;
        } finally {
            if (tmpDbHashTable != null) {
                tmpDbHashTable.clear();
            }
            System.gc();
            return true;
        }
    }

    public void fillCache(Hashtable<String, Table> db) {
        ArrayList<Map.Entry<String, Table>> list = new ArrayList<>(db.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, Table>>() {

            public int compare(Map.Entry<String, Table> o1, Map.Entry<String, Table> o2) {
                return Integer.compare(o2.getValue().getReqCount(), o1.getValue().getReqCount());
            }
        });
        dbCache.clear();
        for (int i = 0; i < list.size() / 2 + 1; i++) {
            dbCache.put(list.get(i).getKey(), list.get(i).getValue());
        }
    }

    public boolean syncCache() {
        try {
            in = new FileInputStream(file);
            ois = new ObjectInputStream(in);
            tmpHashTable = (Hashtable) ois.readObject();
            in.close();
            ois.close();
            fillCache(tmpDbHashTable);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return false;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return false;
        } finally {
            return true;
        }
    }

    public String selectWhere(String tableName, String key) {
        tmpTable = dbCache.get(tableName);
        if(tmpTable != null){
            Value v = tmpTable.getTable().get(key);
            if (v != null){
                try {
                    in = new FileInputStream(file);
                    if (in.getChannel().size() == 0) {
                        return "null";
                    }
                    ois = new ObjectInputStream(in);
                    tmpDbHashTable = (Hashtable) ois.readObject();
                    in.close();
                    ois.close();
                    out = new FileOutputStream(file);
                    oos = new ObjectOutputStream(out);
                    v.setReqCount(v.getReqCount() + 1);
                    tmpTable.put(key, v);
                    tmpTable.setReqCount(tmpTable.getReqCount() + 1);
                    tmpDbHashTable.put(tableName, tmpTable);
                    oos.writeObject(tmpDbHashTable);
                    oos.close();
                    out.close();
                    syncCache();
                    return "[CACHE]    " + v.toString();
                }catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }else {
                try {
                    in = new FileInputStream(file);
                    if (in.getChannel().size() == 0) {
                        return null;
                    }
                    ois = new ObjectInputStream(in);
                    tmpDbHashTable = (Hashtable) ois.readObject();
                    tmpTable = tmpDbHashTable.get(tableName);
                    if (tmpTable == null) {
                        return null;
                    } else {
                        v = tmpTable.getTable().get(key);
                        in.close();
                        ois.close();
                        if (v == null) {
                            if(!uptodate){
                                uptodate = true;
                                fetchMainDB();
                                return selectWhere(tableName, key);
                            }else {
                                return null;
                            }

                        } else {
                            out = new FileOutputStream(file);
                            oos = new ObjectOutputStream(out);
                            v.setReqCount(v.getReqCount()+1);
                            tmpTable.put(key,v);
                            tmpTable.setReqCount(tmpTable.getReqCount() + 1);
                            tmpDbHashTable.put(tableName, tmpTable);
                            oos.writeObject(tmpDbHashTable);
                            oos.close();
                            out.close();
                            syncCache();
                            return "[DISK]    "+v.toString();
                        }
                    }
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        } else {
            try {
                in = new FileInputStream(file);
                if (in.getChannel().size() == 0) {
                    return null;
                }
                ois = new ObjectInputStream(in);
                tmpDbHashTable = (Hashtable) ois.readObject();
                tmpTable = tmpDbHashTable.get(tableName);
                if (tmpTable == null) {
                    return null;
                } else {
                    Value v = tmpTable.getTable().get(key);
                    in.close();
                    ois.close();
                    if (v == null) {
                        if (!uptodate){
                            uptodate = true;
                            fetchMainDB();
                            return selectWhere(tableName, key);
                        }else {
                            return null;
                        }
                    } else {
                        out = new FileOutputStream(file);
                        oos = new ObjectOutputStream(out);
                        v.setReqCount(v.getReqCount()+1);
                        tmpTable.put(key,v);
                        tmpTable.setReqCount(tmpTable.getReqCount() + 1);
                        tmpDbHashTable.put(tableName, tmpTable);
                        oos.writeObject(tmpDbHashTable);
                        oos.close();
                        out.close();
                        syncCache();
                        return "[DISK]    "+v.toString();
                    }
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } finally {
                uptodate = false;
            }
        }
        return null;
    }



    public String fetchMainDB() throws IOException {
        CloseableHttpClient httpClient = HttpClients.createDefault();

        HttpGet request = new HttpGet("http://127.0.0.1:9971/api/syncfetch");
        HttpResponse response = httpClient.execute(request);


        HttpEntity entity =  response.getEntity();
        //FileInputStream is = (FileInputStream) entity.getContent();
        Files.copy(entity.getContent(), Path.of(Constant.dbFile), StandardCopyOption.REPLACE_EXISTING);
        logger.info(tmpHashTable);
        uptodate = false;

        return "OK";
    }

    public void syncWithMainDB() throws IOException, InterruptedException {
        HttpClient httpClient = HttpClient.newHttpClient();
        Path file = Paths.get(Constant.dbFile);
        Resource resource = new UrlResource(file.toUri());

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:9971/api/synccommit"))
                .POST(HttpRequest.BodyPublishers.ofFile(Paths.get(Constant.dbFile)))
                //.headers(HttpHeaders.CONTENT_TYPE, "multipart/form-data")
                .build();

        java.net.http.HttpResponse res = httpClient.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());

    }

    public String sync(byte[] file) throws IOException {
        OutputStream os = new FileOutputStream(new File(Constant.dbFile));
        os.write(file);
        // Files.copy(file, Path.of("testdb.xml"), StandardCopyOption.REPLACE_EXISTING);
        return "OK";
    }
}

