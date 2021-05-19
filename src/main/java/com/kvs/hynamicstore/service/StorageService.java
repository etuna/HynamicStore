package com.kvs.hynamicstore.service;


import com.kvs.hynamicstore.model.Value;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.*;

@Service
public class StorageService {

    private Hashtable<String, Value> cache = new Hashtable<>();
    private FileInputStream in = null;
    private ObjectInputStream ois = null;
    private FileOutputStream out = null;
    private ObjectOutputStream oos = null;
    private Hashtable<String, Value> tmpHashTable;
    private File file = new File("kvstore.xml");

    public StorageService() throws IOException {
        tmpHashTable = new Hashtable<>();
        if (!file.exists()){
            file.createNewFile();
        }
    }


    public Hashtable<String, Value> getCache() {
        return cache;
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


    public String get(String key){
        Value v = cache.get(key);
        if (v == null) {
            try {
                in = new FileInputStream("kvstore.xml");
                ois = new ObjectInputStream(in);
                tmpHashTable = (Hashtable) ois.readObject();
                v = tmpHashTable.get(key);
                in.close();
                ois.close();
                if ( v == null) {
                    return "Not Found with given Key :"+key;
                } else {
                    v.setReqCount(v.getReqCount()+1);
                    tmpHashTable.put(key, v);
                    out = new FileOutputStream("kvstore.xml");
                    oos = new ObjectOutputStream(out);
                    oos.writeObject(tmpHashTable);
                    oos.close();
                    out.close();
                    return v.getVal();
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }finally {
                tmpHashTable.clear();
                System.gc();
                syncCache();
            }
        } else {
            v.setReqCount(v.getReqCount()+1);
            tmpHashTable.put(key, v);
            try {
                out = new FileOutputStream("kvstore.xml");
                oos = new ObjectOutputStream(out);
                oos.writeObject(tmpHashTable);
                oos.close();
                out.close();
                return v.getVal();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return v.getVal();
    }
    public String store(String key, String val){
        try {
            in = new FileInputStream("kvstore.xml");
            if (in.getChannel().size() != 0){
                ois = new ObjectInputStream(in);
                tmpHashTable = (Hashtable) ois.readObject();
                tmpHashTable.put(key, new Value(val, 0));
                in.close();
                ois.close();

                out = new FileOutputStream("kvstore.xml");
                oos = new ObjectOutputStream(out);
                oos.writeObject(tmpHashTable);
                oos.close();
                out.close();
            }else {
                tmpHashTable.put(key, new Value(val, 0));
                out = new FileOutputStream("kvstore.xml");
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
    public String del(String key, String val){
        return "value";
    }

    public void start() {
        populateCache();
    }
    public boolean populateCache(){
        try {
            in = new FileInputStream("kvstore.xml");
            ois = new ObjectInputStream(in);
            tmpHashTable = (Hashtable) ois.readObject();
            in.close();
            ois.close();
            fillCache(tmpHashTable);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return false;
        }finally {
            tmpHashTable.clear();
            System.gc();
            return true;
        }
    }

    public void fillCache(Hashtable<String, Value> hashtable){
        ArrayList<Map.Entry<String, Value>> list = new ArrayList<>(hashtable.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, Value>>(){

            public int compare(Map.Entry<String,Value> o1, Map.Entry<String,Value> o2) {
                return Integer.compare(o2.getValue().getReqCount(), o1.getValue().getReqCount());
            }});
        cache.clear();
        for(int i = 0; i<list.size()/2+1; i++){
            cache.put(list.get(i).getKey(),list.get(i).getValue());
        }
    }
    public boolean syncCache(){
        try {
            in = new FileInputStream("kvstore.xml");
            ois = new ObjectInputStream(in);
            tmpHashTable = (Hashtable) ois.readObject();
            in.close();
            ois.close();
            fillCache(tmpHashTable);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return false;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return false;
        }finally {
            return true;
        }
    }

}
