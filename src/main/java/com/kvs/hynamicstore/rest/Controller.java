package com.kvs.hynamicstore.rest;


import com.kvs.hynamicstore.model.Table;
import com.kvs.hynamicstore.model.Value;
import com.kvs.hynamicstore.service.DatabaseService;
import com.kvs.hynamicstore.service.StorageService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Hashtable;

@RestController
@RequestMapping("api")
public class Controller {

    @Autowired
    StorageService storageService;

    @Autowired
    DatabaseService databaseService;


    private static Logger logger = LoggerFactory.getLogger(Controller.class);

    @GetMapping("test")
    public String test(){
        return "test successful";
    }

    @GetMapping("get")
    public String get(@RequestParam String key){
        Instant reqArrv = Instant.now();
        logger.info("New GET request arrived. Key:"+key);
        String res = storageService.get(key);
        Instant reqProcd = Instant.now();
        return storageService.get(key)+ " (Response duration:"+ Duration.between(reqArrv, reqProcd).toMillis()+")";
    }
    @GetMapping("getAll")
    public String getAll(){
        logger.info("New GET-ALL request arrived.");
        Hashtable<String, Value> hashtable = storageService.getAll();
        logger.info("Hashtable : "+ hashtable);
        String hashTableStr = hashtable.toString();
        hashtable = null;
        return hashTableStr;
    }
    @GetMapping("cache")
    public String getCache(){
        logger.info("New CACHE request arrived.");
        Hashtable<String, Table> cache = databaseService.getDbCache();
        return cache.toString();
    }


    @GetMapping("store")
    public String store(@RequestParam String key,@RequestParam String value ){
        logger.info(String.format("New STORE request arrived. Key:%s, Value:%s", key,value));
        return storageService.store(key,value);
    }

    @GetMapping("del")
    public String del(@RequestParam String key,@RequestParam String value ){
        logger.info(String.format("New DEL request arrived. Key:%s, Value:%s", key,value));
        return storageService.del(key,value);
    }
    @GetMapping("create-table")
    public String createTable(@RequestParam String tableName ) throws IOException, InterruptedException {
        logger.info(String.format("New CREATE-TABLE request arrived. tableName:%s",tableName));
        return databaseService.createTable(tableName);
    }
    @GetMapping("get-db")
    public String getDb(){
        logger.info(String.format("New GET-DB request arrived."));
        return databaseService.getDB();
    }
    @GetMapping("get-table")
    public String getTable(@RequestParam String tableName){
        Instant reqArrv = Instant.now();
        logger.info(String.format("New GET-TABLE request arrived. TableName:"+tableName));
        String res =  databaseService.getTable(tableName) ;
        Instant reqProcd = Instant.now();
        return res + System.lineSeparator() +"(Response duration:"+ Duration.between(reqArrv, reqProcd).toMillis()+")";
    }
    @GetMapping("insert-table")
    public String insertTable(@RequestParam String tableName, @RequestParam String key, @RequestParam String val){
        logger.info(String.format("New INSERT-TABLE request arrived. TableName:%s, Key:%s, Val:%s",tableName,key,val));
        return databaseService.insertTable(tableName, key, val);
    }
    @GetMapping("select-where")
    public String selectWhere(@RequestParam String tableName, @RequestParam String key){
        Instant reqArrv = Instant.now();
        logger.info(String.format("New SELECT-WHERE request arrived. TableName:%s, Key:%s",tableName,key));
        String res = databaseService.selectWhere(tableName, key);
        Instant reqProcd = Instant.now();
        return res + System.lineSeparator() +"(Response duration:"+ Duration.between(reqArrv, reqProcd).toMillis()+")";
    }
    @GetMapping("dbcache")
    public String getDbCache(){
        logger.info(String.format("New DB-CACHE request arrived."));
        return databaseService.getDbCache().toString();
    }

    @GetMapping("fetchtest")
    public String fetchtest() throws IOException {
        logger.info(String.format("New FETCH-TEST request arrived."));
        return storageService.fetchMainDB();
    }

}
