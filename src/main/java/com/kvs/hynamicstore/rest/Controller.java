package com.kvs.hynamicstore.rest;


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
        Hashtable<String, Value> cache = storageService.getCache();
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
    public String createTable(@RequestParam String key,@RequestParam String tableName, @RequestParam String args ){
        logger.info(String.format("New CREATE-TABLE request arrived. Key:%s, tableName:%s, args:%s", key,tableName,args));
        return databaseService.createTable(key,tableName,args);
    }

}
