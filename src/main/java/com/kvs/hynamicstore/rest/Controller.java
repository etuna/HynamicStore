package com.kvs.hynamicstore.rest;


import com.kvs.hynamicstore.model.Value;
import com.kvs.hynamicstore.service.StorageService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Hashtable;

@RestController
@RequestMapping("api")
public class Controller {

    @Autowired
    StorageService storageService;


    private static Logger logger = LoggerFactory.getLogger(Controller.class);

    @GetMapping("test")
    public String test(){
        return "test successful";
    }

    @GetMapping("get")
    public String get(@RequestParam String key){
        logger.info("New GET request arrived. Key:"+key);
        return storageService.get(key);
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
}
