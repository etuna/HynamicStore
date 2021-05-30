package com.kvs.hynamicstore.startup;


import com.kvs.hynamicstore.service.DatabaseService;
import com.kvs.hynamicstore.service.StorageService;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class Startup implements InitializingBean {

    @Autowired
    public StorageService storageService;

    @Autowired
    public DatabaseService databaseService;

    @Override
    public void afterPropertiesSet() throws Exception {
       // storageService.start();
        databaseService.start();
    }
}
