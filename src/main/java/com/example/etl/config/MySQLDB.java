package com.example.etl.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;

/**
 * Created by wangweirong05 on 2017-09-18.
 */
@Configuration
@EnableTransactionManagement
@SpringBootApplication
public class MySQLDB {

    /**
     * Bean
     * @return
     */
    @Bean(name="gd4400DS")
    @Qualifier("gd4400DS")
    @Primary
    @ConfigurationProperties(prefix="spring.datasource.gd4400db")
    public DataSource gd4400DS() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name="bbshopDS")
    @Qualifier("bbshopDS")
    @ConfigurationProperties(prefix="spring.datasource.bbshop")
    public DataSource bbshopDS() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name="fsbbshopDS")
    @Qualifier("fsbbshopDS")
    @ConfigurationProperties(prefix="spring.datasource.fsbbshop")
    public DataSource fsbbshopDS() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name="picczqDS")
    @Qualifier("picczqDS")
    @ConfigurationProperties(prefix="spring.datasource.picczq")
    public DataSource picczqDS() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name="vacationDS")
    @Qualifier("vacationDS")
    @ConfigurationProperties(prefix="spring.datasource.vacation")
    public DataSource vacationDS() {
        return DataSourceBuilder.create().build();
    }



    /**
     * Template
     * @param dataSource
     * @return
     */

    @Bean(name = "gd4400dbConversion")
    public JdbcTemplate gd4400dbConversion(
            @Qualifier("gd4400DS")DataSource dataSource){
        return new JdbcTemplate(dataSource);
    }

    @Bean(name = "bbshop")
    public JdbcTemplate bbshop(
            @Qualifier("bbshopDS")DataSource dataSource){
        return new JdbcTemplate(dataSource);
    }

    @Bean(name = "fsbbshop")
    public JdbcTemplate fsbbshop(
            @Qualifier("fsbbshopDS")DataSource dataSource){
        return new JdbcTemplate(dataSource);
    }

    @Bean(name = "picczq")
    public JdbcTemplate picczq(
            @Qualifier("picczqDS")DataSource dataSource){
        return new JdbcTemplate(dataSource);
    }
    @Bean(name = "vacation")
    public JdbcTemplate vacation(
            @Qualifier("vacationDS")DataSource dataSource){
        return new JdbcTemplate(dataSource);
    }



    //事务管理
//    @Bean
//    public PlatformTransactionManager devTransactionManager(@Qualifier("vacationDS") DataSource sitDataSource) {
//        return new DataSourceTransactionManager(sitDataSource);
//    }


}
