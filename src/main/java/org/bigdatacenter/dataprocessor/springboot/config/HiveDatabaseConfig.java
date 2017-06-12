package org.bigdatacenter.dataprocessor.springboot.config;

import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;

/**
 * Created by hyuk0 on 2017-06-08.
 */
@Configuration
@MapperScan(value = "org.bigdatacenter.dataprocessor.platform.persistence.hive", sqlSessionFactoryRef = "hiveSqlSessionFactory")
@EnableTransactionManagement
public class HiveDatabaseConfig {
    @Bean(name = "hiveDataSource")
    @Primary
    @ConfigurationProperties(prefix = "spring.hive.datasource")
    public DataSource hiveDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name = "hiveSqlSessionFactory")
    @Primary
    public SqlSessionFactory hiveSqlSessionFactory(@Qualifier("hiveDataSource") DataSource hiveDataSource, ApplicationContext applicationContext) throws Exception {
        SqlSessionFactoryBean sqlSessionFactoryBean = new SqlSessionFactoryBean();
        sqlSessionFactoryBean.setDataSource(hiveDataSource);
        sqlSessionFactoryBean.setMapperLocations(applicationContext.getResources("classpath:mybatis/mapper/hive/*.xml"));
        return sqlSessionFactoryBean.getObject();
    }

    @Bean(name = "hiveSqlSessionTemplate")
    @Primary
    public SqlSessionTemplate hiveSqlSessionTemplate(SqlSessionFactory hiveSqlSessionFactory) throws Exception {
        return new SqlSessionTemplate(hiveSqlSessionFactory);
    }
}
