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
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;

/**
 * Created by hyuk0 on 2017-06-08.
 */
@Configuration
@MapperScan(value = "org.bigdatacenter.dataprocessor.platform.persistence.metadb", sqlSessionFactoryRef = "metadbSqlSessionFactory")
@EnableTransactionManagement
public class MetaDatabaseConfig {
    @Bean(name = "metadbDataSource")
    @ConfigurationProperties(prefix = "spring.metadb.datasource")
    public DataSource metadbDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name = "metadbSqlSessionFactory")
    public SqlSessionFactory metadbSqlSessionFactory(@Qualifier("metadbDataSource") DataSource metadbDataSource, ApplicationContext applicationContext) throws Exception {
        SqlSessionFactoryBean sqlSessionFactoryBean = new SqlSessionFactoryBean();
        sqlSessionFactoryBean.setDataSource(metadbDataSource);
        sqlSessionFactoryBean.setMapperLocations(applicationContext.getResources("classpath:mybatis/mapper/metadb/*.xml"));
        return sqlSessionFactoryBean.getObject();
    }

    @Bean(name = "metadbSqlSessionTemplate")
    public SqlSessionTemplate metadbSqlSessionTemplate(SqlSessionFactory metadbSqlSessionFactory) throws Exception {
        return new SqlSessionTemplate(metadbSqlSessionFactory);
    }
}
