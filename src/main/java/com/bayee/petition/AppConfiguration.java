package com.bayee.petition;

import com.bayee.petition.mapper.DataInfoMapper;
import com.bayee.petition.mapper.HBaseMapper;
import com.bayee.petition.mapper.HbCatlogMapper;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.web.multipart.commons.CommonsMultipartResolver;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import com.bayee.petition.configuration.DispatcherConfig;
import com.bayee.petition.configuration.ThymeleafConfig;

@Configuration
@EnableWebMvc
@PropertySource({ "classpath:db.properties" })
@ComponentScan(basePackages = "com.bayee.petition")
@Import({ WebInitializer.class, DispatcherConfig.class, ThymeleafConfig.class })
public class AppConfiguration {

	@Autowired
	private Environment env;

	@Bean
	public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
		return new PropertySourcesPlaceholderConfigurer();
	}

	@Bean
	public BasicDataSource dataSource() {
		BasicDataSource dataSource = new BasicDataSource();
		dataSource.setDriverClassName(env.getProperty("jdbc.driverClassName"));
		dataSource.setUrl(env.getProperty("jdbc.url"));
		dataSource.setUsername(env.getProperty("jdbc.username"));
		dataSource.setPassword(env.getProperty("jdbc.password"));
		dataSource.setDefaultAutoCommit(true);
		return dataSource;
	}

	@Bean
	public BasicDataSource dataSource1() {
		BasicDataSource dataSource = new BasicDataSource();
		dataSource.setDriverClassName(env.getProperty("phoenix.driver"));
		dataSource.setUrl(env.getProperty("phoenix.url"));
		dataSource.setDefaultAutoCommit(true);
		return dataSource;
	}

	@Bean
	public DataSourceTransactionManager transactionManager() {
		DataSourceTransactionManager txManager = new DataSourceTransactionManager();
		txManager.setDataSource(dataSource());
		return txManager;
	}

	@Bean
	public DataSourceTransactionManager transactionManager1() {
		DataSourceTransactionManager txManager = new DataSourceTransactionManager();
		txManager.setDataSource(dataSource1());
		return txManager;
	}

	@Bean(name = "multipartResolver")
	public CommonsMultipartResolver commonsMultipartResolver() {
		CommonsMultipartResolver commonsMultipartResolver = new CommonsMultipartResolver();
		commonsMultipartResolver.setMaxUploadSize(100000000);
		return commonsMultipartResolver;
	}

	@Bean
	public SqlSessionFactory sqlSessionFactory() throws Exception {
		SqlSessionFactoryBean sqlSessionFactory = new SqlSessionFactoryBean();
		sqlSessionFactory.setDataSource(dataSource());
		sqlSessionFactory.getObject().getConfiguration().addMapper(HbCatlogMapper.class);
		sqlSessionFactory.getObject().getConfiguration().addMapper(DataInfoMapper.class);
		return sqlSessionFactory.getObject();
	}

	@Bean
	public SqlSessionFactory sqlSessionFactory1() throws Exception {
		SqlSessionFactoryBean sqlSessionFactory = new SqlSessionFactoryBean();
		sqlSessionFactory.setDataSource(dataSource1());
		sqlSessionFactory.getObject().getConfiguration().addMapper(HBaseMapper.class);
		return sqlSessionFactory.getObject();
	}

	@Bean
	public HbCatlogMapper hbCatlogMapper() throws Exception {
      SqlSessionTemplate sessionTemplate = new SqlSessionTemplate(sqlSessionFactory());
      return sessionTemplate.getMapper(HbCatlogMapper.class);
   }

	@Bean
	public HBaseMapper hbaseMapper() throws Exception {
		SqlSessionTemplate sessionTemplate = new SqlSessionTemplate(sqlSessionFactory1());
		return sessionTemplate.getMapper(HBaseMapper.class);
	}

	@Bean
	public DataInfoMapper dataInfoMapper() throws Exception {
		SqlSessionTemplate sessionTemplate = new SqlSessionTemplate(sqlSessionFactory());
		return sessionTemplate.getMapper(DataInfoMapper.class);
	}
}
