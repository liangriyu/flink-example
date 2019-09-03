package com.landy.flink.base.sink;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Properties;

/**
 * @author liangriyu
 * @description: phoenix sink 公共抽象类
 * @date 2019/6/18
 */
public abstract class MysqlSink<T> extends RichSinkFunction<T> {

    private DruidDataSource dataSource;
    private Connection connection;
    private Statement statement;

    private Properties prop =new Properties();

    public MysqlSink(Properties prop) {
        this.prop = prop;
    }

    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        statement = connection.createStatement();
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭连接和释放资源
        if(null != statement){
            statement.close();
        }
        if (connection != null) {
            connection.close();
        }
        if (dataSource != null){
            dataSource.close();
        }
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(T value, Context context) throws Exception {
        //组装数据，执行插入操作
        this.upsert2Phoenix(value);
    }

    private Connection getConnection() {
        Connection con = null;
        try {
            dataSource = this.getDataSource();
            con = dataSource.getConnection();
        } catch (Exception e) {
            System.out.println("获取数据连接异常 ---> "+ e.getMessage());
        }
        return con;
    }

    private DruidDataSource getDataSource(){
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setDriverClassName(prop.getProperty("mysql.driver"));
        dataSource.setUrl(prop.getProperty("mysql.url"));
        dataSource.setUsername(prop.getProperty("mysql.user"));
        dataSource.setPassword(prop.getProperty("mysql.password"));
        dataSource.setDefaultAutoCommit(Boolean.parseBoolean(prop.getProperty("mysql.auto-commit")));
//        dataSource.setValidationQuery("select 1");
        return dataSource;
    }

    /**
     * 写入Phoenix
     * @param t
     * @throws Exception
     */
    public abstract void upsert2Phoenix(T t);


}