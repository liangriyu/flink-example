package com.landy.flink.base.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;

import java.util.Properties;

/**
 * @author liangriyu
 * @description: hbase sink 公共抽象类
 * @date 2019/6/18
 */
public abstract class HbaseSink<T> extends RichSinkFunction<T> {

    private Connection connection;

    private Properties prop;

    private String tableName;

    public HbaseSink(String tableName, Properties prop) {
        this.tableName = tableName;
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
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭连接和释放资源
        if (connection != null) {
            connection.close();
        }
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param t
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(T t, Context context) throws Exception {
        //组装数据，执行插入操作
        write2HBase(t);
    }

    public abstract void write2HBase(T t) throws Exception;

    private Connection getConnection() {
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(getHBaseConfiguration());
        } catch (Exception e) {
            System.out.println("获取数据连接异常 ---> "+ e.getMessage());
        }
        return connection;
    }

    public org.apache.hadoop.conf.Configuration getHBaseConfiguration(){
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", prop.getProperty("hbase.zookeeper.quorum"));
        config.set("hbase.zookeeper.property.clientPort", prop.getProperty("hbase.zookeeper.property.clientPort"));
        config.set("zookeeper.znode.parent", prop.getProperty("zookeeper.znode.parent"));
        config.setInt("hbase.rpc.timeout", Integer.parseInt(prop.getProperty("hbase.rpc.timeout")));
        config.setInt("hbase.client.operation.timeout", Integer.parseInt(prop.getProperty("hbase.client.operation.timeout")));
        config.setInt("hbase.client.scanner.timeout.period", Integer.parseInt(prop.getProperty("hbase.client.scanner.timeout.period")));
        return config;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}