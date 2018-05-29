# redis-store
>一个分布式redis代理(基于客户端代理)

#### 如何开始
1.  下载 安装 启动zookeeper
2.  zk初始化根节点[/clusters] [/clients/REDIS]
3.  下载 安装 启动Redis
4.  参考 redis-store-core/Main.java 给zookeeper中注册cluster node instance节点
5.  使用 redis-store-core/StoreDaoFactory.java 获取IRedisDao执行测试
