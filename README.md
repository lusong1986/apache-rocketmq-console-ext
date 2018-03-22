**Apache RocketMQ控制台，支持apache rocketmq4.2.0及以上版本，新功能点：**


**1.支持灰度下线、上线消费者**

**2.支持查询生产组列表信息、生产组链接**

**3.支持查询消费组列表信息，消费者链接、消费者绑定的queue、消费者消费进度**

**4.支持查询每个消费组的消费数量统计**

**5.支持查询每日消息生产数据统计、DLQ统计、消息发送top 10统计**

**6.支持查询每个topic的历史消息**

**7.支持查询每个消息的消费状态**

**8.支持查询DLQ消息、单条重发、批量重发等**

**9.支持带properties的消息发送**

**分支 feature/use-lusong-apache-rocketmq需要搭配这个 [apacherocketmq]（https://github.com/lusong1986/apacherocketmq ） 使用。
部署新的broker，console使用新的客户端，新功能才能体现出来。**

**如果使用master分支，部分功能不能使用。**