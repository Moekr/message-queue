# 第四届阿里中间件性能挑战赛（复赛）

## 1. 题目背景
Apache RocketMQ作为的一款分布式的消息中间件，历年双十一承载了万亿级的消息流转，为业务方提供高性能低延迟的稳定可靠的消息服务。随着业务的逐步发展和云上的输出，单机队列数量的逐步增加，给RocketMQ带来了新的挑战。复赛的题目要求设计一个单机百万队列以上的存储引擎，单机内存有限，需要充分利用数据结构与存储技术，最大化吞吐量。  

## 2. 题目描述

### 2.1 题目内容
实现一个进程内的队列引擎，单机可支持100万队列以上。

### 2.2 语言限定
JAVA和C++

## 3.  程序目标

仔细阅读demo项目中的QueueStore，DefaultQueueStoreImpl，DemoTester三个类。

你的coding目标是重写DefaultQueueStoreImpl，并实现以下接口:  
abstract void put(String queueName, String message);  
abstract Collection<String> get(String queueName, long offset, long num);  

## 4. 测试环境描述
测试环境为4c8g的ECS，限定使用的最大JVM大小为4GB(-Xmx4g)。带一块300G左右大小的SSD磁盘。

SSD性能大致如下：
iops 1w 左右；块读写能力(一次读写4K以上) 在200MB/s 左右。

ulimit -a:

```
core file size          (blocks, -c) 0
data seg size           (kbytes, -d) unlimited
scheduling priority             (-e) 0
file size               (blocks, -f) unlimited
pending signals                 (-i) 31404
max locked memory       (kbytes, -l) 64
max memory size         (kbytes, -m) unlimited
open files                      (-n) 6553560
pipe size            (512 bytes, -p) 8
POSIX message queues     (bytes, -q) 819200
real-time priority              (-r) 0
stack size              (kbytes, -s) 10240
cpu time               (seconds, -t) unlimited
max user processes              (-u) 31404
virtual memory          (kbytes, -v) unlimited
file locks                      (-x) unlimited
```
磁盘调度算法是 deadline
其它系统参数都是默认的。

## 5. 程序校验逻辑

校验程序分为三个阶段：
1. 发送阶段
2. 索引校验阶段
3. 顺序消费阶段

请详细阅读DemoTester以理解评测程序的逻辑。

### 5.1. 程序校验规模说明
1. 各个阶段线程数在20~30左右
2. 发送阶段：消息大小在50字节左右，消息条数在20亿条左右，也即发送总数据在100G左右
3. 索引校验阶段：会对所有队列的索引进行随机校验；平均每个队列会校验1~2次；
4. 顺序消费阶段：挑选20%的队列进行全部读取和校验；
5. 发送阶段最大耗时不能超过1800s；索引校验阶段和顺序消费阶段加在一起，最大耗时也不能超过1800s；超时会被判断为评测失败。

## 6. 排名规则

在结果校验100%正确的前提下，按照平均tps从高到低来排名


## 7. 第二/三方库规约

* 仅允许依赖JavaSE 8 包含的lib
* 可以参考别人的实现，拷贝少量的代码
* 我们会对排名靠前的代码进行review，如果发现大量拷贝别人的代码，将扣分

## 8.作弊说明

所有消息都应该进行按实际发送的信息进行存储，可以压缩，但不能伪造。
如果发现有作弊行为，比如通过hack评测程序，绕过了必须的评测逻辑，则程序无效，且取消参赛资格。
