# 基本情况

## 成员

* roseduan：开源爱好者，分布式存储发烧友

* 杨振远：
* 尹港：
* 侯盛鑫：

## 项目进展

- [ ] kv 整体管理，对外接口提供

- [ ] 事务

- [ ] memtable

- [ ] WAL 日志

- [ ] 磁盘数据管理

# 项目介绍

lotusdb 是一个 Go 语言实现的单机 kv 存储引擎，具有读写速度快、读写放大极低的特点。

我们希望能够使 lotusdb 成为 TiKV 中除了 rocksdb 之外的新选择。

# 背景&动机

在目前的单机存储引擎产品中，LSM Tree 派系的产品独秀一枝，其中以 rocksdb 最为流行，应用最为广泛。在顺序 IO 的保证下，写性能较高，但由于存在多级 SSTable，查询的性能会受到影响。并且追加写的特征，虽然最大限度的发挥了顺序 IO 的优势，但也随之而带来了严重的读写放大问题。我们可以认为 LSM 是 Write Friendly 的。

与 LSM Tree 对应的常见的存储模型是 B+ 树，它的特点是原地更新，不会存在 compaction，数据全部存储在了 B+ 树叶子节点当中，读性能稳定。我们可以认为 B+ 树是 Read Friendly 的。

基于以上思路，我们想**结合 LSM 和 B+ Tree 的优势**，尽量降低读写放大问题，以及减少 compaction 带来的 CPU 和磁盘消耗。

况且，在实际的生产环境中，自己维护一个可控、性能优异、简洁的存储引擎，比掌握一个庞大的开源项目（rocksdb）更加可靠，出现了问题更加容易定位和解决，例如 cockroachdb 使用自研的 Pebble 替换掉了 rocksdb，这有助于我们的产品进行快速迭代更新。

# 项目设计

维护的基本组件如下：

* WAL：预写日志，预防内存崩溃
* MemTable：内存中的数据结构（一般为跳表），设定一个阈值，写满后关闭
* Immutable MemTable：不可变的 MemTable，等待被后台线程 Flush 到磁盘当中
* bolt：磁盘索引数据管理，我们使用了 BoltDB，采用 B+ 树模型。如果 value 的大小没超过设置的阈值，则 value 也存放到 B+ 树中
* Value Log：value 日志，用于 kv 分离，如果超过了设定的 value 大小阈值，则会将 value 存放到日志中

读写流程：

写：和 rocksdb 基本一致，先写 WAL 预防内存崩溃数据丢失，然后再写 MemTable。

读：从 MemTable -> Immutable MemTable -> B+ Tree Index -> Value Log

项目整体架构图如下：

![](https://cdn.nlark.com/yuque/0/2021/png/12925940/1639290841436-017252ba-07e9-4991-b0d0-0ea348b54538.png)

