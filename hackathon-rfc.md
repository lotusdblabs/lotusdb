# 团队成员

- [roseduan](https://github.com/roseduan)：就职于哔哩哔哩分布式存储团队，开源爱好者，开源项目 [rosedb](https://github.com/flower-corp/rosedb)、[lotusdb](https://github.com/flower-corp/lotusdb) 开发者，分布式存储发烧友
- [南风](https://github.com/yzy-github)：开源项目 [lotusdb](https://github.com/flower-corp/lotusdb) 开发者，北京大学研究生在读，喜欢学习和 coding，对数据库开发很感兴趣，期待大家共同进步

- [wsyingang](https://github.com/wsyingang)：开源项目 [lotusdb](https://github.com/flower-corp/lotusdb) 开发者，就职于字节跳动，主要做时序数据库方向，对数据库相关原理很感兴趣
- [侯盛鑫](https://github.com/EAHITechnology)：昵称大云，开源爱好者，[lotusdb](https://github.com/flower-corp/lotusdb) 的开发者之一，对数据库和分布式系统很感兴趣

# 项目介绍

LotusDB 是一个 Go 语言实现的单机 kv 存储引擎，兼备 LSM 和 B+ Tree 的优点，读写速度快、读放大和空间放大极低。

我们希望提供一个强有力的存储引擎，使 lotusdb 成为 TiDB 中 Pebble 的替代品，以及 TiKV 中除了 RocksDB 的新选择。

# 背景&动机

在目前的单机存储引擎产品中，LSM Tree 派系的产品独秀一枝，其中以 RocksDB 最为流行，应用最为广泛。在顺序 IO 的保证下，写性能较高，但由于存在多级 SSTable，查询的性能会受到影响。并且追加写的特征，虽然最大限度的发挥了顺序 IO 的优势，但也随之带来了严重的读写放大问题。我们可以认为 LSM 是 **Write Friendly** 的。

与 LSM Tree 对应的常见的存储模型是 B+ 树，它的特点是原地更新，不会存在 compaction，数据全部存储在了 B+ 树叶子节点当中，读性能稳定。我们可以认为 B+ 树是 **Read Friendly** 的。

基于以上思路，我们想**结合 LSM 和 B+ Tree 的优势**，尽量降低读写放大、空间放大问题，以及减少 compaction 带来的 CPU 和磁盘消耗。

况且，在实际的生产环境中，自己维护一个可控、性能优异、简洁的存储引擎，比掌握一个庞大的开源项目（RocksDB）更加可靠，出现了问题更加容易定位和解决，例如 cockroachdb 使用自研的 Pebble 替换掉了 RocksDB，这有助于 TiDB 这样的数据库产品进行快速迭代更新。

# 项目设计

我们的设计源于两篇 USENIX 论文，分别是： USENIX FAST`19 中的 [SLM-DB](https://www.usenix.org/system/files/fast19-kaiyrakhmet.pdf) 和 USENIX FAST`16 中的 [Wisckey](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf)。

LotusDB 的设计细节如下：

**1、**仍然像 LSM 那样，维护一个 WAL 日志，以及内存 MemTable 和 Immutable MemTable，对于写请求，和 LSM 完全一致，先写 WAL 日志，再更新 MemTable。

**2、**MemTable 写满之后，需要 Flush 到磁盘当中，这里和 LSM 处理不一样：我们将 Flush 到磁盘中的数据放到 B+ Tree 当中。为了降低 B+ Tree 带来的写放大，我们采取了两个方案：

- 写入 B+ Tree 的数据尽量采取批次 Flush，避免多次随机 IO 写入
- B+ Tree 只存储索引和小 value 信息，对于大 value，我们采用 kv 分离的方式，将 value 单独存储在日志中

**3、**对于读请求，流程是这样的：内存 MemTable -> 内存 Immutable MemTable -> 磁盘 B+ Tree -> Value Log。



在这种设计下，我们总结出如下优点：

- 对于用户的写请求（写 WAL + MemTable），仍然是 append only 的方式，因此是顺序 IO，Throughput 得到保证，延续了 LSM 特有的写性能优势
- 读请求，先读内存中的 MemTable 和 Immutable MemTable，再读磁盘中的 B+ Tree 和 Value Log，由于 B+ Tree 有稳定的读性能，因此读放大降低。并且得益于 B+ Tree 有序的数据组织方式，范围扫描数据非常的高效

- 除了 value log，其他组件无需进行 compact，虽说 B+ Tree 仍然存在一定的空间浪费，但由于是原地更新，无效的数据比 LSM 少很多，空间放大降低。

基于以上描述，LotusDB 的架构设计图如下：

[![img](https://camo.githubusercontent.com/305b57cb3c30ebed2ed485c4a5bc77baf5ef71bd22fa616ccc91bc97045a3159/68747470733a2f2f63646e2e6e6c61726b2e636f6d2f79757175652f302f323032312f706e672f31323932353934302f313633393239303834313433362d30313732353262612d303765392d343939312d623064302d3065613334386235343533382e706e67)](https://camo.githubusercontent.com/305b57cb3c30ebed2ed485c4a5bc77baf5ef71bd22fa616ccc91bc97045a3159/68747470733a2f2f63646e2e6e6c61726b2e636f6d2f79757175652f302f323032312f706e672f31323932353934302f313633393239303834313433362d30313732353262612d303765392d343939312d623064302d3065613334386235343533382e706e67)

维护的基本组件如下：

- WAL：预写日志，预防内存崩溃
- MemTable：内存中的数据结构（一般为跳表），设定一个阈值，写满后关闭

- Immutable MemTable：不可变的 MemTable，等待被后台线程 Flush 到磁盘当中
- bolt：磁盘索引数据管理，我们使用了 BoltDB，采用 B+ 树模型。如果 value 的大小没超过设置的阈值，则 value 也存放到 B+ 树中

- Value Log：追加写 value 日志，用于 kv 分离，如果超过了设定的 value 大小阈值，则会将 value 存放到日志中

# 功能模块

在本次 Hackathon 中，我们预计开发的基本功能如下：

-  KV 整体管理，对外接口
-  Column Family 支持

-  Memtable
-  WAL 日志

-  磁盘数据管理（B+ 树）
- Value Log

ps. 后续会加上更加丰富的功能，例如事务、MemTable 的多种数据结构选择、Value Log Compaction 等。

考虑到 TiKV 是基于 rust 进行开发的，如果我们在 Go 语言版本的 LotusDB 中验证方案可行的话，后续会基于 rust 重新构建，让 TiKV 可以无语言转换开销接入 LotusDB。 

# 测试用例

测试结果是 Hackathon 的重要部分，因此我们主要的测试 case 如下。

选择的竞品：

- Badger
- Pebble

- BoltDB
- RocksDB

**测试的主要 case：**

1、放大问题

- 读放大
- 写放大

- 空间放大

2、延迟问题

- 随机读延迟
- 顺序读延迟

- 随机写延迟
- 顺序写延迟

针对不同的 value size 进行测试：1kb、4kb、16kb、64kb、128kb

大 value：1mb、2mb、4mb

3、吞吐问题

4、资源消耗问题

- CPU 使用率
- 内存使用率

- 磁盘空间

5、YCSB 工具测试：

https://github.com/pingcap/go-ycsb

