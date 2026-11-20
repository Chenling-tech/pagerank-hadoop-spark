# 实验三 PageRank 算法实现（Hadoop MapReduce + Spark）

山东大学《大数据管理与分析》课程实验三代码仓库。

## 一、实验内容

分别使用 **Hadoop MapReduce** 和 **Spark** 实现 PageRank 算法，在伪分布式环境下对同一份网页链接数据集连续迭代 10 次，输出每个网页的 PR 值（保留 10 位小数），并按 PR 值降序排列。

## 二、环境

| 组件 | 版本 |
| --- | --- |
| 操作系统 | Ubuntu 22.04（VirtualBox 虚拟机） |
| JDK | OpenJDK 1.8.0_504 |
| Hadoop | 3.3.6（伪分布式：HDFS + YARN） |
| Spark | 3.5.9（spark-3.5.9-bin-hadoop3） |
| Python | 3.10 |

## 三、数据集

`data/web-graph.txt`，由 `tools/gen_graph.py` 生成（固定随机种子，可复现）。

- 共 120 个网页节点、675 条有向链接；
- 每行格式：`网页<TAB>该网页链接到的网页集合（英文逗号分隔）`；
- 每个网页至少有一条出链（无悬挂节点）。

示例：

```
Page_001	Page_076,Page_068,Page_051,Page_039,Page_094,Page_029
Page_002	Page_019,Page_055,Page_015,Page_120,Page_056,Page_112,Page_018,Page_041
```

## 四、目录结构

```
├── data/web-graph.txt        # 网页链接数据集
├── src/PageRank.java         # MapReduce 实现（预处理 + 10 轮迭代 + 格式化输出）
├── spark/pagerank.py         # Spark 实现（PySpark）
├── tools/gen_graph.py        # 数据集生成脚本
├── results/                  # 两种实现运行后的输出结果
└── README.md
```

## 五、算法说明

PageRank 迭代公式：

```
PR(p) = (1 - d) + d * Σ PR(q) / L(q)
```

其中 `d = 0.85` 为阻尼系数，`L(q)` 为网页 q 的出链数，求和遍历所有链接到 p 的网页 q。初始时每个网页 PR = 1.0，共迭代 10 轮，全图 PR 总和恒为节点数 N、平均 PR 为 1，与题目示例的量级一致。

- **MapReduce 实现**：`PageRank.java` 中 Driver 循环提交 10 个 MapReduce 作业。Mapper 输出两类记录——网页自身的图结构（`S`）和按出度均分后的 PR 贡献（`C`）；Reducer 汇总贡献并按公式更新 PR。最后用一个单 Reducer 作业按 PR 降序格式化输出 `(网页,PR值)`。
- **Spark 实现**：`pagerank.py` 用 RDD 的 `join` + `flatMap` 计算每轮的 PR 贡献，`reduceByKey` 汇总后更新 PR，循环 10 次，最后 `sortBy` 降序输出。

## 六、编译与运行

### 1. 启动 Hadoop（伪分布式）

```bash
start-dfs.sh
start-yarn.sh
jps   # 应看到 NameNode、DataNode、SecondaryNameNode、ResourceManager、NodeManager
```

### 2. MapReduce 实现

```bash
cd ~/lab3
javac -classpath "$(hadoop classpath)" -d classes src/PageRank.java
jar -cf PageRank.jar -C classes .

hdfs dfs -mkdir -p /lab3/input
hdfs dfs -put -f data/web-graph.txt /lab3/input/
hadoop jar PageRank.jar PageRank /lab3/input /lab3/output 10

hdfs dfs -cat /lab3/output/result/part-r-00000
```

### 3. Spark 实现

```bash
spark-submit --master local[*] spark/pagerank.py hdfs://localhost:9000/lab3/input /lab3/spark_output
hdfs dfs -cat /lab3/spark_output/part-00000
```

## 七、运行结果

两种实现均连续迭代 10 轮，输出 120 个网页，PR 总和为 120（平均 PR = 1）。前 10 名结果如下：

| 排名 | MapReduce 输出 | Spark 输出 |
| --- | --- | --- |
| 1 | (Page_013,2.2788701020) | (Page_013,2.2788701020) |
| 2 | (Page_071,2.0773100419) | (Page_071,2.0773100419) |
| 3 | (Page_086,2.0090448857) | (Page_086,2.0090448857) |
| 4 | (Page_076,1.9138521090) | (Page_076,1.9138521090) |
| 5 | (Page_065,1.7610355137) | (Page_065,1.7610355137) |
| 6 | (Page_049,1.7530271499) | (Page_049,1.7530271499) |
| 7 | (Page_016,1.7190569638) | (Page_016,1.7190569638) |
| 8 | (Page_090,1.6618983018) | (Page_090,1.6618983018) |
| 9 | (Page_023,1.6442395286) | (Page_023,1.6442395286) |
| 10 | (Page_113,1.6386863771) | (Page_113,1.6386863771) |

完整结果见 `results/mapreduce_result.txt` 与 `results/spark_result.txt`。

两种实现的结果除 3 个网页在第 10 位小数相差 1（浮点累加顺序不同导致，如 `Page_055` 为 1.1763479894 / 1.1763479893）外完全一致。

## 八、说明

- 数据集规模较小（120 个节点），目的是验证算法逻辑与两种计算框架的实现方式；
- 由于数据集中每个网页都有出链，不存在悬挂节点（dangling node），因此无需额外分配悬挂节点的 PR；
- 两种实现的迭代公式、初始值与迭代次数完全一致，故输出结果一致。
