#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""实验三：PageRank 算法的 Spark 实现。

输入：每行 "网页\t链接网页1,链接网页2,..."
输出：(网页,PR值)，PR 保留 10 位小数，按 PR 降序排列。
"""
import sys

from pyspark import SparkConf, SparkContext

DAMPING = 0.85
ITERATIONS = 10


def parse(line):
    parts = line.split("\t")
    page = parts[0].strip()
    # 出链用 tuple 保存，便于 RDD 分布式去重（list 不可哈希）
    links = ()
    if len(parts) > 1 and parts[1].strip():
        links = tuple(x.strip() for x in parts[1].split(",") if x.strip())
    return page, links


def main():
    if len(sys.argv) < 3:
        print("用法: spark-submit pagerank.py <输入路径> <输出目录>")
        sys.exit(1)
    input_path, output_path = sys.argv[1], sys.argv[2]

    conf = SparkConf().setAppName("PageRank")
    sc = SparkContext(conf=conf)

    graph = (sc.textFile(input_path)
             .filter(lambda l: l.strip())
             .map(parse)
             .distinct()
             .cache())

    node_count = graph.count()
    print(">>> 网页节点数 N = %d" % node_count)

    # 初始 PR 均为 1.0
    ranks = graph.mapValues(lambda _: 1.0).cache()

    for i in range(1, ITERATIONS + 1):
        # 每个网页把自身 PR 平均分给出链
        contributions = graph.join(ranks).flatMap(
            lambda kv: [(t, kv[1][1] / len(kv[1][0])) for t in kv[1][0]]
        )
        # 没有入链的网页贡献为 0，保证所有网页都参与计算
        base = graph.keys().map(lambda p: (p, 0.0))
        sums = base.union(contributions).reduceByKey(lambda a, b: a + b)
        ranks = sums.mapValues(
            lambda s: (1.0 - DAMPING) + DAMPING * s
        ).cache()

    # 按 PR 降序，格式化为 (网页,PR值)
    result = ranks.sortBy(lambda kv: -kv[1]).map(
        lambda kv: "(%s,%.10f)" % (kv[0], kv[1])
    )
    result.saveAsTextFile(output_path)

    sc.stop()


if __name__ == "__main__":
    main()
