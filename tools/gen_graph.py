#!/usr/bin/env python3
"""生成 PageRank 实验用的有向网页图数据。

每行格式：网页<TAB>该网页链接到的网页集合（英文逗号分隔）
"""
import random

N = 120
random.seed(20261120)

pages = ["Page_%03d" % i for i in range(1, N + 1)]
lines = []
for p in pages:
    k = random.randint(3, 8)
    targets = random.sample([q for q in pages if q != p], k)
    lines.append("%s\t%s" % (p, ",".join(targets)))

with open("web-graph.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(lines) + "\n")
print("pages=%d edges=%d" % (N, sum(len(l.split("\t")[1].split(",")) for l in lines)))
