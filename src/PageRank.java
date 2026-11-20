import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

/**
 * 实验三：PageRank 算法的 Hadoop MapReduce 实现（伪分布式）。
 *
 * 输入：每行 "网页\t链接网页1,链接网页2,..."
 * 输出：(网页,PR值)，PR 保留 10 位小数，按 PR 降序排列。
 */
public class PageRank {

    private static final double DAMPING = 0.85;   // 阻尼系数 d
    private static final int ITERATIONS = 10;     // 迭代次数

    /** 预处理：补齐所有出现过的网页，写入初始 PR=1.0 与出链集合。 */
    public static class InitMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }
            String[] parts = line.split("\t", -1);
            String page = parts[0].trim();
            String links = parts.length > 1 ? parts[1].trim() : "";
            outKey.set(page);
            outValue.set("G" + links);
            context.write(outKey, outValue);
            if (!links.isEmpty()) {
                for (String target : links.split(",")) {
                    String t = target.trim();
                    if (!t.isEmpty()) {
                        outKey.set(t);
                        outValue.set("E");
                        context.write(outKey, outValue);
                    }
                }
            }
        }
    }

    public static class InitReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String links = null;
            for (Text v : values) {
                String s = v.toString();
                if (s.startsWith("G")) {
                    links = s.substring(1);
                    break;
                }
            }
            if (links == null) {
                links = "";
            }
            outValue.set(String.format(Locale.US, "1.0\t%s", links));
            context.write(key, outValue);
        }
    }

    /** 每轮迭代：把当前 PR 按出度均分给链接网页。 */
    public static class RankMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t", -1);
            String page = parts[0];
            double pr = parts.length > 1 ? Double.parseDouble(parts[1]) : 0.0;
            String links = parts.length > 2 ? parts[2] : "";

            outKey.set(page);
            outValue.set("S\t" + links);
            context.write(outKey, outValue);

            if (!links.isEmpty()) {
                String[] targets = links.split(",");
                double share = pr / targets.length;
                for (String t : targets) {
                    outKey.set(t);
                    outValue.set("C\t" + share);
                    context.write(outKey, outValue);
                }
            }
        }
    }

    public static class RankReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String links = "";
            double sum = 0.0;
            for (Text v : values) {
                String s = v.toString();
                if (s.startsWith("S\t")) {
                    links = s.substring(2);
                } else if (s.startsWith("C\t")) {
                    sum += Double.parseDouble(s.substring(2));
                }
            }
            // 随机跳转项按每页 (1-d) 计，保证全图 PR 总和为节点数 N、平均 PR 为 1
            double newPr = (1.0 - DAMPING) + DAMPING * sum;
            outValue.set(String.format(Locale.US, "%.12f\t%s", newPr, links));
            context.write(key, outValue);
        }
    }

    /** 结果格式化：按 PR 降序输出 "(网页,PR值)"。 */
    public static class FormatMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t", -1);
            outKey.set(parts[0]);
            outValue.set(parts[1]);
            context.write(outKey, outValue);
        }
    }

    public static class FormatReducer extends Reducer<Text, Text, NullWritable, Text> {
        private final List<String[]> rows = new ArrayList<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) {
            double pr = 0.0;
            for (Text v : values) {
                pr = Double.parseDouble(v.toString());
            }
            rows.add(new String[]{key.toString(), String.valueOf(pr)});
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Collections.sort(rows, new Comparator<String[]>() {
                @Override
                public int compare(String[] a, String[] b) {
                    return Double.compare(Double.parseDouble(b[1]), Double.parseDouble(a[1]));
                }
            });
            Text out = new Text();
            for (String[] row : rows) {
                out.set("(" + row[0] + "," + String.format(Locale.US, "%.10f",
                        Double.parseDouble(row[1])) + ")");
                context.write(NullWritable.get(), out);
            }
        }
    }

    private static int countLines(Configuration conf, Path dir) throws IOException {
        FileSystem fs = dir.getFileSystem(conf);
        int total = 0;
        for (FileStatus st : fs.globStatus(new Path(dir, "part-*"))) {
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(fs.open(st.getPath())))) {
                while (br.readLine() != null) {
                    total++;
                }
            }
        }
        return total;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("用法: PageRank <输入路径> <输出目录> [迭代次数]");
            System.exit(1);
        }
        Path input = new Path(args[0]);
        Path base = new Path(args[1]);
        int iterations = args.length > 2 ? Integer.parseInt(args[2]) : ITERATIONS;

        Configuration conf = new Configuration();
        FileSystem fs = base.getFileSystem(conf);
        if (fs.exists(base)) {
            fs.delete(base, true);
        }

        // 1. 预处理
        Job init = Job.getInstance(conf, "pagerank-init");
        init.setJarByClass(PageRank.class);
        init.setMapperClass(InitMapper.class);
        init.setReducerClass(InitReducer.class);
        init.setOutputKeyClass(Text.class);
        init.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(init, input);
        Path iter0 = new Path(base, "iter_0");
        FileOutputFormat.setOutputPath(init, iter0);
        if (!init.waitForCompletion(true)) {
            System.exit(1);
        }

        int nodeCount = countLines(conf, iter0);
        System.out.println(">>> 网页节点数 N = " + nodeCount);
        conf.setInt("pagerank.node.count", nodeCount);

        // 2. 迭代 10 轮
        Path prev = iter0;
        for (int i = 1; i <= iterations; i++) {
            Job job = Job.getInstance(conf, "pagerank-iter-" + i);
            job.setJarByClass(PageRank.class);
            job.setMapperClass(RankMapper.class);
            job.setReducerClass(RankReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, prev);
            Path out = new Path(base, "iter_" + i);
            FileOutputFormat.setOutputPath(job, out);
            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }
            prev = out;
            System.out.println(">>> 第 " + i + " 轮迭代完成: " + out);
        }

        // 3. 格式化输出
        Job format = Job.getInstance(conf, "pagerank-format");
        format.setJarByClass(PageRank.class);
        format.setMapperClass(FormatMapper.class);
        format.setReducerClass(FormatReducer.class);
        format.setNumReduceTasks(1);
        format.setOutputKeyClass(Text.class);
        format.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(format, prev);
        FileOutputFormat.setOutputPath(format, new Path(base, "result"));
        System.exit(format.waitForCompletion(true) ? 0 : 1);
    }
}
