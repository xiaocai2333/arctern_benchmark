# arctern_benchmark
arctern benchmark

运行arctern benchmark主要需要以下几个部分：测试case的json文件，测试case文件，配置运行环境

1.如何配置运行环境
    配置文件在conf目录下，spark.yaml就是配置文件，修改spark.yaml文件，如果运行环境是本地spark，则Hadoop对应的value修改为None。
    修改spark的配置，运行模式master， 运行内存executor_memory
运行gisbenchmark:
spark-submit main.py --conf conf.yaml -testcase ./scheduler/gis_only/gis.json