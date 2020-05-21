# arctern_benchmark
arctern benchmark

运行arctern benchmark主要需要以下几个部分：测试case的json文件，测试case文件，配置运行环境

1. 编写要测试的函数对应的py文件，可以参考[st_area.py](test_case/gis_all/st_area.py)

2. 执行命令：
```shell script
    python3 main.py -f test_suites/gis_only/gis_test.txt --times 3 --spark --python -v 0.1.0 --commit_id ae520101d84e76baf3978754e371b0a83a8d36b1 -w True
```

命令详解：
```shell script
    -f #待测试的测试集合文件，每一行是一个测试用例，原文件以及对应的输出文件
    --times #待测试文件的测试次数
    --spark #如果加这个参数，意味着要测试arctern_pyspark的接口
    --Python #是否要测试Python接口
    -v #要测试的arctern版本
    --commit_id #要测试的arctern版本的具体的commit_id
    -w #是否要创建新的conda环境
```