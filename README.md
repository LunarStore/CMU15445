# 应官方要求，本仓库设置成了私有仓库。

# 简单记录一下实验日志

## 2024.2.16

基于centos虚拟机，搭建开发环境。

主要注意的是实验中，自动化安装package的shell脚本只支持Ubuntu。所以centos需要对照这shell脚本，手动安装一些包。

比较关键的一个包是：

```bash
yum install devtoolset-9-libasan-devel
```

## 2024.2.17

完成了lab1的task1

由于之前阅读过LevelDB（Google的基于磁盘的KV存储引擎）的源码，这部分完成的比较快，实现了LRU-K的替换策略。

## 2024.2.20

完成了lab1的task2、task3。右leveldb的基础，难度还好。

在cmu15445中，缓存是直接分配那么多大小，然后在有限的缓存中，进行页面切换，复用已有缓存。

在leveldb中，一开始缓存大小是为0的，然后动态分配内存。当超过限制的buff大小，就会动态释放掉淘汰页面的缓存。

问题：

1. BufferManager的实现中，对page炒作时，是否需要对page加读写锁？

2. 考虑优化一下锁的粒度。目前的实现是直接加大锁。