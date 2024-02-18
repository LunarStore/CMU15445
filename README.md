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
