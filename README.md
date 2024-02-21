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

lab1的代码提交到了gradescop，修复如下bug：

bug1

```
==2340==ERROR: AddressSanitizer: heap-use-after-free on address 0x60200000075c at pc 0x564b46a367f6 bp 0x7ffd677f42b0 sp 0x7ffd677f42a8
9: READ of size 4 at 0x60200000075c thread T0
9:     #0 0x564b46a367f5 in bustub::BufferPoolManager::DeletePage(int) /autograder/source/bustub/src/buffer/buffer_pool_manager.cpp:260:20
...
```

bug2:

```
9: Test command: /autograder/source/bustub/build/test/grading_buffer_pool_manager_test "--gtest_filter=BufferPoolManagerTest.DeletePage" "--gtest_also_run_disabled_tests" "--gtest_color=auto" "--gtest_output=xml:/autograder/source/bustub/build/test/grading_buffer_pool_manager_test.xml" "--gtest_catch_exceptions=0"
9: Test timeout computed to be: 120
9: Running main() from gmock_main.cc
9: Note: Google Test filter = BufferPoolManagerTest.DeletePage
9: [==========] Running 1 test from 1 test suite.
9: [----------] Global test environment set-up.
9: [----------] 1 test from BufferPoolManagerTest
9: [ RUN      ] BufferPoolManagerTest.DeletePage
9: /autograder/source/bustub/test/buffer/grading_buffer_pool_manager_test.cpp:406: Failure
9: Expected equality of these values:
9:   1
9:   bpm->DeletePage(page_ids[4])
9:     Which is: false
9: [  FAILED  ] BufferPoolManagerTest.DeletePage (1 ms)
9: [----------] 1 test from BufferPoolManagerTest (1 ms total)
9: 
9: [----------] Global test environment tear-down
9: [==========] 1 test from 1 test suite ran. (1 ms total)
9: [  PASSED  ] 0 tests.
9: [  FAILED  ] 1 test, listed below:
9: [  FAILED  ] BufferPoolManagerTest.DeletePage
9: 
9:  1 FAILED TEST
9: 
9: =================================================================
9: ==2339==ERROR: LeakSanitizer: detected memory leaks
```