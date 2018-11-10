#pragma once

#include <vector>
#include <thread>

class StoreImpl;
class Threadpool {
  private:
    int pool_size;
    std::vector<std::thread> threads;
  public:
    Threadpool(int pool_size);
    void intializeThreadpool(StoreImpl * store);
    void waitForThreads();
};
