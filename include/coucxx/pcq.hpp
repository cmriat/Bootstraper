#include <glog/export.h>
#include <folly/ProducerConsumerQueue.h>
#include <folly/synchronization/LifoSem.h>

template <typename T>
class BlockingQueue : public folly::ProducerConsumerQueue<T> {
private:
  folly::LifoSem semEmpty;
  folly::LifoSem semFull;

public:
  explicit BlockingQueue(uint32_t bufferSize)
      : folly::ProducerConsumerQueue<T>(bufferSize) {
    semFull.post(bufferSize);
  }

  template <class... Args> bool write(Args &&...recordArgs) {
    semFull.wait();
    auto res = this->folly::ProducerConsumerQueue<T>::write(
        std::forward<Args>(recordArgs)...);
    semEmpty.post();
    return res;
  };

  template <class... Args> bool read(Args &&...recordArgs) {
    semEmpty.wait();
    auto res = this->folly::ProducerConsumerQueue<T>::read(
        std::forward<Args>(recordArgs)...);
    semFull.post();
    return res;
  };
};