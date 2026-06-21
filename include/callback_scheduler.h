#pragma once

#include <chrono>
#include <functional>
#include <cstdint>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <queue>
#include <vector>
#include <unordered_map>
#include <atomic>

using TimePoint = std::chrono::time_point<std::chrono::system_clock>;

class CallbackScheduler {
public:
    using TaskId = std::uint64_t;

    CallbackScheduler() : stop_(false) {
        worker_ = std::thread(&CallbackScheduler::WorkerLoop, this);
    }

    ~CallbackScheduler() {
        stop_.store(true);
        cv_.notify_all();
        if (worker_.joinable())
            worker_.join();
    }

    TaskId Schedule(std::function<void()> callback, TimePoint when) {
        TaskId id = nextId_.fetch_add(1);
        auto task = std::make_shared<Task>(std::move(callback), when, id);
        {
            std::lock_guard<std::mutex> lock(mutex_);
            tasks_.emplace(id, task);
            queue_.push(task);
        }
        cv_.notify_one();
        return id;
    }

    bool Cancel(TaskId id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = tasks_.find(id);
        if (it != tasks_.end()) {
            tasks_.erase(it);
            return true;
        }
        return false;
    }

    CallbackScheduler(const CallbackScheduler&) = delete;
    CallbackScheduler& operator=(const CallbackScheduler&) = delete;

private:
    struct Task {
        std::function<void()> callback;
        TimePoint when;
        TaskId id;

        Task(std::function<void()> cb, TimePoint t, TaskId i)
            : callback(std::move(cb)), when(t), id(i) {}
    };

    struct Compare {
        bool operator()(const std::shared_ptr<Task>& a,
                        const std::shared_ptr<Task>& b) const {
            return a->when > b->when; // минимальное время — на вершине
        }
    };

    using Queue = std::priority_queue<std::shared_ptr<Task>,
                                      std::vector<std::shared_ptr<Task>>,
                                      Compare>;

    std::mutex mutex_;
    std::condition_variable cv_;
    Queue queue_;
    std::unordered_map<TaskId, std::shared_ptr<Task>> tasks_;
    std::atomic<TaskId> nextId_{1};
    std::thread worker_;
    std::atomic<bool> stop_;

    void WorkerLoop() {
        while (!stop_.load()) {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this] {
                return !queue_.empty() || stop_.load();
            });

            if (stop_.load())
                break; // не выполняем оставшиеся задачи при остановке

            auto task = queue_.top();
            queue_.pop();

            auto it = tasks_.find(task->id);
            if (it == tasks_.end()) {
                // задача отменена — пропускаем
                continue;
            }

            // удаляем из tasks_, чтобы предотвратить повторное выполнение
            // и сделать невозможной отмену уже запущенной задачи
            tasks_.erase(it);

            lock.unlock(); // отпускаем мьютекс перед вызовом callback

            // выполняем задачу
            task->callback();

            // после завершения callback задача больше не нужна
        }
    }
};
