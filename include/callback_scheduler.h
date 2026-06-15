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

    CallbackScheduler();
    ~CallbackScheduler();

    // Запланировать вызов callback в момент when (или сразу, если when уже прошло)
    TaskId Schedule(std::function<void()> callback, TimePoint when);

    // Отменить задачу по идентификатору. true, если задача была отменена.
    bool Cancel(TaskId id);

    // Запрет копирования и перемещения
    CallbackScheduler(const CallbackScheduler&) = delete;
    CallbackScheduler& operator=(const CallbackScheduler&) = delete;

private:
    struct Task {
        std::function<void()> callback;
        TimePoint when;
        TaskId id;
        std::atomic<bool> cancelled;

        Task(std::function<void()> cb, TimePoint t, TaskId i)
            : callback(std::move(cb)), when(t), id(i), cancelled(false) {}
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
    std::atomic<bool> stop_{false};

    void WorkerLoop();
};
