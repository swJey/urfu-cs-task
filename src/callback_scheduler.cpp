#include "callback_scheduler.h"
#include <iostream>
#include <stdexcept>

CallbackScheduler::CallbackScheduler()
    : worker_(&CallbackScheduler::WorkerLoop, this) {}

CallbackScheduler::~CallbackScheduler() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stop_ = true;
    }
    cv_.notify_one();
    if (worker_.joinable()) {
        worker_.join();
    }
}

CallbackScheduler::TaskId CallbackScheduler::Schedule(
    std::function<void()> callback, TimePoint when)
{
    std::shared_ptr<Task> task;
    TaskId id;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        id = nextId_++;
        task = std::make_shared<Task>(std::move(callback), when, id);
        tasks_[id] = task;
        bool wasEmpty = queue_.empty();
        TimePoint topTime = wasEmpty ? TimePoint::max() : queue_.top()->when;
        queue_.push(task);

        // Будим воркер, если очередь была пуста или новая задача раньше текущей ближайшей
        if (wasEmpty || when < topTime) {
            cv_.notify_one();
        }
    }
    return id;
}

bool CallbackScheduler::Cancel(TaskId id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = tasks_.find(id);
    if (it != tasks_.end() && !it->second->cancelled) {
        it->second->cancelled = true;
        tasks_.erase(it);
        return true;
    }
    return false;
}

void CallbackScheduler::WorkerLoop() {
    while (true) {
        std::unique_lock<std::mutex> lock(mutex_);

        // Ждём появления хотя бы одной задачи или сигнала остановки
        while (queue_.empty() && !stop_) {
            cv_.wait(lock);
        }
        if (stop_) break;

        auto now = std::chrono::system_clock::now();
        auto top = queue_.top();
        if (top->when > now) {
            // Ожидаем до времени ближайшей задачи или до пробуждения
            cv_.wait_until(lock, top->when, [this] {
                return stop_ || (!queue_.empty() &&
                       queue_.top()->when <= std::chrono::system_clock::now());
            });
            continue;
        }

        // Время задачи наступило
        auto task = queue_.top();
        queue_.pop();

        if (task->cancelled) {
            continue; // отменённые задачи пропускаем
        }

        // Выполняем колбэк без удержания мьютекса
        lock.unlock();
        try {
            task->callback();
        } catch (const std::exception& e) {
            std::cerr << "Exception in callback (id=" << task->id
                      << "): " << e.what() << std::endl;
        } catch (...) {
            std::cerr << "Unknown exception in callback (id="
                      << task->id << ")" << std::endl;
        }
        lock.lock();
    }
}
