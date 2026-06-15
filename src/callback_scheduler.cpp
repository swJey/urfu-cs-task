#include "callback_scheduler.h"
#include <iostream>   // для логирования ошибок (можно заменить на свой логгер)
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
        // Сохраняем время вершины, если очередь непуста
        TimePoint topTime = wasEmpty ? TimePoint::max() : queue_.top()->when;
        queue_.push(task);

        // Если новая задача может стать ближайшей, будим воркер
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
        // Задачу из очереди не удаляем — она будет проигнорирована при извлечении
        return true;
    }
    return false;
}

void CallbackScheduler::WorkerLoop() {
    while (true) {
        std::unique_lock<std::mutex> lock(mutex_);
        // Ждём, пока не придёт время запуска ближайшей задачи или не остановят
        cv_.wait(lock, [this] {
            return stop_ || (!queue_.empty() && queue_.top()->when <= std::chrono::system_clock::now());
        });

        if (stop_) {
            break;
        }

        auto now = std::chrono::system_clock::now();
        // Обрабатываем все "созревшие" задачи
        while (!queue_.empty() && queue_.top()->when <= now) {
            auto task = queue_.top();
            queue_.pop();

            if (task->cancelled) {
                continue;   // отменённые пропускаем
            }

            // Перед вызовом колбэка отпускаем мьютекс
            lock.unlock();
            try {
                task->callback();
            } catch (const std::exception& e) {
                std::cerr << "CallbackScheduler: exception in callback (id="
                          << task->id << "): " << e.what() << std::endl;
            } catch (...) {
                std::cerr << "CallbackScheduler: unknown exception in callback (id="
                          << task->id << ")" << std::endl;
            }
            lock.lock();

            now = std::chrono::system_clock::now(); // обновляем текущее время
        }
        // Цикл ожидания продолжается (cv_.wait) с новыми условиями
    }
}
