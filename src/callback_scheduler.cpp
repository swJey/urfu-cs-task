#include "callback_scheduler.h"
#include <iostream>
#include <stdexcept>

CallbackScheduler::CallbackScheduler()
    : stop_(false)
{
    worker_ = std::thread(&CallbackScheduler::WorkerLoop, this);
}

CallbackScheduler::~CallbackScheduler() {
    stop_.store(true);          // атомарная установка, мьютекс не нужен
    cv_.notify_all();
    if (worker_.joinable())
        worker_.join();
}

CallbackScheduler::TaskId CallbackScheduler::Schedule(
    std::function<void()> callback, TimePoint when)
{
    TaskId id = nextId_.fetch_add(1);   // атомарно, вне мьютекса
    auto task = std::make_shared<Task>(std::move(callback), when, id);

    {
        std::lock_guard<std::mutex> lock(mutex_);
        activeIds_.insert(id);
        queue_.push(task);
        // Всегда будим воркер — проверки излишни
        cv_.notify_one();
    }
    return id;
}

bool CallbackScheduler::Cancel(TaskId id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = activeIds_.find(id);
    if (it != activeIds_.end()) {
        activeIds_.erase(it);
        return true;
    }
    return false;
}

void CallbackScheduler::WorkerLoop() {
    while (!stop_.load()) {
        std::unique_lock<std::mutex> lock(mutex_);

        // Ждём, пока появится задача или будет сигнал остановки
        cv_.wait(lock, [this] {
            return !queue_.empty() || stop_.load();
        });

        if (stop_.load())
            break;

        auto now = std::chrono::system_clock::now();
        auto top = queue_.top();
        if (top->when > now) {
            // Ожидаем до времени ближайшей задачи или до появления новой
            cv_.wait_until(lock, top->when, [this] {
                return stop_.load() || (!queue_.empty() &&
                       queue_.top()->when <= std::chrono::system_clock::now());
            });
            continue;   // после ожидания возвращаемся в начало цикла
        }

        // Время наступило – извлекаем задачу
        auto task = queue_.top();
        queue_.pop();

        // Проверяем, активна ли задача (не отменена)
        auto it = activeIds_.find(task->id);
        if (it == activeIds_.end()) {
            continue;   // отменена – пропускаем
        }
        activeIds_.erase(it);   // удаляем из активных, чтобы нельзя было отменить после старта

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
        // lock автоматически перезахватится при следующей итерации (если нужно),
        // поэтому явный lock.unlock() в конце не требуется.
    }
}
