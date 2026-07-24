#include <ibex/runtime/worker_pool.hpp>

#include <algorithm>
#include <charconv>
#include <condition_variable>
#include <cstddef>
#include <cstdlib>
#include <deque>
#include <exception>
#include <mutex>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace ibex::runtime {

struct WorkerPool::Batch::State {
    std::function<void(std::size_t)> body;

    std::mutex mutex;
    std::condition_variable done;
    std::size_t remaining = 0;
    std::exception_ptr error;
    std::size_t error_worker = 0;
};

namespace {

struct Task {
    std::shared_ptr<WorkerPool::Batch::State> state;
    std::size_t worker_id = 0;
};

/// Runs one worker body and settles its slot in the batch. Exceptions are
/// captured rather than propagated: a throw out of a pool thread would
/// terminate the process, and the batch's owner rethrows deterministically
/// (lowest worker id wins) from `wait()`.
void run_task(Task& task) {
    auto& state = *task.state;
    std::exception_ptr caught;
    try {
        state.body(task.worker_id);
    } catch (...) {
        caught = std::current_exception();
    }
    {
        const std::lock_guard lock(state.mutex);
        if (caught != nullptr && (state.error == nullptr || task.worker_id < state.error_worker)) {
            state.error = caught;
            state.error_worker = task.worker_id;
        }
        --state.remaining;
    }
    state.done.notify_all();
}

[[nodiscard]] auto env_value(const char* name) -> std::string_view {
    const char* raw = std::getenv(name);  // NOLINT(concurrency-mt-unsafe)
    return raw == nullptr ? std::string_view{} : std::string_view{raw};
}

}  // namespace

struct WorkerPool::Impl {
    std::mutex mutex;
    std::condition_variable work;
    std::deque<Task> queue;
    bool stopping = false;
    std::vector<std::jthread> threads;
};

WorkerPool::WorkerPool(std::size_t threads)
    : impl_(std::make_unique<Impl>()), threads_(threads == 0 ? 1 : threads) {
    impl_->threads.reserve(threads_);
    for (std::size_t i = 0; i < threads_; ++i) {
        impl_->threads.emplace_back([this] {
            while (true) {
                Task task;
                {
                    std::unique_lock lock(impl_->mutex);
                    impl_->work.wait(lock,
                                     [this] { return impl_->stopping || !impl_->queue.empty(); });
                    if (impl_->stopping && impl_->queue.empty()) {
                        return;
                    }
                    task = std::move(impl_->queue.front());
                    impl_->queue.pop_front();
                }
                run_task(task);
            }
        });
    }
}

WorkerPool::~WorkerPool() {
    {
        const std::lock_guard lock(impl_->mutex);
        impl_->stopping = true;
    }
    impl_->work.notify_all();
    // ~jthread joins; queued work drains first so no batch is left unsettled.
}

auto WorkerPool::submit(std::size_t worker_count, std::function<void(std::size_t)> body) -> Batch {
    const std::size_t count = std::clamp<std::size_t>(worker_count, 1, threads_);
    auto state = std::make_shared<Batch::State>();
    state->body = std::move(body);
    state->remaining = count;
    {
        const std::lock_guard lock(impl_->mutex);
        for (std::size_t i = 0; i < count; ++i) {
            impl_->queue.push_back(Task{.state = state, .worker_id = i});
        }
    }
    impl_->work.notify_all();
    return Batch{std::move(state)};
}

WorkerPool::Batch::Batch(Batch&&) noexcept = default;

auto WorkerPool::Batch::operator=(Batch&& other) noexcept -> Batch& {
    if (this != &other) {
        // Never abandon a running batch: its bodies still reference whatever
        // the previous owner captured.
        if (state_ != nullptr) {
            std::unique_lock lock(state_->mutex);
            state_->done.wait(lock, [this] { return state_->remaining == 0; });
        }
        state_ = std::move(other.state_);
    }
    return *this;
}

WorkerPool::Batch::~Batch() {
    if (state_ == nullptr) {
        return;
    }
    std::unique_lock lock(state_->mutex);
    state_->done.wait(lock, [this] { return state_->remaining == 0; });
}

void WorkerPool::Batch::wait() {
    if (state_ == nullptr) {
        return;
    }
    std::exception_ptr error;
    {
        std::unique_lock lock(state_->mutex);
        state_->done.wait(lock, [this] { return state_->remaining == 0; });
        error = std::exchange(state_->error, nullptr);
    }
    if (error != nullptr) {
        std::rethrow_exception(error);
    }
}

auto default_thread_count() -> std::size_t {
    const auto raw = env_value("IBEX_THREADS");
    if (!raw.empty() && raw != "auto") {
        std::size_t parsed = 0;
        const auto* end = raw.data() + raw.size();
        const auto result = std::from_chars(raw.data(), end, parsed);
        if (result.ec == std::errc{} && result.ptr == end && parsed > 0) {
            return parsed;
        }
        return 1;
    }
    const unsigned hardware = std::thread::hardware_concurrency();
    return hardware == 0 ? 1 : static_cast<std::size_t>(hardware);
}

auto process_worker_pool() -> WorkerPool& {
    // Function-local static: constructed on first use (thread-safe since C++11)
    // and joined during normal process teardown.
    static WorkerPool pool(default_thread_count());
    return pool;
}

auto parallel_enabled_from_env() -> bool {
    const auto raw = env_value("IBEX_PARALLEL");
    return raw == "1" || raw == "on" || raw == "true" || raw == "yes";
}

auto morsel_rows_from_env() -> std::size_t {
    const auto raw = env_value("IBEX_MORSEL_ROWS");
    if (raw.empty()) {
        return 0;
    }
    std::size_t parsed = 0;
    const auto* end = raw.data() + raw.size();
    const auto result = std::from_chars(raw.data(), end, parsed);
    if (result.ec == std::errc{} && result.ptr == end) {
        return parsed;
    }
    return 0;
}

}  // namespace ibex::runtime
