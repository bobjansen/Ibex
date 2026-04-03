#pragma once

#include <ibex/runtime/extern_registry.hpp>

#include <atomic>
#include <memory>
#include <optional>
#include <thread>
#include <vector>

namespace ibex::runtime {

/// SPSC (single-producer, single-consumer) queue-backed stream source helper.
///
/// Provides a ready-made, thread-safe producer queue for in-process and
/// user-space transports so the plugin author does not need to implement their
/// own.  A producer thread pushes Tables via write(); Ibex's event loop drains
/// them via the ExternFn returned by make_source_fn(), returning StreamTimeout{}
/// when the queue is momentarily empty so wall-clock bucket flushes still fire
/// on schedule.
///
/// Note: Ibex maintains its own TimeBucket accumulation buffer internally.
/// StreamBuffered replaces the plugin's ad-hoc producer queue — not Ibex's
/// internal buffer.
///
/// ## Usage (manual, capacity chosen in C++)
///
///   auto buf = std::make_shared<StreamBuffered>(/*capacity=*/256);
///   registry.register_table("my_src", buf->make_source_fn());
///
///   std::thread producer([buf] {
///       for (auto& batch : my_data_source) {
///           buf->write(batch);
///       }
///       buf->close();
///   });
///
/// ## Usage (make_buffered_source, capacity set from Ibex)
///
/// Prefer make_buffered_source() when the Ibex user should control capacity:
///
///   registry.register_table("my_src",
///       runtime::make_buffered_source([](StreamBuffered& buf) {
///           for (auto& batch : my_data_source) buf.write(batch);
///           buf.close();
///       }));
///
///   // In the Ibex query:
///   extern fn my_src(capacity: Int) -> TimeFrame from "plugin.hpp";
///   Stream { source = my_src(512), transform = [...], sink = my_sink() };
///
/// ## Thread safety
///
/// write() and close() must be called from exactly one producer thread.
/// make_source_fn()'s returned ExternFn is called only from the Ibex event
/// loop (single consumer).  No other synchronisation is required between the
/// two sides.
///
/// ## Backpressure
///
/// write() spins (yielding to the scheduler) if the ring buffer is full.
/// Size the capacity generously relative to the burst rate of your source.
///
/// ## Comparison with kernel-backed sockets
///
/// For UDP/TCP sockets the OS kernel already maintains a receive buffer
/// (SO_RCVBUF) independently of the application — no application-level queue
/// is needed.  Use StreamTimeout{} directly from recvfrom() with a short socket
/// timeout instead of wrapping the source in StreamBuffered.
class StreamBuffered : public std::enable_shared_from_this<StreamBuffered> {
   public:
    /// @param capacity  Maximum number of Tables held in the ring buffer at
    ///                  once.  Must be >= 1.
    explicit StreamBuffered(std::size_t capacity = 256) : ring_(capacity), capacity_(capacity) {}

    /// Push a Table to the queue (producer thread only).
    ///
    /// Blocks — yielding the CPU on each spin — if the ring buffer is full.
    /// Unblocks as soon as the consumer drains at least one slot.
    void write(Table t) {
        while (!try_push(t)) {
            std::this_thread::yield();
        }
    }

    /// Signal end-of-stream.  The producer must not call write() after this.
    ///
    /// The consumer (Ibex event loop) will drain any remaining buffered Tables
    /// and then receive a zero-row Table (EOF signal) on its next call.
    void close() noexcept { closed_.store(true, std::memory_order_release); }

    /// Returns an ExternFn that drains this queue, suitable for
    /// ExternRegistry::register_table().
    ///
    /// The returned function:
    ///   - Returns a Table (rows > 0) when data is available in the ring.
    ///   - Returns StreamTimeout{} when the ring is empty but close() has not
    ///     been called — keeping the stream alive for the next write().
    ///   - Returns an empty Table (rows == 0) when close() has been called and
    ///     the ring is fully drained — signalling EOF to the event loop.
    ///
    /// The ExternFn captures a shared_ptr to *this, so the StreamBuffered will
    /// remain alive for at least as long as the ExternFn exists.
    [[nodiscard]] ExternFn make_source_fn() {
        auto self = shared_from_this();
        return [self](const ExternArgs&) -> std::expected<ExternValue, std::string> {
            Table out;
            if (self->try_pop(out)) {
                return out;
            }
            if (self->closed_.load(std::memory_order_acquire)) {
                return Table{};  // EOF: drained and closed
            }
            return StreamTimeout{};
        };
    }

   private:
    std::vector<Table> ring_;
    std::size_t capacity_;

    // Separate cache lines to avoid false sharing between producer and consumer.
    alignas(64) std::atomic<std::size_t> head_{0};  // consumer advances
    alignas(64) std::atomic<std::size_t> tail_{0};  // producer advances
    std::atomic<bool> closed_{false};

    /// Non-blocking pop.  Returns true and moves the front Table into @p out
    /// on success; returns false without touching @p out if the queue is empty.
    bool try_pop(Table& out) {
        const std::size_t h = head_.load(std::memory_order_relaxed);
        const std::size_t t = tail_.load(std::memory_order_acquire);
        if (h == t)
            return false;
        out = std::move(ring_[h % capacity_]);
        head_.store(h + 1, std::memory_order_release);
        return true;
    }

    /// Non-blocking push.  Moves @p t into the ring on success (returns true).
    /// Returns false without touching @p t if the ring is full.
    bool try_push(Table& t) {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        const std::size_t h = head_.load(std::memory_order_acquire);
        if (tail - h >= capacity_)
            return false;
        ring_[tail % capacity_] = std::move(t);
        tail_.store(tail + 1, std::memory_order_release);
        return true;
    }
};

/// Creates an ExternFn that wraps a StreamBuffered source with lazy
/// initialisation, allowing the ring capacity to be set from the Ibex query.
///
/// The @p producer callable is invoked once in a detached thread the first
/// time the source is called by the event loop.  It receives a reference to
/// the StreamBuffered and must call buf.close() when it has no more data.
///
/// The ring capacity is taken from the first Int argument passed to the extern
/// call in the Ibex query.  If no argument is supplied, 256 is used.
///
/// ## Example
///
///   // C++ plugin — no capacity decision here
///   registry.register_table("my_src",
///       runtime::make_buffered_source([](runtime::StreamBuffered& buf) {
///           for (auto& row : data_source) buf.write(row);
///           buf.close();
///       }));
///
///   // Ibex query — user tunes capacity alongside other stream parameters
///   extern fn my_src(capacity: Int) -> TimeFrame from "plugin.hpp";
///   Stream {
///       source    = my_src(512),
///       transform = [resample 1s, select { close = last(price) }],
///       sink      = my_sink()
///   };
template <typename ProducerFn>
[[nodiscard]] ExternFn make_buffered_source(ProducerFn producer) {
    struct State {
        std::optional<ExternFn> source_fn{};
        ProducerFn producer;
    };
    auto state = std::make_shared<State>(State{{}, std::move(producer)});

    return [state](const ExternArgs& args) mutable -> std::expected<ExternValue, std::string> {
        if (!state->source_fn) {
            std::size_t cap = 256;
            if (!args.empty()) {
                if (const auto* v = std::get_if<std::int64_t>(&args[0])) {
                    if (*v <= 0) {
                        return std::unexpected("StreamBuffered capacity must be > 0");
                    }
                    cap = static_cast<std::size_t>(*v);
                }
            }
            auto buf = std::make_shared<StreamBuffered>(cap);
            state->source_fn = buf->make_source_fn();
            std::thread([buf, p = std::move(state->producer)]() mutable { p(*buf); }).detach();
        }
        return (*state->source_fn)(args);
    };
}

}  // namespace ibex::runtime
