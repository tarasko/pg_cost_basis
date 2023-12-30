#pragma once

#include <type_traits>

extern "C"
{
#include <postgres.h>
#include <utils/memutils.h>
}

#ifdef DEBUG_MEMORY

#include <cxxabi.h>
#include <string>

template<typename T>
std::string cpp_name()
{
    char buf[1024];
    size_t sz=sizeof(buf);
    int status;
    char* res = abi::__cxa_demangle(typeid(T).name(), buf, &sz, &status);
    return std::string(res, sz);
}

#endif

// We define custom allocator in order to prevent memory leaks because postgres doesn't call delete or free allocated objects.
// More on postgres' memory management and memory contexts here:
// https://www.cybertec-postgresql.com/en/memory-context-for-postgresql-memory-management/

// Default memory context is very short-lived. In order to keep containers alive between function calls we use CurTransactionContext
// pallocHook/pfreeHook wrap MemoryContextAlloc(CurTransactionContext, ...) and help to debug allocations

template<typename T>
[[nodiscard]] void* pallocHook(std::size_t n = 1)
{
    void* buffer = MemoryContextAlloc(CurTransactionContext, n * sizeof(T));

    // palloc/MemoryContextAlloc uses postgres exception(long jumps) mechanism on failure.
    // We don't have to check for nullptr here.

#ifdef DEBUG_MEMORY
    ereport(WARNING,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("allocated %zu elements of %s, sizeof(T)=%zu, total %zu bytes: %p",
                    n, cpp_name<T>().c_str(), sizeof(T), n * sizeof(T), buffer)));
#endif
    return buffer;
}

template<typename T>
void pfreeHook(T* p, [[maybe_unused]] std::size_t n)
{
#ifdef DEBUG_MEMORY
    ereport(WARNING,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("deallocate %zu elements of %s, sizeof(T)=%zu, : %p", n, cpp_name<T>().c_str(), sizeof(T), p)));
#endif
    pfree(p);
}

// Generic allocator that uses postgres' palloc and pfree
template<typename T>
struct PgAllocator
{
    using value_type = T;
    using is_always_equal = std::true_type;

    PgAllocator() = default;
    template<typename U> PgAllocator(const PgAllocator<U>) noexcept {}

    [[nodiscard]] value_type* allocate(std::size_t n) const noexcept
    {
        // palloc uses postgresql exception mechanism (implemented via longjmp) in case when memory can't be allocated.
        // no need to check return values
        return static_cast<value_type*>(pallocHook<value_type>(n));
    }

    void deallocate(value_type* p, std::size_t n) const noexcept
    {
        pfreeHook<value_type>(p, n);
    }
};

template<typename T>
bool operator==(PgAllocator<T>, PgAllocator<T>)
{
    return true;
}

template<typename T>
bool operator!=(PgAllocator<T>, PgAllocator<T>)
{
    return false;
}
