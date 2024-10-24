#pragma once

#include <Interpreters/SystemLog.h>
#include <Interpreters/ClientInfo.h>
#include <Common/ProfileEvents.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

struct QueryThreadLogElement
{
    time_t event_time{};
    Decimal64 event_time_microseconds{};
    /// When query was attached to current thread
    time_t query_start_time{};
    /// same as above but adds microsecond precision
    Decimal64 query_start_time_microseconds{};
    /// Real time spent by the thread to execute the query
    UInt64 query_duration_ms{};

    /// The data fetched from DB in current thread to execute the query
    UInt64 read_rows{};
    UInt64 read_bytes{};

    /// The data written to DB
    UInt64 written_rows{};
    UInt64 written_bytes{};

    Int64 memory_usage{};
    Int64 peak_memory_usage{};

    String thread_name;
    UInt64 thread_id{};
    UInt64 master_thread_id{};

    String current_database;
    String query;
    UInt64 normalized_query_hash{};

    ClientInfo client_info;

    std::shared_ptr<ProfileEvents::Counters::Snapshot> profile_counters;

    static std::string name() { return "QueryThreadLog"; }

    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases();
    void appendToBlock(MutableColumns & columns) const;
    static const char * getCustomColumnList() { return nullptr; }
};


class QueryThreadLog : public SystemLog<QueryThreadLogElement>
{
    using SystemLog<QueryThreadLogElement>::SystemLog;
};


}
