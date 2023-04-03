#include "src/server/master_server/raft_service_log_manager.h"

#include <memory>

#include "absl/container/flat_hash_set.h"
#include "gtest/gtest.h"

using namespace gfs::service;

// The fixture for testing RaftServiceLogManager. Handles setup and cleanup for the
// test methods.
class RaftServiceLogManagerTest : public ::testing::Test
{
protected:
    static void SetUpTestSuite()
    {
        // The DB can only be initialized and open once by each thread, so we put
        // it in SetUpTestSuite for one-time setup
        RaftServiceLogManager *raft_service_log_mgr = RaftServiceLogManager::GetInstance();
        raft_service_log_mgr->Initialize(
            // Do NOT use /tmp as the test artifacts will persist this way
            // Instead, use relative directory for Bazel test so it will be cleaned
            // up after the test finishes
            /*raft_server_name=*/"raft_service_log_mgr_test_db");
    }

    void SetUp() override { raft_service_log_mgr = RaftServiceLogManager::GetInstance(); }

    RaftServiceLogManager *raft_service_log_mgr;
};

// Test adding current and updating current term from log;
TEST_F(RaftServiceLogManagerTest, CurrentTermGetUpdateTest)
{
    // initial version is 0
    int initial_version = 0;

    // Check if the initialization of current term to 0 is correct
    EXPECT_EQ(raft_service_log_mgr->GetCurrentTerm().value(), initial_version);

    // Check if updating the current term works
    initial_version++;
    EXPECT_TRUE(raft_service_log_mgr->UpdateCurrentTerm(initial_version).ok());
    EXPECT_EQ(raft_service_log_mgr->GetCurrentTerm().value(), initial_version);
}
// Test adding current and updating current votedFor in log;
TEST_F(RaftServiceLogManagerTest, CurrentVotedForGetUpdateTest)
{
    // initial votedFor should be null at database initialization
    EXPECT_FALSE(raft_service_log_mgr->GetVotedFor().ok());

    // Now test for multiple rounds of updates of votedFor();
    for (int votedFor = 0; votedFor < 100; ++votedFor)
    {
        EXPECT_TRUE(raft_service_log_mgr->UpdateVotedFor(votedFor).ok());
        EXPECT_EQ(raft_service_log_mgr->GetVotedFor().value(), votedFor);
    }
}

// Test log adding, appending and deleting functionalities
TEST_F(RaftServiceLogManagerTest, LogGetAppendDeleteTest)
{
    // Initially expect log to be null at initialization (or after crash)
    EXPECT_FALSE(raft_service_log_mgr->GetLogEntries().ok());

    // Set up logs
    LogEntry log1;
    log1.set_command(LogEntry::CLIENT_NEW_FILE);
    log1.set_index(1);
    log1.set_term(0);
    log1.add_commanddata("testdata1");
    log1.add_commanddata("testdata2");
    LogEntry log2;
    log2.set_command(LogEntry::CLIENT_NEW_FILE);
    log2.set_index(1);
    log2.set_term(0);
    log2.add_commanddata("testdata1");
    log2.add_commanddata("testdata2");
    LogEntry log3;
    log3.set_command(LogEntry::CLIENT_NEW_FILE);
    log3.set_index(1);
    log3.set_term(0);
    log3.add_commanddata("testdata1");
    log3.add_commanddata("testdata2");

    std::vector<LogEntry> log_entries;
    log_entries.push_back(log1);
    log_entries.push_back(log2);
    log_entries.push_back(log3);

    // Should expect this to work now
    EXPECT_TRUE(raft_service_log_mgr->AppendLogEntries(log_entries).ok());
    auto result = raft_service_log_mgr->GetLogEntries();
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.value().size(), log_entries.size());
    for (std::size_t i = 0; i < log_entries.size(); ++i)
    {
        EXPECT_EQ(result.value()[i].SerializeAsString(), log_entries[i].SerializeAsString());
    }

    // Add again and see if the append functionality works
    LogEntry log4;
    log4.set_command(LogEntry::CLIENT_NEW_FILE);
    log4.set_index(1);
    log4.set_term(0);
    log4.add_commanddata("testdata1");
    log4.add_commanddata("testdata2");
    LogEntry log5;
    log5.set_command(LogEntry::CHUNK_LEASE_REQUEST);
    log5.set_index(1);
    log5.set_term(0);
    log5.add_commanddata("testdata1");
    log5.add_commanddata("testdata2");

    std::vector<LogEntry> additional_log_entries;
    log_entries.push_back(log4);
    additional_log_entries.push_back(log4);
    log_entries.push_back(log5);
    additional_log_entries.push_back(log5);

    EXPECT_TRUE(raft_service_log_mgr->AppendLogEntries(additional_log_entries).ok());
    auto new_result = raft_service_log_mgr->GetLogEntries();
    EXPECT_TRUE(new_result.ok());
    EXPECT_EQ(new_result.value().size(), log_entries.size());
    for (std::size_t i = 0; i < log_entries.size(); ++i)
    {
        EXPECT_EQ(new_result.value()[i].SerializeAsString(), log_entries[i].SerializeAsString());
    }

    // Now test to see if resizing to 6 works. It should do nothing
    EXPECT_TRUE(raft_service_log_mgr->DeleteLogEntries(6).ok());
    auto resize_failed_result = raft_service_log_mgr->GetLogEntries();
    EXPECT_TRUE(resize_failed_result.ok());
    EXPECT_EQ(resize_failed_result.value().size(), log_entries.size());
    for (std::size_t i = 0; i < log_entries.size(); ++i)
    {
        EXPECT_EQ(resize_failed_result.value()[i].SerializeAsString(), log_entries[i].SerializeAsString());
    }

    // Now resize to 2.
    for (int i = 0; i < 2; ++i)
    {
        log_entries.pop_back();
    }
    EXPECT_TRUE(raft_service_log_mgr->DeleteLogEntries(3).ok());
    auto resize_succeeded_result = raft_service_log_mgr->GetLogEntries();
    EXPECT_TRUE(resize_succeeded_result.ok());
    EXPECT_EQ(resize_succeeded_result.value().size(), log_entries.size());
    for (std::size_t i = 0; i < log_entries.size(); ++i)
    {
        EXPECT_EQ(resize_succeeded_result.value()[i].SerializeAsString(), log_entries[i].SerializeAsString());
    }
}