defmodule BotArmyDeployPipeline.JobTrackerTest do
  use ExUnit.Case, async: false

  alias BotArmyDeployPipeline.JobTracker

  setup do
    # Application does not start JobTracker in :test env — start it here so
    # the named ETS table exists for each test (fresh per test via
    # start_supervised teardown).
    start_supervised!(JobTracker)
    :ok
  end

  test "find_active returns a pending job for the matching bot+target" do
    JobTracker.create_job("gtd", "air", "0.1.0", "jt-test-1")

    active = JobTracker.find_active("gtd", "air")
    assert %{} = active
    assert active.job_id == "jt-test-1"
    assert active.state == :pending
  end

  test "find_active is scoped to bot+target" do
    JobTracker.create_job("gtd", "air", "0.1.0", "jt-test-2")

    assert JobTracker.find_active("gtd", "mini") == nil
    assert JobTracker.find_active("bridge", "air") == nil
  end

  test "find_active ignores completed jobs — only in-flight deploys block" do
    JobTracker.create_job("gtd", "air", "0.1.0", "jt-test-3")
    JobTracker.complete_job("jt-test-3", "0.1.0", true)

    assert JobTracker.find_active("gtd", "air") == nil
  end

  test "latest_finished returns the most recent completed job" do
    JobTracker.create_job("gtd", "air", "0.1.0", "jt-test-4a")
    JobTracker.complete_job("jt-test-4a", "0.1.0", true)
    JobTracker.create_job("gtd", "air", "0.1.1", "jt-test-4b")
    JobTracker.complete_job("jt-test-4b", "0.1.1", true)

    finished = JobTracker.latest_finished("gtd", "air")
    assert finished.job_id == "jt-test-4b"
    assert finished.verified_version == "0.1.1"
  end

  test "latest_finished returns nil when nothing has finished" do
    assert JobTracker.latest_finished("nonexistent_bot", "air") == nil
  end
end
