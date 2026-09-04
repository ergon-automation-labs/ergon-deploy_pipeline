defmodule BotArmyDeployPipeline.JobTracker do
  @moduledoc """
  Tracks deployment job state: creation, progress, completion, errors.

  Provides:
  - Job creation with unique ID
  - State updates (IN_PROGRESS, COMPLETED, FAILED)
  - Step tracking (what is the job doing now)
  - Status queries
  - Verification results (running version, health checks)
  """

  use GenServer
  require Logger

  @type job_state :: :pending | :in_progress | :completed | :failed
  @type job_info :: %{
          job_id: String.t(),
          state: job_state,
          bot: String.t(),
          target: String.t(),
          version: String.t(),
          current_step: String.t(),
          started_at: DateTime.t(),
          completed_at: DateTime.t() | nil,
          error: String.t() | nil,
          verified_version: String.t() | nil,
          verified_running: boolean(),
          verification_error: String.t() | nil
        }

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    # Use ETS for persistence across GenServer restarts
    :ets.new(:deploy_jobs, [:named_table, :public, {:keypos, 1}])
    Logger.info("JobTracker started")
    {:ok, %{}}
  end

  @doc """
  Create a new deployment job and return its ID.
  """
  def create_job(bot, target, version, request_id \\ nil) do
    job_id = request_id || generate_job_id()

    job = %{
      job_id: job_id,
      state: :pending,
      bot: bot,
      target: target,
      version: version,
      current_step: "Initializing",
      started_at: DateTime.utc_now(),
      completed_at: nil,
      error: nil,
      verified_version: nil,
      verified_running: false,
      verification_error: nil
    }

    :ets.insert(:deploy_jobs, {job_id, job})
    Logger.info("[Job #{job_id}] Created for #{bot}@#{version} -> #{target}")
    job_id
  end

  @doc """
  Update job state and step.
  """
  def update_job(job_id, state, step) do
    Logger.info("[Job #{job_id}] #{step}")

    case :ets.lookup(:deploy_jobs, job_id) do
      [{_key, job}] ->
        updated = %{job | state: state, current_step: step}
        :ets.insert(:deploy_jobs, {job_id, updated})
        updated

      [] ->
        Logger.warning("[Job #{job_id}] Not found during update")
        nil
    end
  end

  @doc """
  Mark job as completed with verification results.
  """
  def complete_job(job_id, verified_version, verified_running, error \\ nil) do
    Logger.info("[Job #{job_id}] Completed - verified_running=#{verified_running}")

    case :ets.lookup(:deploy_jobs, job_id) do
      [{_key, job}] ->
        updated = %{
          job
          | state: :completed,
            completed_at: DateTime.utc_now(),
            verified_version: verified_version,
            verified_running: verified_running,
            verification_error: error,
            current_step: "Complete"
        }

        :ets.insert(:deploy_jobs, {job_id, updated})
        updated

      [] ->
        nil
    end
  end

  @doc """
  Mark job as failed.
  """
  def fail_job(job_id, error) do
    Logger.error("[Job #{job_id}] Failed: #{error}")

    case :ets.lookup(:deploy_jobs, job_id) do
      [{_key, job}] ->
        updated = %{
          job
          | state: :failed,
            completed_at: DateTime.utc_now(),
            error: error,
            current_step: "Failed"
        }

        :ets.insert(:deploy_jobs, {job_id, updated})
        updated

      [] ->
        nil
    end
  end

  @doc """
  Get job status.
  """
  def get_job(job_id) do
    case :ets.lookup(:deploy_jobs, job_id) do
      [{_key, job}] -> job
      [] -> nil
    end
  end

  @doc """
  List all jobs (for debugging).
  """
  def list_jobs do
    :ets.tab2list(:deploy_jobs) |> Enum.map(fn {_key, job} -> job end)
  end

  @doc """
  Find an active (pending or in_progress) job for a bot+target, if any.

  Backs the consumer's dedup guard (one in-flight deploy per bot+target) and
  the deploy.release.status query.
  """
  @spec find_active(String.t(), String.t()) :: map() | nil
  def find_active(bot, target) do
    list_jobs()
    |> Enum.find(fn job ->
      job.bot == bot and job.target == target and job.state in [:pending, :in_progress]
    end)
  end

  @doc """
  Most recent finished (completed or failed) job for a bot+target, if any.

  In-memory only — vanishes when the pipeline restarts; the durable answer
  for "is it deployed?" comes from DeployLedger.
  """
  @spec latest_finished(String.t(), String.t()) :: map() | nil
  def latest_finished(bot, target) do
    list_jobs()
    |> Enum.filter(fn job ->
      job.bot == bot and job.target == target and job.state in [:completed, :failed]
    end)
    |> Enum.sort_by(& &1.started_at, {:desc, DateTime})
    |> List.first()
  end

  # Generate unique job ID with timestamp
  defp generate_job_id do
    timestamp =
      DateTime.utc_now()
      |> DateTime.to_iso8601()
      |> String.replace("-", "")
      |> String.replace(":", "")

    random = :crypto.strong_rand_bytes(4) |> Base.encode16(case: :lower)
    "deploy-#{timestamp}-#{random}"
  end
end
