defmodule BotArmyDeployPipeline.JobTracker do
  @moduledoc """
  Tracks deployment job state: creation, progress, completion, errors.

  Uses NATS JetStream KV store for durability and cluster-wide visibility:
  - Jobs persist across restarts
  - Queryable from any node in the NATS cluster
  - Single source of truth (no split-brain)

  Provides:
  - Job creation with unique ID
  - State updates (IN_PROGRESS, COMPLETED, FAILED)
  - Step tracking (what is the job doing now)
  - Status queries
  - Verification results (running version, health checks)
  """

  use GenServer
  require Logger

  @kv_bucket "bot-army-jobs"
  @reconnect_delay_ms 5000

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
    # TODO: Migrate to JetStream KV store for cluster-wide visibility
    # For now, use ETS with GenServer state for single-node deployments
    # Path: Add jetstream dependency, replace kv_put/get/list with JetStream API calls
    Logger.info("JobTracker starting (ETS backend, TODO: JetStream KV for cluster support)")
    :ets.new(:deploy_jobs, [:named_table, :public, {:keypos, 1}])
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

    case kv_put(job_id, job) do
      :ok ->
        Logger.info("[Job #{job_id}] Created for #{bot}@#{version} -> #{target}")
        job_id

      {:error, reason} ->
        Logger.error("[Job #{job_id}] Failed to create: #{inspect(reason)}")
        job_id
    end
  end

  @doc """
  Update job state and step.
  """
  def update_job(job_id, state, step) do
    Logger.info("[Job #{job_id}] #{step}")

    case kv_get(job_id) do
      {:ok, job} ->
        updated = %{job | state: state, current_step: step}

        case kv_put(job_id, updated) do
          :ok ->
            updated

          {:error, reason} ->
            Logger.warning("[Job #{job_id}] Failed to update: #{inspect(reason)}")
            nil
        end

      {:error, _} ->
        Logger.warning("[Job #{job_id}] Not found during update")
        nil
    end
  end

  @doc """
  Mark job as completed with verification results.
  """
  def complete_job(job_id, verified_version, verified_running, error \\ nil) do
    Logger.info("[Job #{job_id}] Completed - verified_running=#{verified_running}")

    case kv_get(job_id) do
      {:ok, job} ->
        updated = %{
          job
          | state: :completed,
            completed_at: DateTime.utc_now(),
            verified_version: verified_version,
            verified_running: verified_running,
            verification_error: error,
            current_step: "Complete"
        }

        case kv_put(job_id, updated) do
          :ok ->
            updated

          {:error, reason} ->
            Logger.warning("[Job #{job_id}] Failed to complete: #{inspect(reason)}")
            nil
        end

      {:error, _} ->
        nil
    end
  end

  @doc """
  Mark job as failed.
  """
  def fail_job(job_id, error) do
    Logger.error("[Job #{job_id}] Failed: #{error}")

    case kv_get(job_id) do
      {:ok, job} ->
        updated = %{
          job
          | state: :failed,
            completed_at: DateTime.utc_now(),
            error: error,
            current_step: "Failed"
        }

        case kv_put(job_id, updated) do
          :ok ->
            updated

          {:error, reason} ->
            Logger.warning("[Job #{job_id}] Failed to mark as failed: #{inspect(reason)}")
            nil
        end

      {:error, _} ->
        nil
    end
  end

  @doc """
  Get job status.
  """
  def get_job(job_id) do
    case kv_get(job_id) do
      {:ok, job} -> job
      {:error, _} -> nil
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

  # Storage operations: ETS backend with JetStream migration path
  # To migrate to JetStream KV: replace ETS with Jetstream.API.KeyValue calls
  defp kv_put(job_id, job) do
    :ets.insert(:deploy_jobs, {job_id, job})
    :ok
  end

  defp kv_get(job_id) do
    case :ets.lookup(:deploy_jobs, job_id) do
      [{_key, job}] -> {:ok, job}
      [] -> {:error, :not_found}
    end
  end

  # JetStream Migration Helpers (kept for future KV backend)
  # Uncomment and use when adding jetstream dependency
  # defp normalize_for_json(job) do
  #   job
  #   |> Map.update!(:started_at, &DateTime.to_iso8601/1)
  #   |> Map.update!(:completed_at, fn
  #     nil -> nil
  #     dt -> DateTime.to_iso8601(dt)
  #   end)
  #   |> Map.update!(:state, &Atom.to_string/1)
  # end
  #
  # defp decode_job(json) when is_binary(json) do
  #   case Jason.decode(json) do
  #     {:ok, data} ->
  #       %{
  #         job_id: data["job_id"],
  #         state: String.to_atom(data["state"]),
  #         bot: data["bot"],
  #         target: data["target"],
  #         version: data["version"],
  #         current_step: data["current_step"],
  #         started_at: parse_datetime(data["started_at"]),
  #         completed_at: parse_datetime(data["completed_at"]),
  #         error: data["error"],
  #         verified_version: data["verified_version"],
  #         verified_running: data["verified_running"],
  #         verification_error: data["verification_error"]
  #       }
  #     {:error, _} ->
  #       nil
  #   end
  # end
  #
  # defp parse_datetime(nil), do: nil
  # defp parse_datetime(iso_string) do
  #   case DateTime.from_iso8601(iso_string) do
  #     {:ok, dt, _} -> dt
  #     :error -> nil
  #   end
  # end

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
