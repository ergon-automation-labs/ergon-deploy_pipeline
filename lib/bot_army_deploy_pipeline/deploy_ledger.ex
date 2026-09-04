defmodule BotArmyDeployPipeline.DeployLedger do
  @moduledoc """
  Owns the deploy ledger and reconciles deploys whose dispatching beam died
  mid-flight.

  The critical case is the pipeline redeploying ITSELF: `deploy_bot_with_summary.sh`
  ends with a launchctl kickstart of the pipeline, which kills the dispatching
  beam before `System.cmd/3` returns. The dispatching Task never logs success and
  never publishes `ops.deploy.complete` — the deploy may have fully succeeded while
  the pipeline's own record of it vanished with the dying process.

  Two durable artifacts make confirmation possible anyway:

  1. **Pending record** (this module, written BEFORE dispatching the script):
     `/var/log/bot_army/deploy_pipeline_pending.jsonl` — one JSON line per node
     deploy: `%{bot, node, version, tag, dispatched_at, status}`. Status transitions:
     dispatched -> completed | failed | reconciled | reconciled_failed |
     reconciled_unverified | unverifiable.

  2. **Result record** (written by the deploy script as abby, after verifying the
     target node actually RUNS the deployed version):
     `<infra>/scripts/.deploy_results.jsonl` — `%{ts, bot, node, version, tag, exit,
     verified, detail}`. This is ground truth: it is written even when the
     dispatching beam is killed mid-`System.cmd` (the script process is orphaned
     and runs to completion).

  On boot and every 60s this GenServer scans for pending records older than 45s
  (young ones may still be mid-flight) and reconciles them against the script's
  result records, publishing the late `ops.deploy.complete` / `ops.deploy.failed`
  so subscribers (claude_bridge subscribes `ops.deploy.>`) see the true outcome.
  """

  use GenServer
  require Logger

  alias BotArmyLibraryRuntime.NATS.Publisher

  @pending_path "/var/log/bot_army/deploy_pipeline_pending.jsonl"
  @scan_interval_ms 60_000
  @first_scan_delay_ms 20_000
  # Younger than this: the deploy script may still be running (salt apply can
  # take minutes) — the script writes its record when the beam check finishes.
  @reconcile_after_s 45
  # A dispatched record with no matching script record after this long is
  # marked unverifiable (e.g. dispatched by a pipeline version predating the
  # script's record writing).
  @max_pending_age_s 3600
  @prune_after_s 86_400

  # -- Public API ------------------------------------------------------------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def record_dispatched(bot, node, version, tag) do
    GenServer.cast(__MODULE__, {:dispatched, bot, node, version, tag})
  end

  def record_finished(bot, node, version, status) do
    GenServer.cast(__MODULE__, {:finished, bot, node, version, status})
  end

  # -- GenServer -------------------------------------------------------------

  @impl true
  def init(_opts) do
    schedule_scan(@first_scan_delay_ms)
    {:ok, %{}}
  end

  @impl true
  def handle_cast({:dispatched, bot, node, version, tag}, state) do
    record = %{
      "bot" => bot,
      "node" => node,
      "version" => version,
      "tag" => tag,
      "dispatched_at" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "status" => "dispatched"
    }

    append_line(@pending_path, record)
    Logger.info("[Ledger] Dispatched: #{bot}/#{node} v#{version}")
    {:noreply, state}
  end

  def handle_cast({:finished, bot, node, version, status}, state) do
    update_status(bot, node, version, status)
    {:noreply, state}
  end

  @impl true
  def handle_info(:scan, state) do
    reconcile()
    schedule_scan(@scan_interval_ms)
    {:noreply, state}
  end

  # -- Reconciliation --------------------------------------------------------

  defp reconcile do
    lines = read_lines(@pending_path)
    now = DateTime.utc_now()

    {updated_lines, reconciled?} =
      Enum.map_reduce(lines, false, fn line, acc ->
        with %{"status" => "dispatched", "bot" => bot, "node" => node, "version" => version} <-
               line,
             {:ok, dt, _} <- DateTime.from_iso8601(line["dispatched_at"] || ""),
             age when age >= @reconcile_after_s <- DateTime.diff(now, dt) do
          cond do
            age >= @max_pending_age_s ->
              Logger.error(
                "[Ledger] UNVERIFIABLE deploy: #{bot}/#{node} v#{version} dispatched #{div(age, 60)}m ago, no result record — outcome unknown"
              )

              {Map.put(line, "status", "unverifiable"), true}

            true ->
              case resolve(bot, node, version, line) do
                {:publish, subject, extra, new_status} ->
                  publish_outcome(subject, line, extra)
                  {Map.put(line, "status", new_status), true}

                :no_record ->
                  # Script still running (salt apply can take minutes) — retry next scan
                  {line, acc}
              end
          end
        else
          _ -> {line, acc}
        end
      end)

    if reconciled? do
      rewrite_lines(@pending_path, updated_lines)
    end

    prune_old(updated_lines, now)
    :ok
  end

  defp resolve(bot, node, version, pending) do
    case latest_script_record(bot, node, version, pending["dispatched_at"]) do
      {:ok, rec} ->
        verified = rec["verified"]

        cond do
          verified == "true" ->
            Logger.info(
              "[Ledger] Reconciling #{bot}/#{node} v#{version}: script record VERIFIED -> late ops.deploy.complete"
            )

            {:publish, "ops.deploy.complete",
             %{"reconciled" => true, "verified" => true, "detail" => rec["detail"]},
             "reconciled"}

          verified == "unknown" ->
            Logger.warning(
              "[Ledger] Reconciling #{bot}/#{node} v#{version}: script exited 0 but could not verify (#{rec["detail"]})"
            )

            {:publish, "ops.deploy.complete",
             %{"reconciled" => true, "verified" => "unknown", "detail" => rec["detail"]},
             "reconciled_unverified"}

          true ->
            Logger.error(
              "[Ledger] Reconciling #{bot}/#{node} v#{version}: script record says NOT verified (#{rec["detail"]}) -> late ops.deploy.failed"
            )

            {:publish, "ops.deploy.failed",
             %{"reconciled" => true, "verified" => false, "error" => rec["detail"]},
             "reconciled_failed"}
        end

      :no_record ->
        :no_record
    end
  end

  defp latest_script_record(bot, node, version, dispatched_at) do
    with {:ok, dt, _} <- DateTime.from_iso8601(dispatched_at || ""),
         records when is_list(records) <- script_records() do
      matching =
        Enum.filter(records, fn rec ->
          rec["bot"] == bot and rec["node"] == node and rec["version"] == version and
            ts_at_least?(rec["ts"], DateTime.add(dt, -5))
        end)

      case matching do
        [] -> :no_record
        _ -> {:ok, Enum.max_by(matching, & &1["ts"])}
      end
    else
      _ -> :no_record
    end
  end

  defp ts_at_least?(ts, cutoff) do
    case DateTime.from_iso8601(ts || "") do
      {:ok, dt, _} -> DateTime.compare(dt, cutoff) != :lt
      _ -> false
    end
  end

  defp script_records do
    read_lines(results_file())
  end

  defp results_file do
    Application.get_env(
      :bot_army_deploy_pipeline,
      :deploy_results_file,
      "/Users/abby/code/bots/bot_army_infra/scripts/.deploy_results.jsonl"
    )
  end

  defp publish_outcome(subject, line, extra) do
    event_payload =
      Map.merge(
        %{
          "bot" => line["bot"] || "unknown",
          "release_tag" => line["tag"],
          "version" => line["version"],
          "target" => line["node"],
          "deployed_by" => "deploy_pipeline_bot"
        },
        extra
      )

    case Publisher.publish(subject, event_payload) do
      {:ok, _subject} ->
        Logger.info("[Ledger] Published late #{subject} for #{line["bot"]}")

      {:error, reason} ->
        Logger.warning("[Ledger] Failed to publish late #{subject}: #{inspect(reason)}")
    end
  end

  # -- File helpers ----------------------------------------------------------

  defp read_lines(path) do
    case File.read(path) do
      {:ok, content} ->
        content
        |> String.split("\n", trim: true)
        |> Enum.map(&Jason.decode/1)
        |> Enum.filter(&match?({:ok, %{}}, &1))
        |> Enum.map(&elem(&1, 0))

      {:error, _reason} ->
        []
    end
  end

  defp append_line(path, record) do
    case File.write(path, Jason.encode!(record) <> "\n", [:append]) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning("[Ledger] Could not append pending record (#{inspect(reason)})")
        :ok
    end
  end

  defp rewrite_lines(path, lines) do
    content =
      case lines do
        [] -> ""
        _ -> Enum.map_join(lines, "\n", &Jason.encode!/1) <> "\n"
      end

    case File.write(path, content) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning("[Ledger] Could not rewrite pending file (#{inspect(reason)})")
        :ok
    end
  end

  defp update_status(bot, node, version, status) do
    lines = read_lines(@pending_path)

    updated =
      Enum.map(lines, fn line ->
        if line["bot"] == bot and line["node"] == node and line["version"] == version and
             line["status"] == "dispatched" do
          Map.put(line, "status", status)
        else
          line
        end
      end)

    if updated != lines do
      rewrite_lines(@pending_path, updated)
    end
  end

  defp prune_old(lines, now) do
    cutoff = DateTime.add(now, -@prune_after_s)

    kept =
      Enum.filter(lines, fn line ->
        case DateTime.from_iso8601(line["dispatched_at"] || "") do
          {:ok, dt, _} -> DateTime.compare(dt, cutoff) == :gt
          _ -> true
        end
      end)

    if length(kept) != length(lines) do
      rewrite_lines(@pending_path, kept)
    end
  end

  defp schedule_scan(delay) do
    Process.send_after(self(), :scan, delay)
  end
end