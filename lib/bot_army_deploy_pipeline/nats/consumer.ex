defmodule BotArmyDeployPipeline.NATS.Consumer do
  @moduledoc """
  NATS message consumer for deploy_pipeline with split-brain routing.

  Subscribes to NATS subjects and routes messages to handlers.
  Uses standardized Reply format for request/reply patterns.

  Split-brain routing (running on both air and mini):
  - deploy.release.requested: generic (queue group load-balances)
  - deploy.release.requested.air: air-only
  - deploy.release.requested.mini: mini-only

  All request/reply handlers should return responses using Reply helpers:
  - BotArmyLibraryRuntime.NATS.Reply.ok(data) for success
  - BotArmyLibraryRuntime.NATS.Reply.error(message, code) for errors
  """

  use GenServer
  require Logger

  @reconnect_delay_ms 5000
  @version Mix.Project.config()[:version]

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    Logger.info("Starting NATS consumer")

    state = %{
      subscriptions: [],
      conn: nil,
      opts: opts,
      node_id: determine_node_id()
    }

    Logger.info("Running on node: #{state.node_id}")
    {:ok, state, {:continue, :connect}}
  end

  @impl true
  def handle_continue(:connect, state) do
    case GenServer.call(BotArmyLibraryRuntime.NATS.Connection, :get_connection, 5000) do
      {:ok, conn} ->
        BotArmyLibraryRuntime.NATS.Connection.subscribe_to_status()
        Logger.info("Connected to NATS, subscribing to topics")

        subscriptions =
          [
            "deploy.release.requested",
            "deploy.release.requested.#{state.node_id}",
            # Request/reply surfaces
            "deploy.release.status",
            "deploy.job.status.*"
          ]
          |> Enum.map(&subscribe(conn, &1))
          |> Enum.filter(&(not is_nil(&1)))

        # Register subjects for runtime discovery
        subjects = build_subjects(state.node_id)
        BotArmyLibraryRuntime.Registry.register("deploy_pipeline", subjects, @version)

        {:noreply, %{state | subscriptions: subscriptions, conn: conn}}

      {:error, _reason} ->
        Logger.warning("NATS connection not ready, will retry")
        Process.send_after(self(), :connect_retry, @reconnect_delay_ms)
        {:noreply, state}
    end
  end

  # Determine which node we're on (air or mini)
  defp determine_node_id do
    case System.get_env("DEPLOY_NODE_ID") do
      nil ->
        # Fallback: detect from hostname
        case System.cmd("hostname", []) do
          {hostname, 0} ->
            h = hostname |> String.trim() |> String.downcase()

            if String.contains?(h, "mini") do
              "mini"
            else
              "air"
            end

          _ ->
            "air"
        end

      node_id ->
        node_id |> String.trim() |> String.downcase()
    end
  end

  # Build subject registry for this node
  defp build_subjects(node_id) do
    [
      %{
        subject: "deploy.release.requested",
        type: :subscribe,
        description: "Generic deployments (load-balanced via queue group)"
      },
      %{
        subject: "deploy.release.requested.#{node_id}",
        type: :subscribe,
        description: "Node-specific deployments (#{node_id} only)"
      },
      %{
        subject: "deploy.release.status",
        type: :request,
        description:
          "Query release state by {bot, target, version?} — none|scheduled|in_progress|deployed|failed|deployed_unverified|unknown"
      },
      %{
        subject: "deploy.job.status.*",
        type: :request,
        description: "Per-job status by job_id (deploy-bot emits request_id as the job id)"
      }
    ]
  end

  defp subscribe(conn, subject) do
    # Generic subjects use queue group for load-balancing; node-specific are exclusive
    opts =
      if String.ends_with?(subject, ".air") or String.ends_with?(subject, ".mini") do
        []
      else
        [queue_group: "deploy_pipeline_bot"]
      end

    case Gnat.sub(conn, self(), subject, opts) do
      {:ok, sub} ->
        queue_info = if Enum.empty?(opts), do: "(exclusive)", else: "(queue group)"
        Logger.info("Subscribed to #{subject} #{queue_info}")
        sub

      {:error, reason} ->
        Logger.error("Failed to subscribe to #{subject}: #{inspect(reason)}")
        nil
    end
  end

  @impl true
  def handle_info(:connect_retry, state) do
    {:noreply, state, {:continue, :connect}}
  end

  @impl true
  def handle_info({:msg, msg}, state) do
    BotArmyLibraryRuntime.Tracing.with_consumer_span(msg.topic, Map.get(msg, :headers), fn ->
      process_message(msg, state)
    end)

    {:noreply, state}
  end

  @impl true
  def handle_info({:nats, :disconnected}, state) do
    Logger.warning("Disconnected from NATS, will reconnect")
    Process.send_after(self(), :connect_retry, @reconnect_delay_ms)
    {:noreply, %{state | subscriptions: [], conn: nil}}
  end

  @impl true
  def handle_info({:nats, :connected}, state) do
    Logger.info("Reconnected to NATS, re-subscribing")
    {:noreply, state, {:continue, :connect}}
  end

  @impl true
  def handle_info(:reconnect, state) do
    {:noreply, state, {:continue, :connect}}
  end

  defp process_message(msg, state) do
    Logger.debug("Received NATS message on subject: #{msg.topic}")

    if msg.reply_to do
      handle_request_reply(msg, state)
    else
      handle_pub_sub(msg, state)
    end
  end

  defp handle_request_reply(msg, state) do
    case msg.topic do
      "deploy.job.status." <> job_id ->
        handle_job_status(msg, job_id, state)

      "deploy.release.status" ->
        handle_release_status(msg, state)

      _ ->
        Logger.debug("Unknown request/reply subject: #{msg.topic}")
    end
  end

  defp handle_job_status(msg, job_id, state) do
    case BotArmyDeployPipeline.JobTracker.get_job(job_id) do
      nil ->
        response = BotArmyLibraryRuntime.NATS.Reply.error("Job not found: #{job_id}", :not_found)
        if msg.reply_to and state.conn, do: Gnat.pub(state.conn, msg.reply_to, response)

      job ->
        response_data = %{
          "job_id" => job.job_id,
          "state" => Atom.to_string(job.state),
          "bot" => job.bot,
          "target" => job.target,
          "version" => job.version,
          "current_step" => job.current_step,
          "started_at" => DateTime.to_iso8601(job.started_at),
          "completed_at" => if(job.completed_at, do: DateTime.to_iso8601(job.completed_at)),
          "verified_running" => job.verified_running,
          "verified_version" => job.verified_version,
          "error" => job.error
        }

        response = BotArmyLibraryRuntime.NATS.Reply.ok(response_data)
        if msg.reply_to and state.conn, do: Gnat.pub(state.conn, msg.reply_to, response)
    end
  end

  # Body: {"bot": "name", "target": "air", "version": "1.2.3"} — target
  # optional (defaults to this node), version optional. Merges live JobTracker
  # state with the durable DeployLedger: "is it going now?" answers from
  # JobTracker, "is it deployed?" grounds on the ledger (which survives
  # pipeline restarts).
  defp handle_release_status(msg, state) do
    response =
      case Jason.decode(msg.body || "{}") do
        {:ok, %{"bot" => bot} = params} when is_binary(bot) and bot != "" ->
          target = params["target"] || state.node_id

          BotArmyLibraryRuntime.NATS.Reply.ok(release_status(bot, target, params["version"]))

        _ ->
          BotArmyLibraryRuntime.NATS.Reply.error(
            "bot is required: {\"bot\": \"name\", \"target\": \"air\", \"version\": \"1.2.3\"}",
            :invalid_input
          )
      end

    if state.conn, do: Gnat.pub(state.conn, msg.reply_to, response)
  end

  defp release_status(bot, target, version) do
    cond do
      active = BotArmyDeployPipeline.JobTracker.find_active(bot, target) ->
        %{
          "state" => if(active.state == :pending, do: "scheduled", else: "in_progress"),
          "bot" => bot,
          "target" => target,
          "version" => active.version,
          "job_id" => active.job_id,
          "current_step" => active.current_step,
          "started_at" => DateTime.to_iso8601(active.started_at)
        }

      finished = BotArmyDeployPipeline.JobTracker.latest_finished(bot, target) ->
        %{
          "state" => Atom.to_string(finished.state),
          "bot" => bot,
          "target" => target,
          "version" => finished.verified_version || finished.version,
          "job_id" => finished.job_id,
          "verified_running" => finished.verified_running,
          "completed_at" => finished.completed_at && DateTime.to_iso8601(finished.completed_at)
        }

      record = BotArmyDeployPipeline.DeployLedger.latest(bot, target) ->
        %{
          "state" => ledger_state(record["status"]),
          "bot" => bot,
          "target" => target,
          "version" => record["version"],
          "dispatched_at" => record["dispatched_at"],
          "source" => "deploy_ledger"
        }

      true ->
        %{"state" => "none", "bot" => bot, "target" => target}
    end
  end

  # Ledger statuses (dispatched -> completed|failed|reconciled|...) mapped to
  # the caller-facing state machine.
  defp ledger_state(status) do
    case status do
      "dispatched" -> "in_progress"
      "completed" -> "deployed"
      "reconciled" -> "deployed"
      "reconciled_failed" -> "failed"
      "failed" -> "failed"
      "reconciled_unverified" -> "deployed_unverified"
      "unverifiable" -> "unknown"
      _ -> "unknown"
    end
  end

  defp handle_pub_sub(msg, state) do
    case BotArmyLibraryCore.NATS.Decoder.decode(msg.body) do
      {:ok, decoded_message} ->
        route_message(decoded_message, msg.topic, state)

      {:error, reason} ->
        Logger.warning("Failed to decode message from #{msg.topic}: #{inspect(reason)}")
    end
  end

  # Message routing
  # (literal pattern, not a String.starts_with? guard — remote calls are not
  # permitted in guards and only explode on full recompiles)
  defp route_message(message, "deploy.release.requested" = topic, state) do
    Logger.info("Received #{topic} on #{state.node_id} — dispatching to deploy skill")
    # Decoder returns the full envelope; the deploy fields live in "payload"
    payload = Map.get(message, "payload", message)

    case BotArmyDeployPipeline.Skills.Deploy.validate(payload) do
      :ok ->
        bot = payload["bot"]
        target = payload["target"] || state.node_id
        version = payload["version"]
        request_id = payload["request_id"]

        # Dedup guard: one in-flight deploy per bot+target. A second event for
        # the same release (double-fired deploy-bot, two sessions racing) is
        # rejected here instead of racing the first — the surviving job still
        # publishes ground-truth ops.deploy.complete.
        case BotArmyDeployPipeline.JobTracker.find_active(bot, target) do
          nil ->
            # Create job tracker entry
            job_id =
              BotArmyDeployPipeline.JobTracker.create_job(bot, target, version, request_id)

            BotArmyDeployPipeline.JobTracker.update_job(
              job_id,
              :in_progress,
              "Syncing bot to #{target}"
            )

            # Execute asynchronously — a deploy takes minutes and blocking here
            # would stall the consumer (heartbeats, registry presence)
            ctx = %{bot_id: "deploy_pipeline_bot", node: state.node_id, job_id: job_id}

            Task.start(fn ->
              case BotArmyDeployPipeline.Skills.Deploy.execute(payload, ctx) do
                {:ok, result} ->
                  Logger.info("[Deploy #{job_id}] Skill execution completed: #{inspect(bot)}")

                  # Verify the deployment
                  verified_version = result["verified_version"] || version
                  verified_running = result["verified_running"] || false

                  BotArmyDeployPipeline.JobTracker.complete_job(
                    job_id,
                    verified_version,
                    verified_running,
                    result["error"]
                  )

                  # Emit metrics
                  emit_deploy_metric(bot, target, version, verified_running)

                  publish_deploy_outcome("ops.deploy.complete", bot, payload, %{
                    job_id: job_id,
                    verified_running: verified_running
                  })

                {:error, reason} ->
                  Logger.error("[Deploy #{job_id}] Skill execution failed: #{inspect(reason)}")
                  BotArmyDeployPipeline.JobTracker.fail_job(job_id, inspect(reason))

                  publish_deploy_outcome("ops.deploy.failed", bot, payload, %{
                    error: inspect(reason),
                    job_id: job_id
                  })
              end
            end)

          active ->
            Logger.warning(
              "[Deploy] DUPLICATE REJECTED: #{bot} v#{version} -> #{target} — job #{active.job_id} (v#{active.version}) already #{active.state}"
            )

            publish_deploy_outcome("ops.deploy.rejected", bot, payload, %{
              reason: "duplicate_in_progress",
              existing_job_id: active.job_id,
              existing_version: active.version
            })
        end

      {:error, reason} ->
        Logger.warning("[Deploy] #{topic} failed validation: #{inspect(reason)}")
    end
  end

  defp route_message(_message, topic, _state) do
    Logger.debug("Routing message from #{topic}")
  end

  # Emit deployment metrics
  defp emit_deploy_metric(bot, target, version, verified_running) do
    state_metric = if verified_running, do: "up", else: "down"

    BotArmyLibraryRuntime.NATS.Publisher.publish("metrics.deploy.bot", %{
      "bot" => bot,
      "target" => target,
      "version" => version,
      "state" => state_metric,
      "verified_running" => verified_running,
      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
    })
  end

  # Announce deploy outcomes on ops.deploy.complete / ops.deploy.failed so
  # subscribers (claude_bridge subscribes ops.deploy.>) can react.
  defp publish_deploy_outcome(subject, bot, payload, extra) do
    event_payload =
      Map.merge(
        %{
          "bot" => bot || "unknown",
          "release_tag" => payload["tag"],
          "version" => payload["version"],
          "target" => payload["target"],
          "deployed_by" => "deploy_pipeline_bot"
        },
        extra
      )

    case BotArmyLibraryRuntime.NATS.Publisher.publish(subject, event_payload) do
      {:ok, _subject} ->
        Logger.info("[Deploy] Published #{subject} for #{bot}")

      {:error, reason} ->
        Logger.warning("[Deploy] Failed to publish #{subject}: #{inspect(reason)}")
    end
  end

  # Request/reply handlers
  # defp handle_task_list(msg, state) do
  #   response =
  #     case get_tasks() do
  #       {:ok, tasks} ->
  #         BotArmyLibraryRuntime.NATS.Reply.ok(%{"tasks" => tasks})
  #
  #       {:error, reason} ->
  #         BotArmyLibraryRuntime.NATS.Reply.error(inspect(reason), :list_failed)
  #     end
  #
  #   if state.conn do
  #     Gnat.pub(state.conn, msg.reply_to, response)
  #   end
  # end
end
