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
            "deploy.release.requested.#{state.node_id}"
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
      handle_request_reply(msg)
    else
      handle_pub_sub(msg, state)
    end
  end

  defp handle_request_reply(msg) do
    case msg.topic do
      # Add your request/reply handlers here
      # "example.task.list" ->
      #   handle_task_list(msg, state)
      _ ->
        Logger.debug("Unknown request/reply subject: #{msg.topic}")
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
        # Execute asynchronously — a deploy takes minutes and blocking here
        # would stall the consumer (heartbeats, registry presence)
        ctx = %{bot_id: "deploy_pipeline_bot", node: state.node_id}

        Task.start(fn ->
          case BotArmyDeployPipeline.Skills.Deploy.execute(payload, ctx) do
            {:ok, result} ->
              bot = payload["bot"]
              Logger.info("[Deploy] Skill execution completed: #{inspect(bot)}")
              publish_deploy_outcome("ops.deploy.complete", bot, payload, result)

            {:error, reason} ->
              bot = payload["bot"]
              Logger.error("[Deploy] Skill execution failed: #{inspect(reason)}")
              publish_deploy_outcome("ops.deploy.failed", bot, payload, %{error: inspect(reason)})
          end
        end)

      {:error, reason} ->
        Logger.warning("[Deploy] #{topic} failed validation: #{inspect(reason)}")
    end
  end

  defp route_message(_message, topic, _state) do
    Logger.debug("Routing message from #{topic}")
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
