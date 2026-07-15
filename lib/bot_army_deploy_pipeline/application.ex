defmodule BotArmyDeployPipeline.Application do
  @moduledoc """
  Deploy Pipeline Bot application supervisor.

  Follows bot army pattern with environment-aware startup:
  - Repo not started in :test (tests inject mocks)
  - PulsePublisher sends `system.health` liveness every 30s and rich `bot.<service>.pulse` every 30 minutes
  - Workers not started in :test (gated by @env)

  Observability: see `PulsePublisher` — fleet UIs keyed on Synapse hydration should use `system.health` freshness (90s), not pulse interval alone.
  """

  use Application

  @env Mix.env()

  # dialyzer analyzes this module under MIX_ENV=dev by default, where `@env` is the
  # compile-time literal `:dev` — making every `@env == :test` branch below a provable
  # tautology (dialyxir :exact_eq). This is the fleet-wide compile-time env-gating pattern
  # (CLAUDE.md: "Application.ex gates stores with `if @env == :test`"), not a runtime
  # defect: each release is compiled once per env and only one branch is ever live in a
  # given build. Scoped to exactly the three gating functions below — tracked fleet-wide
  # in GTD task deploy-pipeline-dialyzer-env-gating.
  @dialyzer {:nowarn_function,
             maybe_add_pulse_publisher: 1, maybe_add_nats_consumer: 1, maybe_add_workers: 1}

  @impl true
  def start(_type, _args) do
    # Note: BotArmyRuntime.Telemetry and BotArmyRuntime.NATS.Connection are started
    # by bot_army_runtime automatically — do not add them here.

    children =
      []
      |> maybe_add_pulse_publisher()
      |> maybe_add_nats_consumer()
      |> maybe_add_workers()

    opts = [strategy: :one_for_one, name: BotArmyDeployPipeline.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp maybe_add_pulse_publisher(children) do
    if @env == :test do
      children
    else
      [{BotArmyDeployPipeline.PulsePublisher, []} | children]
    end
  end

  defp maybe_add_nats_consumer(children) do
    if @env == :test do
      children
    else
      [{BotArmyDeployPipeline.NATS.Consumer, []} | children]
    end
  end

  defp maybe_add_workers(children) do
    if @env == :test do
      children
    else
      # Bot-specific workers and pollers go here (GenServers that do async work)
      # Examples: Scheduler, Poller, Watcher
      # Pattern: gated with if @env == :test to prevent long-running processes in test
      children
    end
  end
end
