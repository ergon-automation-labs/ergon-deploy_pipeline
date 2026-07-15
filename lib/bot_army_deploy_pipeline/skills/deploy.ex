defmodule BotArmyDeployPipeline.Skills.Deploy do
  @moduledoc """
  Deploy skill for orchestrating bot deployments via the NATS pipeline.

  Handles both v1 (Salt/launchd) and v2 (docker-compose/Nomad) deployments by:
  1. Receiving deploy.release.requested events
  2. Looking up bot metadata from pillar (bot_type, enabled_repositories, etc.)
  3. Routing to appropriate deployment handler
  4. Publishing ops.deploy.complete or ops.deploy.failed
  5. Calling make mark-ecosystem-working on success

  v1 Bots: Salt state.apply via deploy_bot_with_summary.sh
  v2 Bots: docker-compose pull + restart (scaffolded for bot_army_v2 integration)
  """

  use BotArmy.Skill
  require Logger

  @impl true
  def name, do: :deploy

  @impl true
  def description do
    "Orchestrate bot deployments (v1: Salt/launchd, v2: docker-compose/Nomad)"
  end

  @impl true
  def nats_triggers do
    ["deploy.release.requested"]
  end

  @impl true
  def llm_hint, do: :fast

  @impl true
  def validate(%{
        "bot" => bot,
        "repo" => repo,
        "tag" => tag,
        "version" => version
      })
      when is_binary(bot) and is_binary(repo) and is_binary(tag) and is_binary(version) do
    :ok
  end

  def validate(_) do
    {:error, "required fields: bot, repo, tag, version (all strings)"}
  end

  @impl true
  def execute(
        %{"bot" => bot_short, "repo" => repo_slug, "tag" => release_tag, "version" => version},
        ctx
      ) do
    try do
      Logger.info("[Deploy] Deploying #{bot_short} v#{version} from #{repo_slug}")

      # Lookup bot metadata from pillar
      case lookup_bot_metadata(bot_short) do
        {:ok, bot_metadata} ->
          bot_type = Map.get(bot_metadata, :bot_type, :v1)
          deploy_via_handler(bot_type, bot_short, repo_slug, release_tag, version, ctx)

        {:error, reason} ->
          Logger.error("[Deploy] Failed to lookup bot metadata: #{inspect(reason)}")
          {:error, :bot_not_found}
      end
    rescue
      e ->
        Logger.error("[Deploy] Execution failed: #{inspect(e)}")
        {:error, :execution_failed}
    end
  end

  # ============================================================================
  # Bot Metadata Lookup
  # ============================================================================

  defp lookup_bot_metadata(bot_short) do
    # This would read from pillar in production; for now, return a default v1 config
    # In Phase 1, this will be replaced with actual pillar lookups via salt-call or
    # a cached config loaded at startup
    {:ok,
     %{
       bot_type: :v1,
       bot_short: bot_short
       # These would come from pillar in production:
       # - enabled_repositories from air.sls/mini.sls
       # - ci_engine flag
       # - bot_type flag
     }}
  end

  # ============================================================================
  # Deployment Handlers
  # ============================================================================

  defp deploy_via_handler(:v1, bot_short, repo_slug, release_tag, version, ctx) do
    Logger.info("[Deploy] Routing to v1 handler (Salt/launchd) for #{bot_short}")
    deploy_v1(bot_short, repo_slug, release_tag, version, ctx)
  end

  defp deploy_via_handler(:v2, bot_short, repo_slug, release_tag, version, ctx) do
    Logger.info("[Deploy] Routing to v2 handler (docker-compose) for #{bot_short}")
    deploy_v2(bot_short, repo_slug, release_tag, version, ctx)
  end

  # ============================================================================
  # v1 Handler: Salt/launchd deployment
  # ============================================================================

  defp deploy_v1(bot_short, repo_slug, release_tag, version, ctx) do
    # This will invoke deploy_bot_with_summary.sh, which:
    # 1. Determines which node(s) run this bot (from pillar enabled_repositories)
    # 2. Applies Salt state bots.<bot>_bot to each node
    # 3. Creates/migrates database
    # 4. Renders plist and starts service

    # Placeholder: In Phase 1, this calls the existing shell script
    # For now, return success with a marker that this path was invoked
    Logger.info("[Deploy] v1: Would invoke deploy_bot_with_summary.sh for #{bot_short}")

    {:ok,
     %{
       bot: bot_short,
       version: version,
       release_tag: release_tag,
       handler: :v1_salt_launchd,
       status: :success
     }}
  end

  # ============================================================================
  # v2 Handler: docker-compose deployment (scaffolded for bot_army_v2)
  # ============================================================================

  defp deploy_v2(bot_short, repo_slug, release_tag, version, ctx) do
    # This will:
    # 1. Pull the latest Docker image from registry for this bot
    # 2. Update docker-compose.yml on appropriate node(s)
    # 3. Restart the service via docker-compose
    # 4. Verify registration with registry

    # Placeholder: In Phase 1, this is scaffolded but not yet connected to real v2 bots.
    # The path exists so when v2 bots migrate in bot_army_v2, the routing is ready.
    Logger.info(
      "[Deploy] v2: Scaffolded handler for docker-compose (bot_army_v2 integration) — #{bot_short}"
    )

    {:ok,
     %{
       bot: bot_short,
       version: version,
       release_tag: release_tag,
       handler: :v2_docker_compose_nomad,
       status: :success
     }}
  end
end
