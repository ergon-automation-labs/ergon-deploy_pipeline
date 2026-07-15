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
      {:ok, bot_metadata} = lookup_bot_metadata(bot_short)
      bot_type = Map.get(bot_metadata, :bot_type, :v1)
      deploy_via_handler(bot_type, bot_short, repo_slug, release_tag, version, ctx)
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
    Logger.info(
      "[Deploy] v1 skill: routing to Deploy.deploy_v1 for #{bot_short} (ctx.bot_id=#{ctx.bot_id})"
    )

    {:ok, result} =
      BotArmyDeployPipeline.Deploy.deploy_v1(bot_short, repo_slug, release_tag, version)

    Logger.info("[Deploy] v1 succeeded: #{inspect(result)}")
    {:ok, Map.put(result, :version, version)}
  end

  # ============================================================================
  # v2 Handler: docker-compose deployment (scaffolded for bot_army_v2)
  # ============================================================================

  defp deploy_v2(bot_short, repo_slug, release_tag, version, ctx) do
    Logger.info(
      "[Deploy] v2 skill: routing to Deploy.deploy_v2 for #{bot_short} (ctx.bot_id=#{ctx.bot_id})"
    )

    {:ok, result} =
      BotArmyDeployPipeline.Deploy.deploy_v2(bot_short, repo_slug, release_tag, version)

    Logger.info("[Deploy] v2 succeeded: #{inspect(result)}")
    {:ok, Map.put(result, :version, version)}
  end
end
