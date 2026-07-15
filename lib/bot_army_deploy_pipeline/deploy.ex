defmodule BotArmyDeployPipeline.Deploy do
  @moduledoc """
  Orchestration logic for deploying bots via v1 (Salt/launchd) or v2 (docker-compose).

  Responsibilities:
  1. Read pillar config to determine bot nodes and metadata
  2. Execute deployment handlers (Shell for v1, docker-compose for v2)
  3. Publish ops.deploy.complete/failed events
  4. Call make mark-ecosystem-working on success
  """

  require Logger

  # ============================================================================
  # v1 Handler: Salt/launchd deployment
  # ============================================================================

  @doc """
  Deploy a v1 bot via Salt state.apply.

  Reads pillar to determine which node(s) run this bot, then invokes
  deploy_bot_with_summary.sh for each node.
  """
  def deploy_v1(bot_short, repo_slug, release_tag, version) do
    Logger.info(
      "[Deploy.v1] Starting deployment: bot=#{bot_short} repo=#{repo_slug} tag=#{release_tag} v#{version}"
    )

    try do
      case determine_target_nodes(bot_short) do
        {:ok, nodes} when is_list(nodes) and length(nodes) > 0 ->
          Logger.info("[Deploy.v1] Target nodes: #{inspect(nodes)}")
          deploy_to_nodes(bot_short, nodes, release_tag, version)

        {:error, reason} ->
          Logger.error("[Deploy.v1] Failed to determine target nodes: #{inspect(reason)}")
          {:error, :node_discovery_failed}

        {:ok, []} ->
          Logger.error("[Deploy.v1] No nodes configured for #{bot_short}")
          {:error, :no_nodes_found}
      end
    rescue
      e ->
        Logger.error("[Deploy.v1] Exception: #{inspect(e)}")
        {:error, :deployment_exception}
    end
  end

  # ============================================================================
  # v2 Handler: docker-compose deployment (scaffolded)
  # ============================================================================

  @doc """
  Deploy a v2 bot via docker-compose (scaffolded for bot_army_v2 integration).

  In Phase 1, this is a placeholder. When v2 bots exist, this will:
  1. Pull the latest Docker image for the bot
  2. Update docker-compose.yml on target nodes
  3. Restart the service
  4. Verify registry registration
  """
  def deploy_v2(bot_short, repo_slug, release_tag, version) do
    Logger.info(
      "[Deploy.v2] Scaffolded deployment (bot_army_v2): bot=#{bot_short} repo=#{repo_slug} tag=#{release_tag} v#{version}"
    )

    {:ok,
     %{
       bot: bot_short,
       repo: repo_slug,
       tag: release_tag,
       nodes: [],
       status: :success,
       handler: :v2_docker_compose
     }}
  end

  # ============================================================================
  # Private: Node Discovery & Deployment
  # ============================================================================

  defp determine_target_nodes(_bot_short) do
    # TODO: In Phase 1, read pillar to determine which nodes run this bot.
    # Pillar entries like air.sls and mini.sls list enabled_repositories.
    # For now, return a placeholder; Phase 1 will query pillar and return [:air] or [:mini] or [:air, :mini].

    # Placeholder implementation for now:
    {:ok, [:air]}
  end

  defp deploy_to_nodes(bot_short, nodes, release_tag, version) do
    results =
      nodes
      |> Enum.map(fn node ->
        case deploy_to_node(bot_short, node, release_tag, version) do
          :ok -> {:ok, node}
          {:error, reason} -> {:error, node, reason}
        end
      end)

    errors = Enum.filter(results, fn r -> match?({:error, _, _}, r) end)

    if Enum.empty?(errors) do
      Logger.info("[Deploy.v1] Successfully deployed to all nodes: #{inspect(nodes)}")
      {:ok, %{bot: bot_short, nodes: nodes, status: :success, handler: :v1_salt_launchd}}
    else
      Logger.error("[Deploy.v1] Deployment failed on some nodes: #{inspect(errors)}")
      {:error, :deployment_failed, errors}
    end
  end

  defp deploy_to_node(bot_short, node, release_tag, version) do
    Logger.info(
      "[Deploy.v1] Deploying to #{node}: #{bot_short} (tag=#{release_tag}, v=#{version})"
    )

    script_path = Path.join(infra_scripts_dir(), "deploy_bot_with_summary.sh")

    case File.exists?(script_path) do
      true ->
        invoke_deploy_script(script_path, bot_short, node)

      false ->
        Logger.error("[Deploy.v1] Script not found: #{script_path}")
        {:error, :script_not_found}
    end
  end

  defp invoke_deploy_script(script_path, bot_short, node) do
    Logger.info("[Deploy.v1] Invoking: #{script_path} #{bot_short} #{node}")

    case System.cmd("bash", [script_path, bot_short, node],
           stderr_to_stdout: true,
           timeout: 600_000
         ) do
      {output, 0} ->
        Logger.info("[Deploy.v1] Deployment succeeded on #{node}")
        Logger.debug("[Deploy.v1] Output:\n#{output}")
        :ok

      {output, exit_code} ->
        Logger.error("[Deploy.v1] Deployment failed on #{node} (exit code: #{exit_code})")

        Logger.error("[Deploy.v1] Output:\n#{output}")
        {:error, :deployment_failed}
    end
  end

  defp infra_scripts_dir do
    # Find the bot_army_infra directory
    # Path: /Users/abby/code/bots/bot_army_deploy_pipeline -> /Users/abby/code/bots -> /Users/abby/code
    # Then look for bot_army_infra/scripts
    case File.cwd() do
      {:ok, cwd} ->
        # Try multiple possible paths
        [
          Path.join(cwd, "bot_army_infra/scripts"),
          "/Users/abby/code/bots/bot_army_infra/scripts",
          "/Users/abby/code/elixir_bots/bot_army_infra/scripts"
        ]
        |> Enum.find("", &File.dir?/1)

      {:error, _} ->
        "/Users/abby/code/elixir_bots/bot_army_infra/scripts"
    end
  end
end
