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

  alias BotArmyDeployPipeline.DeployLedger

  # ============================================================================
  # v1 Handler: Salt/launchd deployment
  # ============================================================================

  @doc """
  Deploy a v1 bot via Salt state.apply.

  Reads pillar to determine which node(s) run this bot, then invokes
  deploy_bot_with_summary.sh for each node.
  """
  def deploy_v1(bot_short, repo_slug, release_tag, version, target \\ nil) do
    Logger.info(
      "[Deploy.v1] Starting deployment: bot=#{bot_short} repo=#{repo_slug} tag=#{release_tag} v=#{version} target=#{inspect(target)}"
    )

    try do
      nodes =
        case normalize_target(target) do
          {:ok, nodes} -> {:ok, nodes}
          # No explicit target in the event — fall back to scaffolded node discovery
          :error -> determine_target_nodes(bot_short)
        end

      case nodes do
        {:ok, [_ | _] = nodes} ->
          Logger.info("[Deploy.v1] Target nodes: #{inspect(nodes)}")
          deploy_to_nodes(bot_short, nodes, release_tag, version)

        {:error, reason} ->
          Logger.error("[Deploy.v1] Invalid target #{inspect(target)}: #{inspect(reason)}")
          {:error, :invalid_target}
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

  # Event target: "air" | "mini" (string, from deploy.release.requested).
  # nil → no explicit target, caller falls back to scaffolded node discovery.
  # Nodes are strings throughout so they pass safely into System.cmd args.
  defp normalize_target(nil), do: :error
  defp normalize_target(:air), do: {:ok, ["air"]}
  defp normalize_target(:mini), do: {:ok, ["mini"]}
  defp normalize_target(t) when is_binary(t) and t in ["air", "mini"], do: {:ok, [t]}
  defp normalize_target(_), do: {:error, :unknown_node}

  defp determine_target_nodes(_bot_short) do
    # Scaffolded for Phase 1: will read pillar to determine which nodes run this bot.
    # Pillar entries like air.sls and mini.sls list enabled_repositories, which the actual
    # implementation will query to return ["air"] or ["mini"] or ["air", "mini"] as appropriate.
    # For now, default to ["air"]; Phase 1 rollout will integrate pillar lookup.
    {:ok, ["air"]}
  end

  defp deploy_to_nodes(bot_short, nodes, release_tag, version) do
    results =
      nodes
      |> Enum.map(fn node ->
        case deploy_to_node(bot_short, node, release_tag, version) do
          {:ok, verification} -> {:ok, node, verification}
          {:error, reason} -> {:error, node, reason}
        end
      end)

    errors = Enum.filter(results, fn r -> match?({:error, _, _}, r) end)

    if Enum.empty?(errors) do
      verification = Map.new(results, fn {:ok, node, v} -> {node, v} end)

      Logger.info("[Deploy.v1] Successfully deployed to all nodes: #{inspect(nodes)}")
      {:ok, %{bot: bot_short, nodes: nodes, status: :success, handler: :v1_salt_launchd, verification: verification}}
    else
      Logger.error("[Deploy.v1] Deployment failed on some nodes: #{inspect(errors)}")
      {:error, {:deployment_failed, errors}}
    end
  end

  defp deploy_to_node(bot_short, node, release_tag, version) do
    Logger.info(
      "[Deploy.v1] Deploying to #{node}: #{bot_short} (tag=#{release_tag}, v=#{version})"
    )

    script_path = Path.join(infra_scripts_dir(), "deploy_bot_with_summary.sh")

    case File.exists?(script_path) do
      true ->
        invoke_deploy_script(script_path, bot_short, node, release_tag, version)

      false ->
        Logger.error("[Deploy.v1] Script not found: #{script_path}")
        {:error, :script_not_found}
    end
  end

  defp invoke_deploy_script(script_path, bot_short, node, release_tag, version) do
    Logger.info("[Deploy.v1] Invoking: #{script_path} #{bot_short} #{node}")

    # Run the deploy script as abby (the deploy user with ssh keys + NOPASSWD
    # sudo for salt/launchctl). The script is executable with a #!/bin/bash
    # shebang, so it is invoked directly — no /bin/bash prefix. From bot_army
    # (launchd) this rides the scoped sudoers rule in salt/air/users.sls; from
    # an interactive abby shell it is a no-op (self sudo).
    #
    # Ledger: record the dispatch BEFORE the call. When a bot redeploys the
    # pipeline ITSELF, the launchctl kickstart in the script kills this beam
    # mid-call — the pending record is what lets the next boot reconcile the
    # deploy and publish the late ops.deploy.complete.
    DeployLedger.record_dispatched(bot_short, node, version, release_tag)

    case System.cmd("sudo", ["-n", "-u", "abby", script_path, bot_short, node],
           stderr_to_stdout: true
         ) do
      {output, 0} ->
        Logger.debug("[Deploy.v1] Output:\n#{output}")

        # Exit 0 is necessary but not sufficient: confirm the target node
        # actually RUNS the deployed version (the script verified this and
        # wrote a result record).
        case read_verification(bot_short, node, version) do
          {:ok, "true", detail} ->
            Logger.info("[Deploy.v1] Verified on #{node}: beam runs v#{version} (#{detail})")
            DeployLedger.record_finished(bot_short, node, version, "completed")
            {:ok, true}

          {:ok, "unknown", detail} ->
            Logger.warning(
              "[Deploy.v1] Could not verify beam version on #{node} (#{detail}) — accepting exit 0"
            )

            DeployLedger.record_finished(bot_short, node, version, "completed_unverified")
            {:ok, :unverified}

          {:error, :no_record} ->
            Logger.warning(
              "[Deploy.v1] No verification record for #{bot_short}/#{node} v#{version} — accepting exit 0"
            )

            DeployLedger.record_finished(bot_short, node, version, "completed_unverified")
            {:ok, :unverified}

          {:ok, "false", detail} ->
            Logger.error("[Deploy.v1] VERIFICATION FAILED on #{node}: #{detail}")
            DeployLedger.record_finished(bot_short, node, version, "verification_failed")
            {:error, {:verification_failed, detail}}
        end

      {output, exit_code} ->
        Logger.error("[Deploy.v1] Deployment failed on #{node} (exit code: #{exit_code})")

        Logger.error("[Deploy.v1] Output:\n#{output}")
        DeployLedger.record_finished(bot_short, node, version, "failed")
        {:error, :deployment_failed}
    end
  end

  # Read the deploy script's ground-truth verification record (written after
  # the script confirmed the target beam runs the deployed version). Returns
  # {:ok, verified, detail} with verified in ["true", "false", "unknown"], or
  # {:error, :no_record} when the script predates verification or crashed.
  defp read_verification(bot_short, node, version) do
    path = Path.join(infra_scripts_dir(), ".deploy_results.jsonl")

    case File.read(path) do
      {:ok, content} ->
        records =
          content
          |> String.split("\n", trim: true)
          |> Enum.map(&Jason.decode/1)
          |> Enum.filter(&match?({:ok, %{}}, &1))
          |> Enum.map(&elem(&1, 1))
          |> Enum.filter(fn rec ->
            rec["bot"] == bot_short and rec["node"] == node and rec["version"] == version
          end)

        case records do
          [] ->
            {:error, :no_record}

          _ ->
            latest = Enum.max_by(records, &(&1["ts"] || ""))
            {:ok, latest["verified"], latest["detail"]}
        end

      {:error, reason} ->
        Logger.warning("[Deploy.v1] Cannot read verification records (#{inspect(reason)})")
        {:error, :no_record}
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
