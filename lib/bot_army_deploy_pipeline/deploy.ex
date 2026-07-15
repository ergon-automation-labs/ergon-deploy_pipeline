defmodule BotArmyDeployPipeline.Deploy do
  @moduledoc """
  Handles `deploy.release.requested` events.

  Resolves node fan-out by checking `enabled_repositories` in
  `bot_army_infra`'s `air.sls`/`mini.sls` for `bots:<bot>`, runs the shared
  `salt_apply_retry.sh` (master-targeted `salt <target> state.apply
  bots.<bot>_bot`, never `salt-call --local`) against every enabled node,
  then publishes `ops.deploy.complete`/`ops.deploy.failed` preserving the
  payload shape Jenkins already produces (`bot`, `repo`, `node`, `status`,
  `version`, `release_tag`) so `bot_army_gtd` and `bot_army_synapse` keep
  working unmodified.
  """

  require Logger

  alias BotArmyRuntime.NATS.Publisher

  @salt_apply_script "/opt/bot_army/scripts/salt_apply_retry.sh"
  @salt_timeout_s "600"

  @doc "Entry point called by the NATS consumer with the decoded deploy.release.requested payload."
  def handle_release_requested(payload) when is_map(payload) do
    bot = payload["bot"]
    repo = payload["repo"]
    tag = payload["tag"]
    version = payload["version"]

    if is_binary(bot) and bot != "" do
      Task.start(fn -> safe_run(bot, repo, tag, version) end)
    else
      Logger.warning("[Deploy] deploy.release.requested missing bot name: #{inspect(payload)}")
    end

    :ok
  end

  # Any crash inside run/4 (script missing, unexpected exception) still has to
  # surface as ops.deploy.failed — otherwise a waiting GTD task or Synapse
  # staleness cache never learns the deploy died.
  defp safe_run(bot, repo, tag, version) do
    run(bot, repo, tag, version)
  rescue
    e ->
      Logger.error("[Deploy] Crash deploying #{bot}: #{Exception.message(e)}")
      publish_failed(bot, repo, "exception: #{Exception.message(e)}")
  end

  defp run(bot, repo, tag, version) do
    case target_nodes(bot) do
      [] ->
        Logger.warning("[Deploy] #{bot} not enabled on any node (air.sls/mini.sls) — skipping")
        publish_failed(bot, repo, "bot not enabled on any node's enabled_repositories")

      nodes ->
        state = salt_state(bot)
        results = Enum.map(nodes, fn node -> {node, apply_state(state, node)} end)
        failed_nodes = for {node, {:error, _}} <- results, do: node

        if failed_nodes == [] do
          Logger.info("[Deploy] #{bot} deployed successfully to #{Enum.join(nodes, ", ")}")
          publish_complete(bot, repo, nodes, tag, version)
          mark_ecosystem_working()
        else
          Logger.error("[Deploy] #{bot} failed on #{Enum.join(failed_nodes, ", ")}")
          publish_failed(bot, repo, "state.apply failed on #{Enum.join(failed_nodes, ", ")}")
        end
    end
  end

  defp target_nodes(bot) do
    [{"air", air_pillar_path()}, {"mini", mini_pillar_path()}]
    |> Enum.filter(fn {_node, path} -> repo_enabled?(path, bot) end)
    |> Enum.map(fn {node, _path} -> node end)
  end

  defp repo_enabled?(pillar_path, bot) do
    case File.read(pillar_path) do
      {:ok, content} ->
        Regex.match?(~r/^\s*-\s*bots:#{Regex.escape(bot)}\s*$/m, content)

      {:error, reason} ->
        Logger.error("[Deploy] Could not read #{pillar_path}: #{inspect(reason)}")
        false
    end
  end

  # Mirrors deploy_bot_with_summary.sh's bot_state resolution.
  defp salt_state("mentra_glass"), do: "surfaces.mentra_glass"
  defp salt_state("trading_" <> _ = bot), do: "bots.#{bot}"
  defp salt_state(bot), do: "bots.#{bot}_bot"

  defp apply_state(state, node) do
    case System.cmd(@salt_apply_script, [state, node, @salt_timeout_s, "", ""],
           stderr_to_stdout: true
         ) do
      {output, 0} ->
        Logger.info("[Deploy] state.apply #{state} -> #{node} succeeded\n#{output}")
        :ok

      {output, code} ->
        Logger.error("[Deploy] state.apply #{state} -> #{node} failed (exit #{code})\n#{output}")
        {:error, output}
    end
  rescue
    e ->
      Logger.error("[Deploy] Failed to invoke #{@salt_apply_script}: #{Exception.message(e)}")
      {:error, Exception.message(e)}
  end

  defp publish_complete(bot, repo, nodes, tag, version) do
    payload = %{
      "bot" => bot,
      "repo" => repo || "",
      "node" => Enum.join(nodes, ","),
      "triggered_by" => "deploy_pipeline_bot",
      "status" => "success",
      "version" => version || "unknown",
      "release_tag" => tag || "unknown"
    }

    case Publisher.publish("ops.deploy.complete", payload) do
      {:ok, _} ->
        Logger.info("[Deploy] Published ops.deploy.complete for #{bot}")

      {:error, reason} ->
        Logger.error("[Deploy] Failed to publish ops.deploy.complete: #{inspect(reason)}")
    end
  end

  defp publish_failed(bot, repo, error) do
    payload = %{
      "bot" => bot,
      "repo" => repo || "",
      "node" => "air",
      "triggered_by" => "deploy_pipeline_bot",
      "status" => "failure",
      "error" => error
    }

    case Publisher.publish("ops.deploy.failed", payload) do
      {:ok, _} ->
        Logger.info("[Deploy] Published ops.deploy.failed for #{bot}")

      {:error, reason} ->
        Logger.error("[Deploy] Failed to publish ops.deploy.failed: #{inspect(reason)}")
    end
  end

  defp mark_ecosystem_working do
    root = monorepo_root()

    case System.cmd("make", ["mark-ecosystem-working"], cd: root, stderr_to_stdout: true) do
      {output, 0} ->
        Logger.info("[Deploy] mark-ecosystem-working succeeded\n#{output}")

      {output, code} ->
        Logger.error("[Deploy] mark-ecosystem-working failed (exit #{code})\n#{output}")
    end
  rescue
    e ->
      Logger.error(
        "[Deploy] Failed to invoke make mark-ecosystem-working: #{Exception.message(e)}"
      )
  end

  defp monorepo_root do
    Application.get_env(:bot_army_deploy_pipeline, :monorepo_root, "/Users/abby/code/elixir_bots")
  end

  defp bot_army_infra_root do
    Application.get_env(
      :bot_army_deploy_pipeline,
      :bot_army_infra_root,
      "/Users/abby/code/bots/bot_army_infra"
    )
  end

  defp air_pillar_path, do: Path.join([bot_army_infra_root(), "pillar", "air.sls"])
  defp mini_pillar_path, do: Path.join([bot_army_infra_root(), "pillar", "mini.sls"])
end
