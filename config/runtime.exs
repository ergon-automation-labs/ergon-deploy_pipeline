import Config

# The salt-rendered config file has to be put somewhere before it can be read: the launchd
# wrapper deliberately skips sourcing an Elixir `%{}` file (it is not shell), and
# `ConfigLoader.get/2` reads `:config_data` from this app's env and then `System.get_env/1` —
# so without these two lines every pillar knob in this file is inert and the release quietly
# runs on its compiled defaults. Measured across the fleet 2026-10-24 (gtd 33c845f5): on
# wife_care the file named the 27B for the press and the running VM answered `nil`, so the
# press silently used the 4B. A bot that reads `ConfigLoader` without loading it reads
# nothing. Guarded off `:test` because a test run has no rendered file and no business
# reading the node's credentials.
if config_env() != :test do
  config_data = BotArmyLibraryRuntime.ConfigLoader.load_config()
  Application.put_env(:bot_army_library_runtime, :config_data, config_data)
end

# NATS configuration for bot_army_runtime
#
# Without this file the library runtime falls back to its fail-safe default
# ("localhost", 4223) and the pipeline silently listens on the DEV broker,
# missing prod publishes (same class of bug as sre v0.5.8's 4223 default).
# The plist env sets NATS_HOST/NATS_PORT from pillar (localhost/4222 on air/mini).
if config_env() != :test do
  nats_host = BotArmyLibraryRuntime.ConfigLoader.get("NATS_HOST", "localhost")

  nats_port =
    BotArmyLibraryRuntime.ConfigLoader.get("NATS_PORT", "4223") |> String.to_integer()

  config :bot_army_library_runtime, :nats,
    servers: [{nats_host, nats_port}],
    ping_interval: 30_000,
    max_reconnect_attempts: 10,
    reconnect_delay_ms: 1000
end