import Config

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