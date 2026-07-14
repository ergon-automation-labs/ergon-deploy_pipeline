Application.ensure_all_started(:mox)

ExUnit.configure(exclude: [:integration, :load, :nats_live])

ExUnit.start()

# Define Mox mocks for external dependencies
Mox.defmock(HTTPClientMock, for: BotArmyDeployPipeline.HTTPClient)

# Try to set up database sandbox, but don't fail if database is unavailable
try do
  Ecto.Adapters.SQL.Sandbox.mode(BotArmyDeployPipeline.Repo, :manual)
rescue
  _ -> :ok
end
