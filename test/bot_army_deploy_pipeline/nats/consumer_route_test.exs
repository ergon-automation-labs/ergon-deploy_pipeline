defmodule BotArmyDeployPipeline.NATS.ConsumerRouteTest do
  use ExUnit.Case

  @moduletag :nats

  alias BotArmyDeployPipeline.NATS.Consumer

  # Regression guard for the dispatch bug that made every `make publish-release`
  # deploy request a no-op: the consumer used to route by `reply_to` alone, so a
  # request-style deploy request (which is what the bot-side publish-release
  # target sends with `nats request`) landed in handle_request_reply/2 — a
  # handler that only knows deploy.job.status and deploy.release.status. The
  # request was dropped at debug level and the producer got an empty ack.

  describe "route_kind/2" do
    test "deploy requests are dispatched even when the producer used request/reply" do
      assert Consumer.route_kind("deploy.release.requested.air", "_INBOX.abc123") == :deploy
      assert Consumer.route_kind("deploy.release.requested.mini", "_INBOX.abc123") == :deploy
      assert Consumer.route_kind("deploy.release.requested", "_INBOX.abc123") == :deploy
    end

    test "deploy requests published without a reply inbox are still dispatched" do
      assert Consumer.route_kind("deploy.release.requested", nil) == :deploy
      assert Consumer.route_kind("deploy.release.requested.air", nil) == :deploy
      assert Consumer.route_kind("deploy.release.requested.air", "") == :deploy
    end

    test "status subjects stay on the request/reply path" do
      assert Consumer.route_kind("deploy.job.status", "_INBOX.abc123") == :request_reply
      assert Consumer.route_kind("deploy.release.status", "_INBOX.abc123") == :request_reply
    end

    test "other subjects fall back to pub/sub" do
      assert Consumer.route_kind("events.fitness.workout.logged", nil) == :pub_sub
      assert Consumer.route_kind("deploy.job.status", nil) == :pub_sub
      assert Consumer.route_kind("ops.deploy.complete", nil) == :pub_sub
    end

    test "a subject that merely mentions a deploy request is not one" do
      assert Consumer.route_kind("events.deploy.release.requested", nil) == :pub_sub
      assert Consumer.route_kind("deploy.release.requestedness", nil) == :pub_sub
    end
  end
end
