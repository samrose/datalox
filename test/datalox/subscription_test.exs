defmodule Datalox.SubscriptionTest do
  use ExUnit.Case, async: true

  alias Datalox.Subscription

  describe "subscribe/2 and notify/2" do
    test "notifies subscribers of new facts" do
      {:ok, sub} = Subscription.start_link([])

      test_pid = self()
      Subscription.subscribe(sub, {:user, [:_, :_]}, fn fact ->
        send(test_pid, {:fact_added, fact})
      end)

      Subscription.notify(sub, {:assert, {:user, ["alice", :admin]}})

      assert_receive {:fact_added, {:user, ["alice", :admin]}}, 1000
    end

    test "filters by pattern" do
      {:ok, sub} = Subscription.start_link([])

      test_pid = self()
      Subscription.subscribe(sub, {:user, [:_, :admin]}, fn fact ->
        send(test_pid, {:admin_added, fact})
      end)

      Subscription.notify(sub, {:assert, {:user, ["alice", :admin]}})
      Subscription.notify(sub, {:assert, {:user, ["bob", :viewer]}})

      assert_receive {:admin_added, {:user, ["alice", :admin]}}, 1000
      refute_receive {:admin_added, {:user, ["bob", :viewer]}}, 100
    end
  end
end
