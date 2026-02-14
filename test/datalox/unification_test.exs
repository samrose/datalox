defmodule Datalox.UnificationTest do
  use ExUnit.Case, async: true

  alias Datalox.Unification

  describe "variable?/1" do
    test "uppercase atoms are variables" do
      assert Unification.variable?(:X)
      assert Unification.variable?(:Foo)
      assert Unification.variable?(:Person)
    end

    test "lowercase atoms are not variables" do
      refute Unification.variable?(:x)
      refute Unification.variable?(:foo)
      refute Unification.variable?(:parent)
    end

    test "wildcard is not a variable" do
      refute Unification.variable?(:_)
    end

    test "non-atoms are not variables" do
      refute Unification.variable?(42)
      refute Unification.variable?("hello")
      refute Unification.variable?([1, 2])
      refute Unification.variable?(nil)
    end
  end

  describe "substitute/2" do
    test "replaces bound variables with their values" do
      assert Unification.substitute(:X, %{X: "alice"}) == "alice"
    end

    test "unbound variables become :_" do
      assert Unification.substitute(:X, %{}) == :_
    end

    test "constants are unchanged" do
      assert Unification.substitute(:parent, %{X: "alice"}) == :parent
      assert Unification.substitute(42, %{X: "alice"}) == 42
      assert Unification.substitute("hello", %{}) == "hello"
    end

    test "wildcard is unchanged" do
      assert Unification.substitute(:_, %{X: "alice"}) == :_
    end
  end

  describe "unify/3" do
    test "empty lists unify with empty binding" do
      assert Unification.unify([], [], %{}) == {:ok, %{}}
    end

    test "variable binds to value" do
      assert Unification.unify([:X], ["alice"], %{}) == {:ok, %{X: "alice"}}
    end

    test "multiple variables bind correctly" do
      assert Unification.unify([:X, :Y], ["alice", "bob"], %{}) ==
               {:ok, %{X: "alice", Y: "bob"}}
    end

    test "existing binding is preserved if consistent" do
      assert Unification.unify([:X], ["alice"], %{X: "alice"}) == {:ok, %{X: "alice"}}
    end

    test "conflicting binding fails" do
      assert Unification.unify([:X], ["bob"], %{X: "alice"}) == :fail
    end

    test "wildcard matches anything" do
      assert Unification.unify([:_], ["alice"], %{}) == {:ok, %{}}
    end

    test "constant must match exactly" do
      assert Unification.unify([:parent], [:parent], %{}) == {:ok, %{}}
      assert Unification.unify([:parent], [:child], %{}) == :fail
    end

    test "mixed terms and variables" do
      assert Unification.unify([:X, :parent, :Y], ["alice", :parent, "bob"], %{}) ==
               {:ok, %{X: "alice", Y: "bob"}}
    end

    test "mismatched lengths fail" do
      assert Unification.unify([:X, :Y], ["alice"], %{}) == :fail
    end
  end

  describe "unify_one/3" do
    test "wildcard matches anything" do
      assert Unification.unify_one(:_, "anything", %{}) == {:ok, %{}}
    end

    test "non-atom constant matches itself" do
      assert Unification.unify_one(42, 42, %{}) == {:ok, %{}}
      assert Unification.unify_one(42, 43, %{}) == :fail
    end
  end
end
