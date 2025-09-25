from invoke.collection import Collection
from invoke.tasks import task


@task
def test_all(ctx):
    """Run all tests."""
    ctx.run("pytest tests/")


test_ns = Collection("test", test_all=test_all)
