from invoke.collection import Collection
from invoke.tasks import task

TEST_DIRECTORY = "tests"
PROJECT_DIRECTORY = "src"
PYTHON_DIRECTORIES = f"{PROJECT_DIRECTORY} {TEST_DIRECTORY} tasks alembic"


@task
def bandit(
    ctx,
):
    """
    Check project with bandit.
    """
    ctx.run(f"bandit -r {PYTHON_DIRECTORIES}")


@task
def ruff(ctx, auto_fix=False):
    """
    Check project with ruff.
    """
    check_only_arg = "" if not auto_fix else "--fix --unsafe-fixes"
    ctx.run(f"ruff check {check_only_arg} {PYTHON_DIRECTORIES}")


@task
def pyrefly(ctx):
    """
    Check project with pyrefly.
    """
    ctx.run(f"pyrefly check {PYTHON_DIRECTORIES}")


@task
def format_code(ctx):
    """
    Format code with ruff.
    """
    ctx.run(f"ruff format {PYTHON_DIRECTORIES}")


@task(
    pre=[format_code, ruff, bandit, pyrefly],
    default=True,
)
def check_all(ctx):
    """Run all checkers."""


check_ns = Collection(
    "check",
    bandit=bandit,
    format_code=format_code,
    pyrefly=pyrefly,
    ruff=ruff,
    check_all=check_all,
)
