import sys
import dbt_wrapper.main as main

# Only import dbt adapter when actually needed, not for help
if '--help' not in sys.argv:
    import dbt.adapters.fabricsparknb  # noqa: F401

main = main.app()