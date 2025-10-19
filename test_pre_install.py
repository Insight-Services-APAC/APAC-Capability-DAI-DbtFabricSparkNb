import sys
import dbt_wrapper.main as main

# Only import dbt adapter when actually needed, not for help
if '--help' not in sys.argv:
    import dbt.adapters.fabricsparknb  # noqa: F401

main = main.app()

# python ./test_pre_install.py run-all ./samples_tests --no-clean-target-dir --no-generate-pre-dbt-scripts --no-download-metadata --no-auto-execute-metadata-extract --no-upload-notebooks-via-api --no-auto-run-master-notebook --pre-install