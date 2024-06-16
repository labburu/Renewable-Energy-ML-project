#!/bin/bash -e
export PYTHONPATH=`pwd`:${PYTHONPATH}
export SPARK_HOME=$VIRTUAL_ENV/lib/python3.5/site-packages/pyspark

pyenv local 3.5.6
pip install -r test/requirements_local.txt

if [ $# -eq 0 ]; then
    pytest -v dags/nba_insight_nba_generation/tests/
    exit 0
fi

for element in $@
do
    case $element in
        "nba")
            pytest -v dags/nba_insight_nba_generation/tests/nba_insight_generate_nbas;;
        "nba-kubra")
            pytest -v dags/nba_insight_nba_generation/tests/nba_insight_generate_nbas/test_nba_insight__kubra.py;;
        "nba-rules")
            pytest -v dags/nba_insight_nba_generation/tests/nba_insight_generate_nbas/test_nba_insight__rules.py;;
        "nba-pa")
            pytest -v dags/nba_insight_nba_generation/tests/nba_insight_generate_nbas/test_nba_insight__payment_agreement.py;;
        "nba-pp")
            pytest -v dags/nba_insight_nba_generation/tests/nba_insight_generate_nbas/test_nba_insight__payment_protection_eligibility.py;;
        "nba-pabb")
            pytest -v dags/nba_insight_nba_generation/tests/nba_insight_generate_nbas/test_nba_insight__payment_agreement_with_budget_billing.py;;
        "nba-pls")
            pytest -v dags/nba_insight_nba_generation/tests/nba_insight_generate_nbas/test_nba_insight__paperless_eligibility.py;;
        "nba-liheap")
            pytest -v dags/nba_insight_nba_generation/tests/nba_insight_generate_nbas/test_nba_insight__liheap_eligibility.py;;
        "nba-lidr")
            pytest -v dags/nba_insight_nba_generation/tests/nba_insight_generate_nbas/test_nba_insight__lidr_eligibility.py;;
        "nba-pa-bp")
            pytest -v dags/nba_insight_nba_generation/tests/nba_insight_generate_nbas/test_nba_insight__payment_agreement_with_budget_plan.py;;
        "nba-hea")
            pytest -v dags/nba_insight_nba_generation/tests/nba_insight_generate_nbas/test_nba_insight__hea_eligibility.py;;
        "nba-amp")
            pytest -v dags/nba_insight_nba_generation/tests/nba_insight_generate_nbas/test_nba_insight__amp_eligibility.py;;
        "nba-gnf")
            pytest -v dags/nba_insight_nba_generation/tests/nba_insight_generate_nbas/test_nba_insight__gnf_eligibility.py;;
        "import-auto")
            pytest -v dags/nba_insight_nba_generation/tests/ngrid_eligibility_data_import_job/auto/test_auto_css_import.py;;
        "import-manual")
            pytest -v dags/nba_insight_nba_generation/tests/ngrid_eligibility_data_import_job/manual/test_manual_css_import.py;;
        "auto-gen")
            pytest -v dags/nba_insight_nba_generation/tests/ngrid_eligibility_data_import_job/auto/test_auto_generate.py;;
        *)
            echo "=== No test found for $element ===" ;;
    esac
done
