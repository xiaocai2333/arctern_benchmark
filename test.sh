rm gen_html/version_build_time.txt
rm -rf output/*
python3 main.py -w True -f test_suites/gis_only/gis_test.txt -t 3 -s -p -c True
