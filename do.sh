export NOW=$(date +'%s')
virtualenv -p /usr/bin/python3 venv-$NOW
source  venv-$NOW/bin/activate
pip install -r political_transparency_report_site/requirements.txt
pip install -r transparency_bundle/requirements.txt
pip install -r youtube_dot_com/requirements.txt
python3 transparency_bundle/daily.py
python3 political_transparency_report_site/scrape_political_transparency_report.py
python3 youtube_dot_com/get_ad_video_info_from_youtube.py
deactivate
rm -r  venv-$NOW