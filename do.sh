export NOW=$(date +'%s')
virtualenv -p /usr/bin/python3 venv-$NOW
source  venv-$NOW/bin/activate
pip install -r google_political_transparency_report/political_transparency_report_site/requirements.txt
pip install -r google_political_transparency_report/transparency_bundle/requirements.txt
pip install -r google_political_transparency_report/youtube_dot_com/requirements.txt
python3 -m google_political_transparency_report.transparency_bundle.daily
python3 -m google_political_transparency_report.political_transparency_report_site.scrape_political_transparency_report
python3 -m google_political_transparency_report.youtube_dot_com.get_ad_video_info_from_youtube
deactivate
rm -r  venv-$NOW