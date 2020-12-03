rsync -av -e ssh --exclude='*.env' --exclude='data' --exclude='examples' . ccs1:google_political_transparency_report
