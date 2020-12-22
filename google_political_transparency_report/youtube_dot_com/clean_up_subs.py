"""helper script to remove duplicate lines from subtitle files"""


from dotenv import load_dotenv

load_dotenv()
import records

DB = records.Database()


for row in DB.query("SELECT * FROM youtube_videos WHERE subs is not null;"):
    subtitle_lines = [l for l in row["subs"].split("\n") if l.strip() != ''] 
    if len(subtitle_lines) == 0:
        continue
    subtitle_lines_deduped = [subtitle_lines[0]]
    for line_a, line_b in zip(subtitle_lines[:-1], subtitle_lines[1:]):
        if line_a not in line_b:
            subtitle_lines_deduped.append(line_b)
    subs = '\n'.join(subtitle_lines_deduped)
    DB.query("UPDATE youtube_videos SET subs = :subs  WHERE id = :id", id=row["id"], subs=subs)