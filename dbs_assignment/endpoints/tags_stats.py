import psycopg2
from fastapi import APIRouter

from dbs_assignment.config import settings

router = APIRouter()


def get_tags_stats(tagname):
    connection = psycopg2.connect(database=settings.DATABASE_NAME, user=settings.DATABASE_USER,
                                  password=settings.DATABASE_PASSWORD,
                                  host=settings.DATABASE_HOST, port=settings.DATABASE_PORT)

    cursor = connection.cursor()
    cursor.execute("""
    SELECT ROUND((COUNT(CASE WHEN tags.tagname = %s THEN 1 ELSE NULL END) * 100.0)
                 / COUNT(DISTINCT posts.id), 2) AS percentage
    FROM posts
    JOIN post_tags ON posts.id = post_tags.post_id
    JOIN tags ON post_tags.tag_id = tags.id
    GROUP BY TO_CHAR(creationdate, 'Day')
    ORDER BY CASE WHEN EXTRACT(DOW FROM MIN(creationdate)) = 0 THEN 7 ELSE EXTRACT(DOW FROM MIN(creationdate)) END;
    """, [tagname])
    tags_stats = cursor.fetchall()
    tags_stats = [row[0] for row in tags_stats] # convert list of tuples to list of integers
    cursor.close()
    connection.close()
    return tags_stats


@router.get("/v2/tags/{tagname}/stats")
async def tagsstats(tagname: str):
    tags_stats = get_tags_stats(tagname)
    return {
        'result': {
            'monday': tags_stats[0],
            'tuesday': tags_stats[1],
            'wednesday': tags_stats[2],
            'thursday': tags_stats[3],
            'friday': tags_stats[4],
            'saturday': tags_stats[5],
            'sunday': tags_stats[6]
        }
    }

