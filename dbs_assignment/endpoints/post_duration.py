import psycopg2
from fastapi import APIRouter, Query

from dbs_assignment.config import settings

router = APIRouter()

def get_post_duration(duration, limit):
    connection = psycopg2.connect(database=settings.DATABASE_NAME, user=settings.DATABASE_USER,
                                  password=settings.DATABASE_PASSWORD,
                                  host=settings.DATABASE_HOST, port=settings.DATABASE_PORT)

    cursor = connection.cursor()
    cursor.execute("""
    SELECT posts.id, posts.creationdate, posts.viewcount, posts.lasteditdate,
       posts.lastactivitydate, posts.title, posts.closeddate,
       ROUND(EXTRACT(EPOCH FROM closeddate - creationdate) / 60, 2) AS duration
    FROM posts
    WHERE closeddate IS NOT NULL
    AND EXTRACT(EPOCH FROM closeddate - creationdate) / 60 <= %s
    ORDER BY closeddate DESC
    LIMIT %s;
    """, [duration, limit])

    post_duration_values = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    post_duration = [{column_name: value for column_name, value in zip(column_names, row)} for row in post_duration_values]
    cursor.close()
    connection.close()
    return post_duration


@router.get("/v2/posts/")
async def postduration(duration: int = Query(None, description="duration"),
                        limit: int = Query(None, description="limit")):
    if duration is not None and limit is not None:
        post_duration = get_post_duration(duration, limit)
        return post_duration
