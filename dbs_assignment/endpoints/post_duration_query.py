import psycopg2
from fastapi import APIRouter, Query

from dbs_assignment.config import settings

router = APIRouter()

def get_post_query(limit, query):
    connection = psycopg2.connect(database=settings.DATABASE_NAME, user=settings.DATABASE_USER,
                                  password=settings.DATABASE_PASSWORD,
                                  host=settings.DATABASE_HOST, port=settings.DATABASE_PORT)

    cursor = connection.cursor()
    cursor.execute("""
    SELECT posts.id, TO_CHAR(posts.creationdate, 'YYYY-MM-DD"T"HH24:MI:SS.MS"+00"') AS creationdate, posts.viewcount,
       TO_CHAR(posts.lasteditdate,'YYYY-MM-DD"T"HH24:MI:SS.MS"+00"') AS lasteditdate,
       TO_CHAR(posts.lastactivitydate,'YYYY-MM-DD"T"HH24:MI:SS.MS"+00"') AS lastactivitydate, posts.title, posts.body, posts.answercount,
       TO_CHAR(posts.closeddate,'YYYY-MM-DD"T"HH24:MI:SS.MS"+00"') AS closeddate, array_agg(tags.tagname) AS tags FROM (
    SELECT posts.* FROM posts
    WHERE posts.title ILIKE %s OR posts.body ILIKE %s
    ORDER BY posts.creationdate DESC) AS posts
    JOIN post_tags ON posts.id = post_tags.post_id
    JOIN tags ON post_tags.tag_id = tags.id
    group by posts.id, posts.creationdate, posts.viewcount, posts.lasteditdate,
             posts.lastactivitydate, posts.title, posts.body, posts.answercount,
             posts.closeddate
    order by posts.creationdate DESC
    LIMIT %s;
    """, ('%' + query + '%', '%' + query + '%', limit))

    #old one
    # cursor.execute("""
    # SELECT posts.id, TO_CHAR(posts.creationdate, 'YYYY-MM-DD"T"HH24:MI:SS.MS"+00"') AS creationdate, posts.viewcount,
    #    TO_CHAR(posts.lasteditdate,'YYYY-MM-DD"T"HH24:MI:SS.MS"+00"') AS lasteditdate,
    #    TO_CHAR(posts.lastactivitydate,'YYYY-MM-DD"T"HH24:MI:SS.MS"+00"') AS lastactivitydate, posts.title, posts.body, posts.answercount,
    #    TO_CHAR(posts.closeddate,'YYYY-MM-DD"T"HH24:MI:SS.MS"+00"') AS closeddate, array_agg(tags.tagname) AS tags FROM (
    # SELECT posts.* FROM posts
    # WHERE posts.title ILIKE %s OR posts.body ILIKE %s
    # ORDER BY posts.creationdate DESC) AS posts
    # JOIN post_tags ON posts.id = post_tags.post_id
    # JOIN tags ON post_tags.tag_id = tags.id
    # group by posts.id, posts.creationdate, posts.viewcount, posts.lasteditdate,
    #          posts.lastactivitydate, posts.title, posts.body, posts.answercount,
    #          posts.closeddate
    # order by posts.creationdate DESC
    # LIMIT %s;
    # """, ('%' + query + '%', '%' + query + '%', limit))

    post_query_values = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    post_query = [{column_name: value for column_name, value in zip(column_names, row)} for row in post_query_values]
    cursor.close()
    connection.close()
    return post_query


def get_post_duration(duration, limit):
    connection = psycopg2.connect(database=settings.DATABASE_NAME, user=settings.DATABASE_USER,
                                  password=settings.DATABASE_PASSWORD,
                                  host=settings.DATABASE_HOST, port=settings.DATABASE_PORT)

    cursor = connection.cursor()
    cursor.execute("""
    SELECT posts.id, TO_CHAR(posts.creationdate, 'YYYY-MM-DD"T"HH24:MI:SS.MS"+00"') AS creationdate, posts.viewcount,
       TO_CHAR(posts.lasteditdate,'YYYY-MM-DD"T"HH24:MI:SS.MS"+00"') AS lasteditdate,
       TO_CHAR(posts.lastactivitydate,'YYYY-MM-DD"T"HH24:MI:SS.MS"+00"') AS lastactivitydate, posts.title,
       TO_CHAR(posts.closeddate,'YYYY-MM-DD"T"HH24:MI:SS.MS"+00"') AS closeddate,
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



@router.get("/v2/posts")
async def postquery(duration: int = Query(None, description="duration"),
                    limit: int = Query(None, description="limit"),
                    query: str = Query(None, description="query")):
    if duration is not None and limit is not None:
        post_duration = get_post_duration(duration, limit)
        return {
            "items": post_duration
        }
    elif query is not None and limit is not None:
        post_query = get_post_query(limit, query)
        return {
            "items": post_query
        }

