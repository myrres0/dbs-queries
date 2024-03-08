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
    SELECT posts.id, posts.creationdate, posts.viewcount, posts.lasteditdate,
       posts.lastactivitydate, posts.title, posts.body, posts.answercount,
       posts.closeddate, array_agg(tags.tagname) AS tags FROM (
    SELECT posts.* FROM posts
    WHERE posts.title ILIKE %s OR posts.body ILIKE %s
    ORDER BY posts.creationdate DESC
    LIMIT %s) AS posts
    JOIN post_tags ON posts.id = post_tags.post_id
    JOIN tags ON post_tags.tag_id = tags.id
    group by posts.id, posts.creationdate, posts.viewcount, posts.lasteditdate,
             posts.lastactivitydate, posts.title, posts.body, posts.answercount,
             posts.closeddate;
    """, ('%' + query + '%', '%' + query + '%', limit))

    post_query_values = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    post_query = [{column_name: value for column_name, value in zip(column_names, row)} for row in post_query_values]
    cursor.close()
    connection.close()
    return post_query


@router.get("/v2/posts")
async def postquery(limit: int = Query(None, description="limit"),
                        query: str = Query(None, description="query")):
    if limit is not None and query is not None:
        post_query = get_post_query(limit, query)
        return post_query
