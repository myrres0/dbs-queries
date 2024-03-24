import psycopg2
from fastapi import APIRouter, Query

from dbs_assignment.config import settings

router = APIRouter()



def get_post_limit(post_id, limit):
    connection = psycopg2.connect(database=settings.DATABASE_NAME, user=settings.DATABASE_USER,
                                  password=settings.DATABASE_PASSWORD,
                                  host=settings.DATABASE_HOST, port=settings.DATABASE_PORT)

    cursor = connection.cursor()
    cursor.execute("""
       --SET TIMEZONE = 'UTC';
        SELECT users.displayname, posts.body, TO_CHAR(posts.creationdate, 'YYYY-MM-DD"T"HH24:MI:SS.MS"+00"') as created_at FROM posts
        JOIN users ON posts.owneruserid = users.id
        WHERE posts.id = %s
        UNION
        SELECT users.displayname, posts.body, TO_CHAR(posts.creationdate, 'YYYY-MM-DD"T"HH24:MI:SS.MS"+00"') as created_at FROM posts
        JOIN users ON posts.owneruserid = users.id
        WHERE posts.parentid = %s
        ORDER BY created_at
        LIMIT %s;
    """, [post_id, post_id, limit])

    data = cursor.fetchall() # data from the query
    column_names = [desc[0] for desc in cursor.description] # extract column names
    post_limit = [{column_name: value for column_name, value in zip(column_names, row)} for row in data]
    cursor.close()
    connection.close()
    return post_limit



@router.get("/v3/posts/{post_id}")
async def postusers(post_id: int,
                    limit: int = Query(None, description="Limit the number of items returned")):
    result = get_post_limit(post_id, limit)
    return {
        "items": result
    }
