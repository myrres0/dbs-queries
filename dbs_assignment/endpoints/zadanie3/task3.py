import psycopg2
from fastapi import APIRouter, Query

from dbs_assignment.config import settings

router = APIRouter()



def get_task3(tag, position, limit):
    connection = psycopg2.connect(database=settings.DATABASE_NAME, user=settings.DATABASE_USER,
                                  password=settings.DATABASE_PASSWORD,
                                  host=settings.DATABASE_HOST, port=settings.DATABASE_PORT)

    cursor = connection.cursor()
    cursor.execute("""
        SELECT comments.id, users.displayname, posts.body, comments.text, comments.score, %s as position
        FROM (
            SELECT posts.*, count(comments.id)
            FROM posts
            JOIN post_tags ON posts.id = post_tags.post_id
            JOIN tags ON post_tags.tag_id = tags.id
            JOIN comments ON posts.id = comments.postid
            WHERE tags.tagname = %s
            GROUP BY posts.id, posts.creationdate
            HAVING count(comments.id) > %s
            ORDER BY posts.creationdate
            LIMIT %s
        ) AS posts
        JOIN (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY postid ORDER BY creationdate) as rn
            FROM comments
        ) AS comments ON posts.id = comments.postid AND comments.rn = %s
        JOIN users ON comments.userid = users.id;
    """, [position, tag,position, limit,position])

    data = cursor.fetchall() # data from the query
    column_names = [desc[0] for desc in cursor.description] # extract column names
    task3_result = [{column_name: value for column_name, value in zip(column_names, row)} for row in data]
    cursor.close()
    connection.close()
    return task3_result



@router.get("/v3/tags/{tagname}/comments/{position}")
async def postusers(tagname: str, position: int,
                    limit: int = Query(None, description="Limit the number of items returned")):
    result = get_task3(tagname, position, limit)
    return {
        "items": result
    }
