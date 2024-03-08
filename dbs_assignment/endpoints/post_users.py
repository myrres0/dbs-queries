import psycopg2
from fastapi import APIRouter

from dbs_assignment.config import settings

router = APIRouter()


def get_post_users(post_id):
    connection = psycopg2.connect(database=settings.DATABASE_NAME, user=settings.DATABASE_USER,
                                  password=settings.DATABASE_PASSWORD,
                                  host=settings.DATABASE_HOST, port=settings.DATABASE_PORT)

    cursor = connection.cursor()
    cursor.execute("""
    SELECT users.* FROM comments
    JOIN users ON comments.userid = users.id
    WHERE comments.postid = %s
    ORDER BY comments.creationdate DESC;
    """, [post_id])
    post_users_data = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    post_users = [{column_name: value for column_name, value in zip(column_names, row)} for row in post_users_data]
    cursor.close()
    connection.close()
    return post_users


@router.get("/v2/posts/{post_id}/users")
async def postusers(post_id: int):
    post_users = get_post_users(post_id)
    return post_users
