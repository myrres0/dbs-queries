import psycopg2
from fastapi import APIRouter
from datetime import datetime

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
    post_users = change_datetime_format(post_users)
    return post_users


# function to change datetime format to ”2023-12-01T00:05:24.3+00:00”,
def change_datetime_format(post_users):
    for user in post_users:
        user['creationdate'] = user['creationdate'].strftime("%Y-%m-%dT%H:%M:%S.%f%z")[:-10]+"+00:00"
        user['lastaccessdate'] = user['lastaccessdate'].strftime("%Y-%m-%dT%H:%M:%S.%f%z")[:-8]+"+00:00"
    return post_users


@router.get("/v2/posts/{post_id}/users")
async def postusers(post_id: int):
    post_users = get_post_users(post_id)
    return {
        "items": post_users
    }
