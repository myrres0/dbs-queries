import psycopg2
from fastapi import APIRouter

from dbs_assignment.config import settings

router = APIRouter()


def get_post_users(post_id):
    connection = psycopg2.connect(database=settings.DATABASE_NAME, user=settings.DATABASE_USER,
                                  password=settings.DATABASE_PASSWORD,
                                  host=settings.DATABASE_HOST, port=settings.DATABASE_PORT)

    cursor = connection.cursor()

    cursor.execute(" SET TIMEZONE='UTC'; ")
    cursor.execute("""
    SELECT users.id, reputation, TO_CHAR(users.creationdate, 'YYYY-MM-DD"T"HH24:MI:SS.MS"+00:00"') AS creationdate,
       displayname, lastaccessdate,TO_CHAR(users.lastaccessdate, 'YYYY-MM-DD"T"HH24:MI:SS.MS"+00:00"') AS lastaccessdate,
       location, aboutme, views, upvotes, downvotes, profileimageurl, age, accountid, websiteurl FROM comments
    JOIN users ON comments.userid = users.id
    WHERE comments.postid = %s
    ORDER BY comments.creationdate DESC;
    """, [post_id])

    post_users_data = cursor.fetchall() # data from the query
    column_names = [desc[0] for desc in cursor.description] # extract column names
    # 1. zip column names and data,
    # 2. convert list of tuples to list of dictionaries
    post_users = [{column_name: value for column_name, value in zip(column_names, row)} for row in post_users_data]

    cursor.close()
    connection.close()
    return post_users


@router.get("/v2/posts/{post_id}/users")
async def postusers(post_id: int):
    post_users = get_post_users(post_id)
    return {
        "items": post_users
    }
