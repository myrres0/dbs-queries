import psycopg2
from fastapi import APIRouter
from datetime import datetime
from dbs_assignment.config import settings

router = APIRouter()

def get_user_friends(user_id):
    connection = psycopg2.connect(database=settings.DATABASE_NAME, user=settings.DATABASE_USER,
                                  password=settings.DATABASE_PASSWORD,
                                  host=settings.DATABASE_HOST, port=settings.DATABASE_PORT)

    cursor = connection.cursor()
    cursor.execute("""
    SET TIMEZONE='UTC';
    SELECT DISTINCT users.id, reputation,TO_CHAR(users.creationdate, 'YYYY-MM-DD"T"HH24:MI:SS.MS"+00"') AS creationdate, users.creationdate,
       displayname, TO_CHAR(users.lastaccessdate, 'YYYY-MM-DD"T"HH24:MI:SS.MS"+00"') AS lastaccessdate,
       websiteurl, location, aboutme, views, upvotes, downvotes, profileimageurl, age, accountid FROM users
    LEFT JOIN comments ON comments.userid = users.id
    WHERE comments.postid IN
    (SELECT id FROM posts WHERE owneruserid = %s )
    OR comments.postid IN
    (SELECT postid FROM comments WHERE userid = %s)
    ORDER BY users.creationdate;
    """, [user_id, user_id])

    #without distinct
    # cursor.execute("""
    # SELECT users.id, reputation, TO_CHAR(users.creationdate, 'YYYY-MM-DD"T"HH24:MI:SS.MS"+00"') AS creationdate,
    #    displayname, TO_CHAR(users.lastaccessdate, 'YYYY-MM-DD"T"HH24:MI:SS.MS"+00"') AS lastaccessdate,
    #    websiteurl, location, aboutme, views, upvotes, downvotes, profileimageurl, age, accountid FROM users
    # LEFT JOIN comments ON comments.userid = users.id
    # WHERE comments.postid IN
    # (SELECT id FROM posts WHERE owneruserid = %s )
    # OR comments.postid IN
    # (SELECT postid FROM comments WHERE userid = %s)
    # ORDER BY users.creationdate;
    # """, [user_id, user_id])

    user_friends_values = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    user_friends = [{column_name: value for column_name, value in zip(column_names, row)} for row in user_friends_values]
    cursor.close()
    connection.close()
    return user_friends



@router.get("/v2/users/{user_id}/friends")
async def userfriends(user_id: int):
    user_friends = get_user_friends(user_id)
    return {
        "items": user_friends
    }
