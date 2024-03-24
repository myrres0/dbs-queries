import psycopg2
from fastapi import APIRouter, Query

from dbs_assignment.config import settings

router = APIRouter()



def get_task1(user_id):
    connection = psycopg2.connect(database=settings.DATABASE_NAME, user=settings.DATABASE_USER,
                                  password=settings.DATABASE_PASSWORD,
                                  host=settings.DATABASE_HOST, port=settings.DATABASE_PORT)

    cursor = connection.cursor()
    cursor.execute("""
        SELECT *, ROW_NUMBER() OVER() AS position
        FROM (
            SELECT DISTINCT ON (post_id) post_badge.*,'badge' AS b_type, posts.title, 'post' AS p_type,
            TO_CHAR(posts.creationdate, 'YYYY-MM-DD"T"HH24:MI:SS.MS"+00"') AS creationdate
            FROM (SELECT badges.id,badges.name,TO_CHAR(badges.date , 'YYYY-MM-DD"T"HH24:MI:SS.MS"+00"') AS date,
             max(CASE WHEN posts.creationdate < badges.date THEN posts.id END) AS post_id
                FROM badges
                JOIN posts ON posts.owneruserid = badges.userid
                WHERE badges.userid = %s
                GROUP BY badges.id, badges.date
                ORDER BY badges.date, badges.id) AS post_badge
            JOIN posts ON post_badge.post_id = posts.id
        ) AS post_badge;
    """, [user_id])

    data = cursor.fetchall() # data from the query
    column_names = [desc[0] for desc in cursor.description] # extract column names
    posts_badges = [{column_name: value for column_name, value in zip(column_names, row)} for row in data]

    # to new dictionary
    result = []

    for badge in posts_badges:
        result.append(
            {
                "id": badge["post_id"],
                "title": badge["title"],
                "type": badge["p_type"],
                "created_at": badge["creationdate"],
                "position": badge["position"]
            }
        )
        result.append({
            "id": badge["id"],
            "title": badge["name"],
            "type": badge["b_type"],
            "created_at": badge["date"],
            "position": badge["position"]
        })

    cursor.close()
    connection.close()
    return result

@router.get("/v3/users/{user_id}/badge_history")
async def task1(user_id: int):
    result = get_task1(user_id)
    return {
        "items": result
    }
