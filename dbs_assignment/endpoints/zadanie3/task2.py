import psycopg2
from fastapi import APIRouter, Query

from dbs_assignment.config import settings

router = APIRouter()



def get_task2(tag, count):
    connection = psycopg2.connect(database=settings.DATABASE_NAME, user=settings.DATABASE_USER,
                                  password=settings.DATABASE_PASSWORD,
                                  host=settings.DATABASE_HOST, port=settings.DATABASE_PORT)

    cursor = connection.cursor()
    cursor.execute("""
        SELECT *,CASE
        WHEN EXTRACT(DAY FROM comments_with_time_difference.diff) > 0 THEN
            EXTRACT(DAY FROM comments_with_time_difference.diff) || ' days ' ||
            TO_CHAR(comments_with_time_difference.diff, 'HH24:MI:SS.MS')
        ELSE
            TO_CHAR(comments_with_time_difference.diff, 'HH24:MI:SS.MS')
        END AS diff,  TO_CHAR(AVG(comments_with_time_difference.diff) OVER
        (PARTITION BY post_id ORDER BY comments_with_time_difference.created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ),'HH24:MI:SS.US') AS avg FROM (
            SELECT *,
            CASE
            WHEN LAG(comments_sorted.created_at) OVER (PARTITION BY post_id ORDER BY comments_sorted.created_at) IS NULL THEN
            (comments_sorted.created_at - post_created_at)
            ELSE
            (comments_sorted.created_at - LAG(comments_sorted.created_at)
            OVER (PARTITION BY post_id ORDER BY comments_sorted.created_at))
            END AS diff
            FROM (
                SELECT posts_sorted.id as post_id, posts_sorted.title, users.displayname,
                comments.text, posts_sorted.creationdate AS post_created_at, comments.creationdate as created_at
                 FROM (
                    SELECT DISTINCT posts.id,posts.title,posts.creationdate, COUNT(comments.id) as comments_count
                     FROM(
                        SELECT posts.* FROM posts
                        JOIN post_tags ON posts.id = post_tags.post_id
                        JOIN tags ON post_tags.tag_id = tags.id
                        WHERE tags.tagname = %s)
                        AS posts
                        JOIN comments ON posts.id = comments.postid
                        GROUP BY posts.id, posts.title, posts.creationdate
                        HAVING COUNT(comments.id) > %s) AS posts_sorted
                JOIN comments ON posts_sorted.id = comments.postid
                LEFT OUTER JOIN public.users on users.id = comments.userid
                ORDER BY comments.creationdate) AS comments_sorted
            ORDER BY comments_sorted.post_id, comments_sorted.created_at) AS comments_with_time_difference
        ORDER BY post_id, comments_with_time_difference.created_at;
    """, [tag, count])

    data = cursor.fetchall() # data from the query
    column_names = [desc[0] for desc in cursor.description] # extract column names
    result_dict = [{column_name: value for column_name, value in zip(column_names, row)} for row in data]
    cursor.close()
    connection.close()
    return result_dict



@router.get("/v3/tags/{tag}/comments")
async def task2(tag: str, count: int = Query(None, description="count")):
    result = get_task2(tag, count)
    return {
        "items": result
    }
