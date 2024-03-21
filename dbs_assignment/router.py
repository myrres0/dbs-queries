from fastapi import APIRouter

from dbs_assignment.endpoints import post_users, user_friends, tags_stats, post_duration_query
from dbs_assignment.endpoints.zadanie3 import post_limit, task3, task1, task2

router = APIRouter()
router.include_router(post_users.router)
router.include_router(user_friends.router)
router.include_router(tags_stats.router)
router.include_router(post_duration_query.router)
router.include_router(post_limit.router)
router.include_router(task3.router)
router.include_router(task1.router)
router.include_router(task2.router)
