from fastapi import APIRouter

from dbs_assignment.endpoints import post_users, user_friends, tags_stats, post_duration, post_query

router = APIRouter()
router.include_router(post_users.router)
router.include_router(user_friends.router)
router.include_router(tags_stats.router)
router.include_router(post_duration.router)
router.include_router(post_query.router)
