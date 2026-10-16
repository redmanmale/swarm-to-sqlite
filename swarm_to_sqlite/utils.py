import datetime
import time
import requests
import os
import urllib.request
import click
from sqlite_utils.db import AlterError, ForeignKey

API_BASE = "https://api.foursquare.com/v2"
API_VERSION = "20190101"


def save_checkin(checkin, db, photos_path=None, friends_history=False):
    if friends_history:
        checkin = dict(checkin["checkin"])
    # Create copy that we can modify
    checkin = dict(checkin)
    if "venue" in checkin:
        venue = checkin.pop("venue")
        categories = venue.pop("categories")
        venue.update(venue.pop("location"))
        venue.pop("labeledLatLngs", None)
        venue["latitude"] = venue.pop("lat")
        venue["longitude"] = venue.pop("lng")
        v = db["venues"].insert(venue, pk="id", alter=True, replace=True)
        for category in categories:
            cleanup_category(category)
            v.m2m("categories", category, pk="id", alter=True)
        checkin["venue"] = venue["id"]
    else:
        checkin["venue"] = None
    if "createdBy" not in checkin:
        checkin["createdBy"] = None
    if "event" in checkin:
        event = checkin.pop("event")
        categories = event.pop("categories")
        e = db["events"].insert(event, pk="id", alter=True, replace=True)
        for category in categories:
            cleanup_category(category)
            e.m2m("categories", category, pk="id", alter=True)
        checkin["event"] = event["id"]
    else:
        checkin["event"] = None

    if "sticker" in checkin:
        sticker = checkin.pop("sticker")
        sticker_image = sticker.pop("image")
        sticker["image_prefix"] = sticker_image["prefix"]
        sticker["image_sizes"] = sticker_image["sizes"]
        sticker["image_name"] = sticker_image["name"]
        checkin["sticker"] = (
            db["stickers"].insert(sticker, pk="id", alter=True, replace=True).last_pk
        )
    else:
        checkin["sticker"] = None

    checkin["created"] = datetime.datetime.utcfromtimestamp(
        checkin["createdAt"]
    ).isoformat()
    source = checkin["source"] if "source" in checkin else {"name": "unknown"}
    checkin["source"] = db["sources"].lookup(source)
    users_with = checkin.pop("with", None) or []
    users_likes = []
    for group in checkin["likes"]["groups"]:
        users_likes.extend(group["items"])
    del checkin["likes"]
    photos = checkin.pop("photos")["items"]
    posts = (checkin.pop("posts") or {}).get("items") or []
    if checkin.get("createdBy"):
        created_by_user = checkin.pop("createdBy")
        cleanup_user(created_by_user)
        db["users"].insert(created_by_user, pk="id", replace=True, alter=True)
        checkin["createdBy"] = created_by_user["id"]
    if checkin.get("comments"):
        checkin["comments_count"] = checkin.pop("comments")["count"]
    if checkin.get("user"):
        user = checkin.pop("user")
        cleanup_user(user)
        db["users"].insert(user, pk="id", replace=True, alter=True)
        checkin["user"] = user["id"]
    else:
        checkin["user"] = None
    # Actually save the checkin
    checkins_table = db["checkins"].insert(
        checkin,
        pk="id",
        foreign_keys=(("venue", "venues", "id"), ("source", "sources", "id")),
        alter=True,
        replace=True,
    )
    # Save m2m 'with' users and 'likes' users
    for user in users_with:
        cleanup_user(user)
        checkins_table.m2m("users", user, m2m_table="with", pk="id", alter=True)
    for user in users_likes:
        cleanup_user(user)
        checkins_table.m2m("users", user, m2m_table="likes", pk="id", alter=True)
    # Handle photos
    photos_table = db.table("photos", pk="id", foreign_keys=("user", "source"))
    for photo in photos:
        photo["created"] = datetime.datetime.utcfromtimestamp(
            photo["createdAt"]
        ).isoformat()
        source = photo["source"] if "source" in photo else {"name": "unknown"}
        photo["source"] = db["sources"].lookup(source)
        user = photo.pop("user")
        cleanup_user(user)
        db["users"].insert(user, pk="id", replace=True, alter=True)
        photo["checkin_id"] = checkin["id"]
        photo["user"] = user["id"]
        photos_table.insert(photo, replace=True, alter=True)
        if photos_path and not os.path.exists(os.path.join(photos_path, photo["suffix"][1:])):
            urllib.request.urlretrieve(photo["prefix"] + 'original' + photo["suffix"],
                os.path.join(photos_path, photo["suffix"][1:]))
    # Handle posts
    posts_table = db.table("posts", pk="id")
    for post in posts:
        post["created"] = datetime.datetime.utcfromtimestamp(
            post["createdAt"]
        ).isoformat()
        post["post_source"] = (
            db["post_sources"]
            .insert(post.pop("source"), pk="id", replace=True, alter=True)
            .last_pk
        )
        post["checkin"] = checkin["id"]
        posts_table.insert(
            post, foreign_keys=("post_source", "checkin"), replace=True, alter=True
        )


def cleanup_user(user):
    photo = user.pop("photo", None) or {}
    user["photo_prefix"] = photo.get("prefix")
    user["photo_suffix"] = photo.get("suffix")


def cleanup_category(category):
    category["icon_prefix"] = category["icon"]["prefix"]
    category["icon_suffix"] = category["icon"]["suffix"]
    del category["icon"]


def ensure_foreign_keys(db):
    existing = []
    for table in db.tables:
        existing.extend(table.foreign_keys)
    desired = [
        ForeignKey(
            table="checkins", column="createdBy", other_table="users", other_column="id"
        ),
        ForeignKey(
            table="checkins", column="event", other_table="events", other_column="id"
        ),
        ForeignKey(
            table="checkins",
            column="sticker",
            other_table="stickers",
            other_column="id",
        ),
    ]
    for fk in desired:
        if fk not in existing:
            try:
                db[fk.table].add_foreign_key(fk.column, fk.other_table, fk.other_column)
            except AlterError:
                pass


def create_views(db, photos_prefix=None):
    photo_url = "'" + photos_prefix + "'" if photos_prefix else "photos.prefix || 'original'"
    for name, sql in (
        (
            "venue_details",
            """
select
    min(created) as first,
    max(created) as last,
    count(venues.id) as count,
    group_concat(distinct categories.name) as venue_categories,
    venues.*
from venues
    join checkins on checkins.venue = venues.id
    join categories_venues on venues.id = categories_venues.venues_id
    join categories on categories.id = categories_venues.categories_id
group by venues.id
        """,
        ),
        (
            "checkin_details",
            """
select
    checkins.id,
    strftime('%Y-%m-%dT%H:%M:%S', checkins.createdAt, 'unixepoch') as created,
    venues.id as venue_id,
    venues.name as venue_name,
    venues.latitude,
    venues.longitude,
    group_concat(distinct categories.name) as venue_categories,
    checkins.shout,
    checkins.createdBy,
    events.name as event_name,
    group_concat((""" + photo_url + """ || photos.suffix), CHAR(10)) as photo_links,
    users.firstName as user
from checkins
    join venues on checkins.venue = venues.id
    left join events on checkins.event = events.id
    join categories_venues on venues.id = categories_venues.venues_id
    join categories on categories.id = categories_venues.categories_id
    left join photos on checkins.id = photos.checkin_id
    left join users on checkins.user = users.id
group by checkins.id
order by checkins.createdAt desc
        """,
        ),
    ):
        try:
            db.create_view(name, sql, replace=True)
        except Exception:
            pass


def _api_get(token, path, **params):
    params = dict(params)
    params.setdefault("oauth_token", token)
    params.setdefault("v", API_VERSION)
    response = requests.get(API_BASE + path, params=params)
    response.raise_for_status()
    return response.json()


def _friends_items(friends_block):
    """Normalize /users/self/friends payload (flat items or grouped)."""
    if not friends_block:
        return []
    items = friends_block.get("items")
    if items:
        return items
    grouped = []
    for group in friends_block.get("groups") or []:
        grouped.extend(group.get("items") or [])
    return grouped


def fetch_friends(token):
    """Return (friends, estimated_checkin_count) from /users/self/friends."""
    friends = []
    estimated = 0
    offset = 0
    limit = 500
    while True:
        data = _api_get(token, "/users/self/friends", limit=limit, offset=offset)
        friends_block = data.get("response", {}).get("friends") or {}
        items = _friends_items(friends_block)
        if not items:
            break
        for user in items:
            friends.append(user)
            checkins = user.get("checkins") or {}
            if isinstance(checkins, dict) and checkins.get("count"):
                estimated += int(checkins["count"])
        count = friends_block.get("count")
        offset += len(items)
        if count is not None and offset >= count:
            break
        if len(items) < limit:
            break
    return friends, estimated


def fetch_user_activities(token, user_id, since_delta=None):
    """Yield activity items that contain a checkin for the given user."""
    params = {
        "limit": 100,
        "attachmentsLimit": 5,
    }
    if since_delta:
        params["afterTimestamp"] = int(time.time() - since_delta)
    before_marker = None
    while True:
        page_params = dict(params)
        if before_marker is not None:
            page_params["beforeMarker"] = before_marker
        data = _api_get(token, "/users/{}/activities".format(user_id), **page_params)
        activities = data.get("response", {}).get("activities") or {}
        items = activities.get("items") or []
        if not items:
            break
        for item in items:
            if item.get("checkin"):
                yield item
        trailing = activities.get("trailingMarker")
        more_data = activities.get("moreData")
        if not trailing or more_data is False:
            break
        before_marker = trailing


def fetch_all_checkins(token, friends_history, count_first=False, since_delta=None):
    # Generator that yields all checkins using the provided OAuth token
    # If count_first is True it first yields the total checkins count
    if friends_history:
        friends, estimated = fetch_friends(token)
        if count_first:
            yield estimated
        for friend in friends:
            user_id = friend.get("id")
            if not user_id:
                continue
            try:
                for item in fetch_user_activities(token, user_id, since_delta=since_delta):
                    yield item
            except Exception as exc:
                name = friend.get("firstName") or user_id
                click.echo(
                    "Warning: failed to load checkins for {}: {}".format(name, exc),
                    err=True,
                )
        return

    before_timestamp = None
    params = {
        "oauth_token": token,
        "v": API_VERSION,
        "sort": "newestfirst",
        "limit": "250",
    }
    if since_delta:
        params["afterTimestamp"] = int(time.time() - since_delta)
    first = True
    while True:
        if before_timestamp is not None:
            params["beforeTimestamp"] = before_timestamp
        data = requests.get(API_BASE + "/users/self/checkins", params).json()
        if first:
            first = False
            if count_first:
                yield data["response"]["checkins"]["count"]
        if not data.get("response", {}).get("checkins", {}).get("items"):
            break
        for item in data["response"]["checkins"]["items"]:
            yield item
        before_timestamp = item["createdAt"]
