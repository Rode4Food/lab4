
import requests
import os
from neo4j import GraphDatabase
from neo4j._sync.work import session

# VK API configuration
user_id = "203083218"
access_token = ""


# VK API request function
def vk_request(method, access_token, params=None):
    url = f"https://api.vk.com/method/{method}"
    params = params or {}
    params.update({
        "access_token": access_token,
        "v": "5.131"
    })
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

def get_all_followers(user_id, access_token):
    followers = []
    offset = 0
    while True:
        response = vk_request("users.getFollowers", access_token,
                               {"user_id": user_id, "offset": offset, "count": 1000, "fields": "first_name,last_name,screen_name"})
        items = response['response'].get('items', [])
        if not items:
            break
        followers.extend(items)
        offset += 100
    return followers


def get_all_subscriptions(user_id, access_token):
    subscriptions = []
    offset = 0
    while True:
        response = vk_request("users.getSubscriptions", access_token,
                              {"user_id": user_id, "offset": offset, "count": 1000, "fields": "first_name,last_name,screen_name"})
        items = response['response'].get('items', [])
        if not items:
            break
        subscriptions.extend(items)
        offset += 100
    return subscriptions



def get_user_data_recursive(user_id, access_token, depth=2):
    data = []
    try:
        # Получаем информацию о пользователе
        response = vk_request("users.get", access_token,
                               {"user_ids": user_id, "fields": "screen_name,first_name,last_name,sex,city"})
        user_info = response.get('response', [{}])[0]  # Получаем первый элемент или пустой словарь
        if not user_info.get("id"):
            # Если данных нет, пропускаем пользователя
            return data

        # Получаем данные о подписчиках, подписках, друзьях и группах
        followers = get_all_followers(user_id, access_token) or []
        subscriptions = get_all_subscriptions(user_id, access_token) or []
        friends_response = vk_request("friends.get", access_token,
                                       {"user_id": user_id, "fields": "screen_name,first_name,last_name"})
        friends = friends_response.get('response', {}).get('items', []) or []
        groups_response = vk_request("groups.get", access_token, {"user_id": user_id, "extended": 1})
        groups = groups_response.get('response', {}).get('items', []) or []

        # Формируем данные о пользователе
        user_data = {
            "id": user_info.get("id"),
            "screen_name": user_info.get("screen_name"),
            "name": f"{user_info.get('first_name')} {user_info.get('last_name')}",
            "sex": user_info.get("sex"),
            "city": user_info.get("city", {}).get("title"),
            "followers": followers,
            "subscriptions": subscriptions,
            "friends": friends,
            "groups": [
                {"id": group.get("id"), "name": group.get("name"), "screen_name": group.get("screen_name")}
                for group in groups
            ]
        }
        data.append(user_data)

        # Рекурсивно собираем данные для вложенных пользователей
        if depth > 1:
            for follower in followers:
                if "id" in follower:
                    data.extend(get_user_data_recursive(follower["id"], access_token, depth - 1))
            for subscription in subscriptions:
                if "id" in subscription:
                    data.extend(get_user_data_recursive(subscription["id"], access_token, depth - 1))
            for friend in friends:
                if "id" in friend:
                    data.extend(get_user_data_recursive(friend['id'], access_token, depth - 1))

    except Exception as e:
        # Сохраняем информацию об ошибке для отладки (можно выключить в production)
        # print(f"Error fetching data for user {user_id}: {e}")
        pass
    return data





class Neo4jDatabase:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def insert_data(self, data):
        with self.driver.session() as session:
            for user_data in data:
                self._create_user_node(session, user_data)
                self._insert_relationships(session, user_data)

    def _create_user_node(self, session, user_info):
        session.run(
            "MERGE (u:User {id: $id}) "
            "SET u.screen_name = $screen_name, u.name = $name, u.sex = $sex, u.city = $city",
            id=user_info["id"],
            screen_name=user_info.get("screen_name"),
            name=user_info.get("name"),
            sex=user_info.get("sex"),
            city=user_info.get("city")
        )

    def _insert_relationships(self, session, user_data):

        for follower in user_data["followers"]:
            session.run(
                "MERGE (f:User {id: $follower_id, name: $follower_name}) "
                "WITH f "
                "MATCH (u:User {id: $user_id}) "
                "MERGE (f)-[:FOLLOWS]->(u)",
                follower_id=follower["id"], follower_name=f"{follower.get('first_name')} {follower.get('last_name')}",
                user_id=user_data["id"]
            )

        for subscription in user_data["subscriptions"]:
            session.run(
                "MERGE (s:User {id: $subscription_id, name: $subscription_name}) "
                "WITH s "
                "MATCH (u:User {id: $user_id}) "
                "MERGE (u)-[:SUBSCRIBES]->(s)",
                subscription_id=subscription["id"],
                subscription_name=f"{subscription.get('first_name')} {subscription.get('last_name')}",
                user_id=user_data["id"]
            )
        # Insert relationships for friends
        for friend in user_data["friends"]:
            session.run(
                "MERGE (f:User {id: $friend_id, name: $friend_name}) "
                "WITH f "
                "MATCH (u:User {id: $user_id}) "
                "MERGE (u)-[:FRIENDS_WITH]->(f)",
                friend_id=friend["id"], friend_name=f"{friend.get('first_name')} {friend.get('last_name')}",
                user_id=user_data["id"]
            )
        # Insert relationships for groups
        for group in user_data["groups"]:
            session.run(
                "MERGE (g:Group {id: $group_id, name: $group_name}) "
                "WITH g "
                "MATCH (u:User {id: $user_id}) "
                "MERGE (u)-[:MEMBER_OF]->(g)",
                group_id=group["id"], group_name=group["name"], user_id=user_data["id"]
            )


def main():
    # Initialize Neo4j connection
    db = Neo4jDatabase("bolt://localhost:7687", "neo4j", "neo4jlab4")

    user_data = get_user_data_recursive(user_id, access_token, depth=2)
    print("Сбор данных заверешн")
    print("Загрузка данных в БД")
    db.insert_data(user_data)

    db.close()

if __name__ == "__main__":
    main()
