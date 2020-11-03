import pandas as pd
from elasticsearch import Elasticsearch, helpers
import numpy as np


class ElasticClient:
    def __init__(self, address='localhost:10000'):
        self.es = Elasticsearch(address)

    # ------ Simple operations ------
    def index_documents(self):
        df = pd \
                 .read_csv('data/user_ratedmovies.dat', delimiter='\t', nrows=100000) \
                 .loc[:, ['userID', 'movieID', 'rating']]
        means = df.groupby(['userID'], as_index=False, sort=False) \
                    .mean() \
                    .loc[:, ['userID', 'rating']] \
            .rename(columns={'rating': 'ratingMean'})
        df = pd.merge(df, means, on='userID', how="left", sort=False)
        df['ratingNormal'] = df['rating'] - df['ratingMean']

        ratings = df.loc[:, ['userID', 'movieID', 'ratingNormal']] \
            .rename(columns={'ratingNormal': 'rating'}) \
            .pivot_table(index='userID', columns='movieID', values='rating') \
            .fillna(0)

        print("Indexing users...")
        index_users = [{
            "_index": "users",
            "_type": "user",
            "_id": index,
            "_source": {
                'ratings': row[row > 0] \
                    .sort_values(ascending=False) \
                    .index.values.tolist()
            }
        } for index, row in ratings.iterrows()]
        helpers.bulk(self.es, index_users)
        print("Done")
        print("Indexing movies...")
        index_movies = [{
            "_index": "movies",
            "_type": "movie",
            "_id": column,
            "_source": {
                "whoRated": ratings[column][ratings[column] > 0] \
                    .sort_values(ascending=False) \
                    .index.values.tolist()
            }
        } for column in ratings]
        helpers.bulk(self.es, index_movies)
        print("Done")

    def get_movies_liked_by_user(self, user_id, index='users'):
        user_id = int(user_id)
        return self.es.get(index=index, doc_type="user", id=user_id)["_source"]

    def get_users_that_like_movie(self, movie_id, index='movies'):
        movie_id = int(movie_id)
        return self.es.get(index=index, doc_type="movie", id=movie_id)["_source"]

    def get_preselection_for_user(self, user_id, index='users'):
        user_id = int(user_id)

        movies_liked = self.es.search(index=index, body={
            "query": {
                "term": {
                    "_id": user_id
                }
            }})["hits"]["hits"][0]["_source"]["ratings"]

        users_with_similar_taste = self.es.search(index=index, body={
            "query": {
                "terms": {
                    "ratings": movies_liked
                }
            }})["hits"]["hits"]

        recommended_set = set()
        for ratings in users_with_similar_taste:
            if ratings["_id"] != user_id:
                ratings = ratings["_source"]["ratings"]
                for rating in ratings:
                    if rating not in movies_liked:
                        recommended_set.add(rating)

        return list(recommended_set)

    def get_preselection_for_movie(self, movie_id, index='movies'):
        movie_id = int(movie_id)

        users_liking = self.es.search(index=index, body={
            "query": {
                "term": {
                    "_id": movie_id
                }
            }})["hits"]["hits"][0]["_source"]["whoRated"]

        movies_liked_by_the_same_people = self.es.search(index=index, body={
            "query": {
                "terms": {
                    "whoRated": users_liking
                }
            }})["hits"]["hits"]

        recommended_set = set()
        for ratings in movies_liked_by_the_same_people:
            if ratings["_id"] != movie_id:
                ratings = ratings["_source"]["whoRated"]
                for rating in ratings:
                    if rating not in users_liking:
                        recommended_set.add(rating)

        return list(recommended_set)

    def add_user_document(self, user_id, movies_liked, user_index='users', movie_index='movies'):
        user_id = int(user_id)
        self.es.index(index=user_index, doc_type='user', id=user_id, body={
            "ratings": movies_liked
        })
        for e in movies_liked:
            temp = list(self.get_users_that_like_movie(e, movie_index)["whoRated"])
            temp.append(user_id)
            self.update_movie_document(int(e), temp, movie_index)

    def add_movie_document(self, movie_id, users_liking, movie_index='movies', user_index='users'):
        movie_id = int(movie_id)
        self.es.index(index=movie_index, doc_type='movie', id=movie_id, body={
            "whoRated": users_liking
        })
        for e in users_liking:
            temp = list(self.get_movies_liked_by_user(e, user_index)["ratings"])
            temp.append(movie_id)
            self.update_user_document(int(e), temp, user_index)

    def update_user_document(self, user_id, movies_liked, user_index='users'):
        user_id = int(user_id)
        self.es.index(index=user_index, doc_type='user', id=user_id, body={
            "ratings": movies_liked
        })

    def update_movie_document(self, movie_id, users_liking, movie_index='movies'):
        movie_id = int(movie_id)
        self.es.index(index=movie_index, doc_type='movie', id=movie_id, body={
            "whoRated": users_liking
        })

    def bulk_user_update(self, body, user_index):
        for e in body:
            user_id = int(e["user_id"])

            movies_liked_before = self.get_movies_liked_by_user(user_id, user_index)["ratings"]
            for movie in list(movies_liked_before):
                temp = list(self.get_users_that_like_movie(movie)["whoRated"])
                if user_id in temp:
                    temp.remove(user_id)
                self.update_movie_document(int(movie), temp)

            self.es.index(index=user_index, doc_type='user', id=user_id, body={
                "ratings": e["liked_movies"]
            })

            movies_liked_now = list(e["liked_movies"])
            for movie in list(movies_liked_now):
                temp = list(self.get_users_that_like_movie(movie)["whoRated"])
                temp.append(user_id)
                self.update_movie_document(int(movie), temp)

    def bulk_movie_update(self, body, movie_index):
        for e in body:
            movie_id = int(e["movie_id"])

            users_liking_before = self.get_users_that_like_movie(movie_id, movie_index)["whoRated"]
            for user in list(users_liking_before):
                temp = list(self.get_movies_liked_by_user(user)["ratings"])
                if movie_id in temp:
                    temp.remove(movie_id)
                self.update_user_document(int(user), temp)
            self.es.index(index=movie_index, doc_type='movie', id=movie_id, body={
                "ratings": e["users_who_liked_movie"]
            })

            users_liking_now = list(e["users_who_liked_movie"])
            for user in list(users_liking_now):
                temp = list(self.get_users_that_like_movie(user)["ratings"])
                temp.append(movie_id)
                self.update_movie_document(int(user), temp)

    def delete_user_document(self, user_id, user_index, movie_index='movies'):
        user_id = int(user_id)
        movies_liked = self.get_movies_liked_by_user(user_id, user_index)["ratings"]
        self.es.delete(index=user_index, doc_type="user", id=user_id)
        for e in list(movies_liked):
            temp = list(self.get_users_that_like_movie(e, movie_index)["whoRated"])
            if user_id in temp:
                temp.remove(user_id)
            self.update_movie_document(int(e), temp)

    def delete_movie_document(self, movie_id, movie_index, user_index='users'):
        movie_id = int(movie_id)
        users_liking = self.get_users_that_like_movie(movie_id, movie_index)["whoRated"]
        self.es.delete(index=movie_index, doc_type="movie", id=movie_id)
        for e in list(users_liking):
            temp = list(self.get_movies_liked_by_user(e, user_index)["ratings"])
            if movie_id in temp:
                temp.remove(movie_id)
            self.update_user_document(int(e), temp)

    def create_index(self, index):
        self.es.indices.create(index=index, body={
            "settings": {
                "number_of_shards": 5,
                "number_of_replicas": 1
            }})

    def get_indexes(self):
        return self.es.indices.get_alias()

    def reindex(self, old_index, new_index):
        helpers.reindex(self.es, source_index=old_index, target_index=new_index)

    def delete_index(self, index):
        self.es.indices.delete(index=index, ignore=[400, 404])


if __name__ == "__main__":
    ec = ElasticClient()
    # ec.index_documents()
