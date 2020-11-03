import pandas as pd
from elasticsearch import Elasticsearch, helpers
import numpy as np


class ElasticClient:
    def __init__(self, address='localhost:10000'):
        self.es = Elasticsearch(address)

    # ------ Simple operations ------
    def index_documents(self):
        df = pd \
            .read_csv('data/user_ratedmovies.dat', delimiter='\t') \
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
                'ratings': row[row > 0]\
                    .sort_values(ascending=False)\
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

    def get_recommended_movies_for_user(self, user_id, index='users'):
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

    def get_recommended_users_for_movie(self, movie_id, index='movies'):
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


if __name__ == "__main__":
    ec = ElasticClient()
    # ec.index_documents()
    # ------ Simple operations ------
    user_document = ec.get_movies_liked_by_user(75)
    movie_id = np.random.choice(user_document['ratings'])
    movie_document = ec.get_users_that_like_movie(movie_id)
    random_user_id = np.random.choice(movie_document['whoRated'])
    random_user_document = ec.get_movies_liked_by_user(random_user_id)
    print('User 75 likes following movies:')
    print(user_document)
    print('Movie {} is liked by following users:'.format(movie_id))
    print(movie_document)
    print('Is user 75 among users in movie {} document?'.format(movie_id))
    print(movie_document['whoRated'].index(75) != -1)

    import random

    some_test_movie_ID = 1
    print("Some test movie ID: ", some_test_movie_ID)
    list_of_users_who_liked_movie_of_given_ID = ec.get_users_that_like_movie(some_test_movie_ID)["whoRated"]
    print("List of users who liked the test movie: ", *list_of_users_who_liked_movie_of_given_ID)
    index_of_random_user_who_liked_movie_of_given_ID = random.randint(0,
                                                                  len(list_of_users_who_liked_movie_of_given_ID))
    print("Index of random user who liked the test movie: ",
    index_of_random_user_who_liked_movie_of_given_ID)
    some_test_user_ID = list_of_users_who_liked_movie_of_given_ID[index_of_random_user_who_liked_movie_of_given_ID]
    print("ID of random user who liked the test movie: ", some_test_user_ID)
    movies_liked_by_user_of_given_ID = ec.get_movies_liked_by_user(some_test_user_ID)["ratings"]
    print("IDs of movies liked by the random user who liked the test movie: ",
    *movies_liked_by_user_of_given_ID)
    if some_test_movie_ID in movies_liked_by_user_of_given_ID:
        print("As expected, the test movie ID is among the IDs of movies " +
        "liked by the random user who liked the test movie ;-)")
    print("Recommended for user 78")
    print(ec.get_recommended_movies_for_user(78))
    print("Recommended for movie 3")
    print(ec.get_recommended_users_for_movie(3))
