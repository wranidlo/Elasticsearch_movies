from flask import Flask, jsonify, abort, request
from extended_elasticsearch_client import ElasticClient
app = Flask(__name__)
es = ElasticClient()


# ------ Simple operations ------
@app.route("/user/document/<id>", methods=["GET"])
def get_user(id):
    try:
        index = request.args.get('user_index', default='users')
        result = es.get_movies_liked_by_user(id, index=index)
        return jsonify(result)
    except:
        abort(404)


@app.route("/movie/document/<id>", methods=["GET"])
def get_movie(id):
    try:
        index = request.args.get('movie_index', default='movies')
        result = es.get_users_that_like_movie(id, index)
        return jsonify(result)
    except:
        abort(404)


# ------ Preselection ------
@app.route("/user/preselection/<id>", methods=["GET"])
def user_preselection(id):
    try:
        index = request.args.get('user_index', default='users')
        result = es.get_preselection_for_user(int(id), index)
        result = {
        "moviesFound": result
        }
        return jsonify(result)
    except:
        abort(404)


@app.route("/movie/preselection/<id>", methods=["GET"])
def movies_preselection(id):
    try:
        index = request.args.get('movie_index', default='movies')
        result = es.get_preselection_for_movie(int(id), index)
        result = {
        "usersFound": result
        }
        return jsonify(result)
    except:
        abort(404)


# ------ Add/Update/Delete ------
@app.route("/user/document/<user_id>", methods=["PUT"])
def add_user_document(user_id):
    try:
        user_index = request.args.get('user_index', default='users')
        movie_index = request.args.get('movie_index', default='movies')
        movies_liked_by_user = request.json
        es.add_user_document(user_id, movies_liked_by_user, user_index, movie_index)
        return "Ok", 200
    except:
        abort(400)


@app.route("/movie/document/<movie_id>", methods=["PUT"])
def add_movie_document(movie_id):
    try:
        user_index = request.args.get('user_index', default='users')
        movie_index = request.args.get('movie_index', default='movies')
        users_who_like_movie = request.json
        es.add_movie_document(movie_id, users_who_like_movie, movie_index, user_index)
        return "Ok", 200
    except:
        abort(400)


@app.route("/user/document/<user_id>", methods=["POST"])
def update_user_document(user_id):
    try:
        user_index = request.args.get('user_index', default='users')
        movie_index = request.args.get('movie_index', default='movies')
        movies_liked_by_user = request.json
        es.update_user_document(user_id, movies_liked_by_user, user_index, movie_index)
        return "Ok", 200
    except:
        abort(400)


@app.route("/movie/document/<movie_id>", methods=["POST"])
def update_movie_document(movie_id):
    try:
        user_index = request.args.get('user_index', default='users')
        movie_index = request.args.get('movie_index', default='movies')
        users_who_like_movie = request.json
        es.update_movie_document(movie_id, users_who_like_movie, movie_index, user_index)
        return "Ok", 200
    except:
        abort(400)


@app.route("/user/document/<user_id>", methods=["DELETE"])
def delete_user_document(user_id):
    try:
        user_index = request.args.get('user_index', default='users')
        movie_index = request.args.get('movie_index', default='movies')
        es.delete_user_document(user_id, user_index, movie_index)
        return "Ok", 200
    except:
        abort(400)


@app.route("/movie/document/<movie_id>", methods=["DELETE"])
def delete_movie_document(movie_id):
    try:
        user_index = request.args.get('user_index', default='users')
        movie_index = request.args.get('movie_index', default='movies')
        es.delete_movie_document(movie_id, movie_index, user_index)
        return "Ok", 200
    except:
        abort(400)


@app.route("/user/bulk", methods=["POST"])
def bulk_update_users():
    """
    Body should look like this: [{"user_id": 123, "liked_movies": [1,2,3,4]}, ...]
    """
    user_index = request.args.get('user_index', default='users')
    movie_index = request.args.get('movie_index', default='movies')
    body = request.json
    es.bulk_user_update(body, user_index)
    return 'Ok', 200


@app.route("/movie/bulk", methods=["POST"])
def bulk_update_movies():
    """
    Body should look like this: [{"movie_id": 123, "users_who_liked_movie": [1,2,3,4]}, ...]
    """
    user_index = request.args.get('user_index', default='users')
    movie_index = request.args.get('movie_index', default='movies')
    body = request.json
    es.bulk_movie_update(body, movie_index)
    return 'Ok', 200


@app.route("/indices/<index_name>", methods=["PUT"])
def create_index(index_name):
    try:
        es.create_index(str(index_name))
        return 'Ok', 200
    except:
        abort(404)


@app.route("/indices", methods=["GET"])
def get_indexes():
    try:
        result = es.get_indexes()
        return jsonify(result)
    except:
        abort(404)


@app.route("/reindex", methods=["POST"])
def reindex():
    """
    Body should look like this:  {'source': 'users', 'dest': 'temp'}
    """
    body = request.json
    es.reindex(body["source"], body["dest"])
    return 'Ok', 200


@app.route("/indices/<index_name>", methods=["DELETE"])
def delete_index(index_name):
    try:
        es.delete_index(str(index_name))
        return 'Ok', 200
    except:
        abort(404)


if __name__ == '__main__':
    es.index_documents()
    app.run(port=5000)
