import requests as rq
import re
prefix = 'http://localhost:5000'


def print_response(response, body=None):
    print('Request:')
    print('\tUrl: {}'.format(response.url))
    print('\tMethod: {}'.format(re.search("<PreparedRequest \[(\w+)\]>", str(response.request)).group(1)))
    print('\tBody: {}'.format(body))
    print('Response:')
    print('\tCode: {}'.format(response.status_code))
    content = response.content.decode("utf-8")
    if len(content) > 0:
        content = content[:160] + "..." + content[-38:]
        print('\tContent: {}'.format(content), end='')
        print('\tHeaders: {}'.format(response.headers))
        print('-' * 30)


def send_get(message, url):
    print(message)
    url = prefix + url
    response = rq.get(url)
    print_response(response)


def send_post(message, url, body=None):
    print(message)
    url = prefix + url
    response = rq.post(url, json=body)
    print_response(response, body)


def send_put(message, url, body=None):
    print(message)
    url = prefix + url
    if body is None:
        response = rq.put(url)
        print_response(response, body)
    else:
        response = rq.put(url, data=body, headers={"Content-Type": "application/json"})
        print_response(response, body)


def send_delete(message, url):
    print(message)
    url = prefix + url
    response = rq.delete(url)
    print_response(response)


# ------ Simple operations ------
send_get('Document for user with ID = 75', '/user/document/75')
send_get('Document for user with ID = 78', '/user/document/78')
send_get('Non existing user document', '/user/document/0')
send_get('Document for movie with ID = 3', '/movie/document/3')
send_get('Document for movie with ID = 101', '/movie/document/101')
# ------ Preselection ------
send_get('Preselection for user 75', '/user/preselection/75')
send_get('Preselection for movie 3', '/movie/preselection/3')
# ------ Add/Update/Delete ------
send_put('Add new movie document number 80000 that nobody likes', '/movie/document/80000', '[]')
send_put('Add new movie document number 80001 that nobody likes', '/movie/document/80001', '[]')
send_put('Add new movie document number 80002 that nobody likes', '/movie/document/80002', '[]')
send_put('Add new user document number 90000, who likes movies 80000 and 80001',
 '/user/document/90000', '[80000, 80001]')
send_get('Get new user 90000 document', '/user/document/90000')
send_get('Get updated movie 80000 document', '/movie/document/80000')
send_get('Get updated movie 80001 document', '/movie/document/80001')
send_post('Update user 90000, that he now likes movies 80000 and 80002', '/user/bulk',
  [{"user_id": 90000, "liked_movies": [80000, 80002]}])
send_get('Get updated user 90000 document', '/user/document/90000')
send_get('Get updated movie 80000 document', '/movie/document/80000')
send_get('Get updated movie 80001 document', '/movie/document/80001')
send_get('Get updated movie 80002 document', '/movie/document/80002')
send_delete('Remove user document number 90000', '/user/document/90000')
send_get('Get updated movie 80000 document', '/movie/document/80000')
send_get('Get updated movie 80001 document', '/movie/document/80001')
send_get('Get updated movie 80002 document', '/movie/document/80002')
send_delete('Remove movie document number 80000', '/movie/document/80000')
send_delete('Remove movie document number 80001', '/movie/document/80001')
send_delete('Remove movie document number 80002', '/movie/document/80002')


send_put('Adding new index temp', '/indices/temp')
send_get('Getting list of indexes', '/indices')
send_post('Reindex users to temp', '/reindex', {'source': 'users', 'dest': 'temp'})
send_get('Document for user with ID = 75 for index temp', '/user/document/75?user_index=temp')
send_put('Add new movie document number 80003 that nobody likes for index temp',
         '/movie/document/80003?user_index=temp', '[75]')
send_get('Get updated movie 80003 document', '/movie/document/80003?user_index=temp')
send_get('Document for user with ID = 75  for index temp', '/user/document/75?user_index=temp')
send_delete('Delete index temp', '/indices/temp')
send_get('Getting list of indexes', '/indices')
