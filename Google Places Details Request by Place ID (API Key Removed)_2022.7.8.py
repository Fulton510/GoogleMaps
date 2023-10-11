import datetime
import json
import os
import re
import sqlite3
import requests

DATABASE_FOLDER = 'db'
DATAFILES_FOLDER = 'datafiles'
LOG_FOLDER = 'logs'
PLACEDETAILS_FOLDER = 'placedetails'
FIELDS = "formatted_address,formatted_phone_number,website,url,business_status"
APIKEY = "Insert API Key"

statsfile = 'google-api-stats.csv'
notfound =  'notfound.csv'

def save_not_found(msg):
  now = datetime.datetime.now()
  date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
  file_exists = os.path.exists(notfound)
  if file_exists:
    stats = open(notfound, 'a')
  else:
    stats = open(notfound, 'w')
  stats.write(date_time + ", " + msg + "\n")
  stats.close()

def save_api_use_stat(msg):
  now = datetime.datetime.now()
  date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
  file_exists = os.path.exists(statsfile)
  if file_exists:
    stats = open(statsfile, 'a')
  else:
    stats = open(statsfile, 'w')
  stats.write(date_time + ", " + msg + "\n")
  stats.close()

class GooglePlaces(object):
  def __init__(self, apiKey):
    super(GooglePlaces, self).__init__()
    self.apiKey = apiKey

  def get_place_details(self, place_id, fields):
    func = "PLACE DETAILS"
    print(func, place_id)
    endpoint_url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        'placeid': place_id,
        'fields': ",".join(fields),
        'key': self.apiKey
    }
    save_api_use_stat("url: " + str(endpoint_url) + ", placeid: " + str(place_id) + ", fields: " + str(",".join(fields)))

    res = requests.get(endpoint_url, params=params)

    place_details = json.loads(res.content)
    place_details_status = place_details['status']
    print(place_id, place_details_status)
    save_api_use_stat("status: " + str(place_details_status))

    if place_details_status == 'OVER_QUERY_LIMIT' or place_details_status == 'REQUEST_DENIED' or place_details_status == 'INVALID_REQUEST' or place_details_status == 'UNKNOWN_ERROR':
      msg = "Google API Error. Operation aborted..."
      print(msg, place_details_status)
      if place_details_status == 'OVER_QUERY_LIMIT':
        file_exists = os.path.exists(LOG_FOLDER + '/google_error.log')
        if file_exists == True:
          with open(LOG_FOLDER + '/google_error.log', 'a') as outfile:
              outfile.write(place_details_status + ',' + place_id + '\n')
        else:
          with open(LOG_FOLDER + '/google_error.log', 'w') as outfile:
              outfile.write(place_details_status + ',' + place_id + '\n')
        os._exit(0)

    if place_details_status == 'OK':
      place_details_result = place_details['result']
      return place_details_result
    else:
      save_not_found("placeid: " + str(place_id) + "status: " + str(place_details_status))
      print(place_details_status)
      return

class DAL(object):
  def __init__(self, db):
    self.con = sqlite3.connect(db)
    self.con.row_factory = self.dict_factory

  def dict_factory(self, cursor, row):
      d = {}
      for idx, col in enumerate(cursor.description):
          d[col[0]] = row[idx]
      return d

  def retrieve_place_id_s_from_pow(self):
    sql = "SELECT place_id FROM pow_details"
    results = []
    try:
      with self.con:
        cur = self.con.execute(sql)
        results = cur.fetchall()
    except:
      print("couldn't find any rows")
    return results

def download_build_pow_details(place_id):
  func = "BUILD PLACE DETAILS RECORDS"
  print(func)
  fields = FIELDS.split(",")
  details = api.get_place_details(place_id, fields)  # calls to place details api

  with open(PLACEDETAILS_FOLDER + "/" + place_id + '.json', 'w') as outfile:
    json.dump(details, outfile)

  address = ""
  phone = ""
  website = ""
  url = ""
  business_status = ''
  urls = []

  try:
    address = details['formatted_address']
  except:
    pass
  try:
    phone = details['formatted_phone_number']
  except:
    pass
  try:
    website = details['website']
  except:
    pass
  try:
    url = details['url']
    urls.append(url)
  except:
    pass
  try:
    business_status = details['business_status']
  except:
    pass

  statexobj = [address, phone, website, url, business_status]
  print(statexobj)
  return statexobj

if __name__ == '__main__':
  print("begin fetching place_details")
  dal = DAL(DATABASE_FOLDER + '/powdata.db')
  api = GooglePlaces(APIKEY)

  place_ids = dal.retrieve_place_id_s_from_pow()

  # skipto = "ChIJ--rrD0i_woARhFN4rWq9zmo"
  # skipto = "ChIJFXfaQmjJ-4YRHP7Otuh6gc4"
  skipto = "ChIJy6PB2k88sYkR8sQXMl9wWrs"

  fetch = True
  count = 0
  for place_id in place_ids:
    placeid = place_id['place_id']
    print(placeid)
    count = count + 1
    if placeid == skipto:
      fetch = True
      print("COUNT", count)
      # break
    if fetch == True:
      print("FOUND THE ID")
      download_build_pow_details(placeid)
      # break
      
  # TEST
  # place_id = 'ChIJv7ui_Z7sFogRBKLDuIj7T6I'
  # name = 'Community Church of Waterford'
  # download_build_pow_details(place_id, name)

