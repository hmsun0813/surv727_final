import requests
import json
import time
import pandas as pd

bearer_token = '<Bearer Token>'
ENDPOINT = "https://api.twitter.com/2/tweets/search/recent"


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2RecentSearchPython"
    return r


def connect_to_endpoint(url, params):
    """
    Request tweets from twitter api v2.
    
    Parameters:
        url (str): the endpoint url
        params (dict): a dict of the query parameters
        
    Return:
        response.json() : a json object containing the response from twitter api
    """

    response = requests.get(url, auth=bearer_oauth, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


def datetime(date_list, time_list):
    """
    Iteratively generate formatted start_time and end_time for query
    
    Parameters:
        date_list (list): list of dates to collect data
        time_list (list): list of times to collect data
        
    Return:
        (start_time, end_time) (tuple): a tuple contaings two lists,
            start_time list and the correspoding end_time list.
    """

    start_time = []
    end_time = []

    for date in date_list:
        for time in time_list:
            time_index = time_list.index(time)

            if time_index == 12:
                continue

            start_time.append(f'{date}T{time}Z')
            end_time.append(f'{date}T{time_list[time_index+1]}Z')

    return (start_time, end_time)


def main():
    """
    Note:
        With current authoritation, the maximum of tweets returned per response is 100.
        In order to retrieve tweets more than that,
        looking for and adding next_token to the query repeatedly is needed here.
    """
    
    # Set time units
    date_list = [
        '2021-11-27', '2021-11-28', '2021-11-29', '2021-11-30',
        '2021-12-01', '2021-12-02', '2021-12-03', '2021-12-04'
    ]

    time_list = [
        '00:00:00', '02:00:00', '04:00:00', '06:00:00',
        '08:00:00', '10:00:00', '12:00:00', '14:00:00',
        '16:00:00', '18:00:00', '20:00:00', '22:00:00', '23:59:59'
    ]

    start_times, end_times = datetime(date_list, time_list)
    start_times = start_times[5:-9]
    end_times = end_times[5:-9]

    # Loop all time units
    for i in range(0, len(start_times)):
        
        # Initialize settings for each time unit
        query_params = {
            'query': '#Omicron (lang:en) -is:retweet',
            'max_results': '100',
            'start_time': None,
            'end_time': None
        }
        next_token = None
        text_data = pd.DataFrame(columns=['id', 'text', 'start_time'])

        # Iterate the request by pagination (next_token)
        for j in range(0, 10):
            query_params.update({
                'start_time': start_times[i],
                'end_time': end_times[i]
            })

            # Update the query; Break the loop if there is no next_token any more
            if j:
                if next_token:
                    query_params.update({'next_token':next_token})
                else:
                    break
            
            # Request tweets
            json_response = connect_to_endpoint(ENDPOINT, query_params)

            df = pd.DataFrame(json_response['data'])
            df['start_time'] = start_times[i]
            text_data = pd.concat([text_data, df])

            # Look for next_token
            if 'next_token' in json_response['meta'].keys():
                next_token = json_response['meta']['next_token']
            else:
                next_token = None

            print(f'Success -- Start_time{start_times[i]}, page:{j+1}')
            j += 1
            time.sleep(10)

        text_data.to_csv(f'text_{i}.csv')


if __name__ == "__main__":
    main()
