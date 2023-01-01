import numpy as np
import pandas as pd
import betfairlightweight as blw
import os
import datetime
import json

with open ('credentials.json') as f:
    cred = json.load(f)
    my_username = cred['username']
    my_password = cred['password']
    my_app_key = cred['app_key']

certs_path = "/home/fiachra/dev_workspace/betfair/certs/"
trading = blw.APIClient(username=my_username,
                        password=my_password,
                        app_key=my_app_key,
                        certs=certs_path)

trading.login()

event_types = trading.betting.list_event_types()

sport_ids = pd.DataFrame({
    'Sport': [event_type_object.event_type.name for event_type_object in event_types],
    'ID': [event_type_object.event_type.id for event_type_object in event_types]
}).set_index('Sport').sort_index()

# print(sport_ids)

# Filter for just horse racing
horse_racing_filter = blw.filters.market_filter(text_query='Horse Racing')

# This returns a list
horse_racing_event_type = trading.betting.list_event_types(
    filter=horse_racing_filter)

# print(len(horse_racing_event_type))
# print(horse_racing_event_type[0])
# print(vars(horse_racing_event_type[0]))

# Get the first element of the list
horse_racing_event_type = horse_racing_event_type[0]

horse_racing_event_type_id = horse_racing_event_type.event_type.id
print(f"The event type id for horse racing is {horse_racing_event_type_id}")

# Define a market filter
uk_event_filter = blw.filters.market_filter(
    event_type_ids=[horse_racing_event_type_id],
    market_countries=['GB'],
    market_start_time={
        'to': (datetime.datetime.utcnow() + datetime.timedelta(days=2)).strftime("%Y-%m-%dT%TZ")
    }
)

# Get a list of all thoroughbred events as objects
uk_hr_events = trading.betting.list_events(
    filter=uk_event_filter
)

# Create a DataFrame with all the events by iterating over each event object
gb_hr_events_today = pd.DataFrame({
    'Event Name': [event_object.event.name for event_object in uk_hr_events],
    'Event ID': [event_object.event.id for event_object in uk_hr_events],
    'Event Venue': [event_object.event.venue for event_object in uk_hr_events],
    'Country Code': [event_object.event.country_code for event_object in uk_hr_events],
    'Time Zone': [event_object.event.time_zone for event_object in uk_hr_events],
    'Open Date': [event_object.event.open_date for event_object in uk_hr_events],
    'Market Count': [event_object.market_count for event_object in uk_hr_events]
})

gb_hr_events_today = gb_hr_events_today[gb_hr_events_today['Event Name'] != "Specials"]
print(vars(uk_hr_events[0]))
gb_hr_events_today.sort_values('Open Date', inplace=True)
print(gb_hr_events_today)

print(gb_hr_events_today['Event ID'].iloc[1])
# Define a market filter
market_types_filter = blw.filters.market_filter(market_type_codes=['WIN'], event_ids=[gb_hr_events_today['Event ID'].iloc[0]])
print(gb_hr_events_today['Event ID'].iloc[1])
# Request market types
market_types = trading.betting.list_market_types(
        filter=market_types_filter
)

print(market_types[0].__dict__)
#print(market_types[0].__dict__.keys())
print(vars(market_types[0]))

# Create a DataFrame of market types
market_types_mooney_valley = pd.DataFrame({
    'Market Type': [market_type_object.market_type for market_type_object in market_types],
})

print(market_types_mooney_valley)
#market_catalogue_filter = blw.filters.market_filter(event_ids=['28971066'])

market_catalogues = trading.betting.list_market_catalogue(
    filter=market_types_filter,
    market_projection=['RUNNER_DESCRIPTION'],
    max_results='100',
    sort='FIRST_TO_START'
)

#print(market_catalogues[0].__dict__)
print(market_catalogues[0].__dict__.keys())
# Create a DataFrame for each market catalogue
market_types_mooney_valley = pd.DataFrame({
    'Competition': [market_cat_object.competition for market_cat_object in market_catalogues],
    'Description': [market_cat_object.description for market_cat_object in market_catalogues],
    'event': [market_cat_object.event for market_cat_object in market_catalogues],
    'event type': [market_cat_object.event_type for market_cat_object in market_catalogues],
#    'event id': [market_cat_object.event_id for market_cat_object in market_catalogues],
    'Market Name': [market_cat_object.market_name for market_cat_object in market_catalogues],
    'Market ID': [market_cat_object.market_id for market_cat_object in market_catalogues],
    'start time': [market_cat_object.market_start_time for market_cat_object in market_catalogues],
    'runners': [market_cat_object.runners for market_cat_object in market_catalogues],
    'Total Matched': [market_cat_object.total_matched for market_cat_object in market_catalogues],
})

print(market_types_mooney_valley)
description = market_types_mooney_valley['runners'][0]

print(description[0])
for runner in description:
    print(runner[0])
def process_runner_books(runner_books):
    '''
    This function processes the runner books and returns a DataFrame with the best back/lay prices + vol for each runner
    :param runner_books:
    :return:
    '''
    best_back_prices = [runner_book.ex.available_to_back[0].price
        if runner_book.ex.available_to_back[0].price
        else 1.01
        for runner_book
        in runner_books]
    best_back_sizes = [runner_book.ex.available_to_back[0].size
        if runner_book.ex.available_to_back[0].size
        else 1.01
        for runner_book
        in runner_books]

    best_lay_prices = [runner_book.ex.available_to_lay[0].price
        if runner_book.ex.available_to_lay[0].price
        else 1000.0
        for runner_book
        in runner_books]
    best_lay_sizes = [runner_book.ex.available_to_lay[0].size
        if runner_book.ex.available_to_lay[0].size
        else 1.01
        for runner_book
        in runner_books]

    selection_ids = [runner_book.selection_id for runner_book in runner_books]
    last_prices_traded = [runner_book.last_price_traded for runner_book in runner_books]
    total_matched = [runner_book.total_matched for runner_book in runner_books]
    statuses = [runner_book.status for runner_book in runner_books]
    scratching_datetimes = [runner_book.removal_date for runner_book in runner_books]
    adjustment_factors = [runner_book.adjustment_factor for runner_book in runner_books]

    df = pd.DataFrame({
        'Selection ID': selection_ids,
        'Best Back Price': best_back_prices,
        'Best Back Size': best_back_sizes,
        'Best Lay Price': best_lay_prices,
        'Best Lay Size': best_lay_sizes,
        'Last Price Traded': last_prices_traded,
        'Total Matched': total_matched,
        'Status': statuses,
        'Removal Date': scratching_datetimes,
        'Adjustment Factor': adjustment_factors
    })
    return df

# Create a price filter. Get all traded and offer data
price_filter = blw.filters.price_projection(
    price_data=['EX_BEST_OFFERS']
)

# Request market books
market_books = trading.betting.list_market_book(
    market_ids=[market_types_mooney_valley['Market ID'][1]],
    price_projection=price_filter
)

# Grab the first market book from the returned list as we only requested one market
market_book = market_books[0]

print("runner 0 : ", market_book.runners[0])
print(vars(market_book.runners[0]))
print("avail to back: ", market_book.runners[0]['ex']['available_to_back'])
print(vars(market_book.runners[0]['ex']))

for runner in market_book.runners:
    #print(vars(runner['ex']))
    print(runner)
i=100
# while i>0:
#     runners_df = process_runner_books(market_book.runners)
#     os.system('clear')
#     print(i,runners_df)
#     i=i-1

trading.logout()
