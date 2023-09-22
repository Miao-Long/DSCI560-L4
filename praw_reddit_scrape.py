#please install the praw API first using pip install praw/pip3 install praw


import pandas as pd
import numpy as np
import bs4, requests
from bs4 import BeautifulSoup
import csv, re, json, time
import praw
import warnings
import storage
import getpass

website = 'https://www.reddit.com/r/tech/'

print('Going to scrape data from Reddit forum using Praw...')

# Creating a Read-only instance, client details are to be kept secret
reddit_read_only = praw.Reddit(client_id="HQYzCFV2GrEKeh7erCEn0A",		 # your client id
                                client_secret="6cnb0x2h8u_iI34KCVB7ItqmTDrZRw",	 # your client secret
                                user_agent="Scraping data from Reddit using PRAW")	 # your user agent


subreddit = reddit_read_only.subreddit("Python")

input_posts = int(input('Enter number of Reddit posts to fetch...'))

#fetching those many top Reddit posts... Praw API has a limit of fetching maximum 1000 posts at a time.

posts_dict = dict()

start = time.time()

posts = []

# Praw fetches only 1000 requests in every single go so sending multiple requests
batch_size = 1000

# Fetching posts in batches
for _ in range(0, input_posts, batch_size):
    try:
        print('Fetching data.....')
        batch = subreddit.top(limit=batch_size)
        posts.extend(batch)
    except:
        #deliberate wait time of 40 seconds
        print('Inside except...')
        time.sleep(40)


posts_dict = {"Title": [], "Post Text": [],
              "ID": [], "Score": [],
              "Total Comments": [], "Post URL": []
              }

for post in posts:

    # Unique ID of each post
    posts_dict["ID"].append(post.id)

    # Title of each post
    posts_dict["Title"].append(post.title)

    # Text inside a post
    posts_dict["Post Text"].append(post.selftext)

    # The score of a post
    posts_dict["Score"].append(post.score)

    # Total number of comments inside the post
    posts_dict["Total Comments"].append(post.num_comments)

    # URL of each post
    posts_dict["Post URL"].append(post.url)

end = time.time()   

# print(len(posts_dict['ID']))
print('Time taken : ', end - start)
#creating dataframe from the dictionary 

dataframe_reddit_praw = pd.DataFrame(posts_dict,columns=['ID', 'Title','Post Text','Post URL','Total Comments','Score'])

#WRITING to CSSV
dataframe_reddit_praw.to_csv('Praw_reddit_data.csv',index=False)

print('Done')


print("""
#########################
# MySQL Reddit Storage  #
#########################
""")

#storage credentials
host = "localhost"
user = "root"
password = getpass.getpass("Enter your MySQL password: ")
database="dsci560_lab4"

database = storage.RedditStorage(
        host=host,
        user=user,
        password=password,
        database=database,
    )

database.readin_csv("cleaned_data.csv", "RedditPosts")
