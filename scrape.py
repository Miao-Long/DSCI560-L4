import pandas as pd
import praw
import time
import os
import schedule
import argparse
from gensim.models.doc2vec import Doc2Vec, TaggedDocument
from nltk.tokenize import word_tokenize
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from sklearn.feature_extraction.text import TfidfVectorizer

# Function to fetch Reddit posts using PRAW and update the database
def fetch_and_update_database(subreddit_name, input_posts):
    print('Fetching data from Reddit...')
    # Your PRAW credentials
    reddit_read_only = praw.Reddit(client_id="YOUR_CLIENT_ID",
                                    client_secret="YOUR_CLIENT_SECRET",
                                    user_agent="YOUR_USER_AGENT")
    
    subreddit = reddit_read_only.subreddit(subreddit_name)

    # Fetching posts
    posts = []
    batch_size = 1000

    for _ in range(0, input_posts, batch_size):
        try:
            batch = subreddit.top(limit=batch_size)
            posts.extend(batch)
        except Exception as e:
            print(f'Error fetching data: {str(e)}')
            continue

    # Prepare data for the database
    posts_dict = {"Title": [], "Post Text": [],
                  "ID": [], "Score": [],
                  "Total Comments": [], "Post URL": []
                  }

    for post in posts:
        posts_dict["ID"].append(post.id)
        posts_dict["Title"].append(post.title)
        posts_dict["Post Text"].append(post.selftext)
        posts_dict["Score"].append(post.score)
        posts_dict["Total Comments"].append(post.num_comments)
        posts_dict["Post URL"].append(post.url)

    # Create or update the database
    dataframe_reddit_praw = pd.DataFrame(posts_dict, columns=['ID', 'Title', 'Post Text', 'Post URL', 'Total Comments', 'Score'])
    dataframe_reddit_praw.to_csv('Praw_reddit_data.csv', index=False)
    print('Database updated successfully.')

# Function to process and cluster the data
def process_and_cluster_data():
    # Load data from the database
    reddit_post_df = pd.read_csv('Praw_reddit_data.csv')
    reddit_post_titles = reddit_post_df['Title'].values.tolist()

    # Train Doc2Vec model
    tagged_data = [TaggedDocument(words=word_tokenize(doc.lower()), tags=[str(i)]) for i, doc in enumerate(reddit_post_titles)]
    model = Doc2Vec(vector_size=30, min_count=2, epochs=100)
    model.build_vocab(tagged_data)
    model.train(tagged_data, total_examples=model.corpus_count, epochs=model.epochs)

    # Get document vectors
    document_vectors = [model.infer_vector(word_tokenize(doc.lower())) for doc in reddit_post_titles]

    # Cluster using KMeans
    model_KMeans = KMeans(n_clusters=30, max_iter=500, random_state=37)
    model_KMeans.fit(document_vectors)

    # Implement PCA for visualization
    pca = PCA(n_components=2)
    document_vectors_2d = pca.fit_transform(document_vectors)

    # Create a scatter plot
    plt.scatter(document_vectors_2d[:, 0], document_vectors_2d[:, 1], c=model_KMeans.labels_, cmap='rainbow')
    plt.title('KMeans Clustering Results with 2 features using PCA')
    plt.xlabel('PCA Dimension 1')
    plt.ylabel('PCA Dimension 2')
    plt.show()

    # ... Rest of your code for word clouds and clustering ...

# Function to handle user input and display clusters
def handle_user_input():
    while True:
        user_input = input("Enter keywords or 'quit' to exit: ")
        if user_input == 'quit':
            break
        else:
            # Process user input and find the closest cluster
            # Display cluster messages and graphical representations
            closest_cluster = find_closest_cluster(user_input)
            display_cluster_messages_and_graph(closest_cluster)

# Function to find the closest cluster based on user input
def find_closest_cluster(user_input):
    # Implement logic to find the closest cluster based on user input
    # You can use techniques like cosine similarity between user input and cluster centroids
    # Return the cluster label that is closest to the user input
    # For now, let's assume a random cluster label (0-29) as an example
    import random
    return random.randint(0, 29)

# Function to display cluster messages and graphical representations
def display_cluster_messages_and_graph(cluster_label):
    # Load data from the database
    reddit_post_df = pd.read_csv('Praw_reddit_data.csv')
    reddit_post_titles = reddit_post_df['Title'].values.tolist()

    # Get messages in the selected cluster
    cluster_messages = [reddit_post_titles[i] for i, label in enumerate(model_KMeans.labels_) if label == cluster_label]

    # Display messages
    print(f"Messages in Cluster {cluster_label}:")
    for message in cluster_messages:
        print("- " + message)

    # ... Rest of your code for displaying word clouds and graphical representations ...

if __name__ == "__main__":
    # Command-line arguments for updating interval
    print("a")
    parser = argparse.ArgumentParser(description='Reddit Data Updater and Analyzer')
    print("b")
    parser.add_argument('update_interval', type=int, help='Update interval in minutes')
    print("c")
    args = parser.parse_args()

    # Schedule data update job
    schedule.every(args.update_interval).minutes.do(fetch_and_update_database, subreddit_name="Python", input_posts=1000)



    # Run data update and analysis in the background
    while True:
        schedule.run_pending()
        time.sleep(1)

    # When the user interrupts the script, allow user interaction
    handle_user_input()