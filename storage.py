import mysql.connector
from mysql.connector import Error
import csv
import getpass
import datetime

# Get the current date

class RedditStorage:
    def __init__(self, host, user, password, database) -> None:
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.cursor = self.connect_to_database()
        self.start_date = datetime.date.today()

        self.select_query = """SELECT * FROM RedditPosts;"""
        self.posts_table_query = """
            CREATE TABLE IF NOT EXISTS RedditPosts (
                ID INT PRIMARY KEY AUTO_INCREMENT,  
                Title VARCHAR(1023),  
                post_text TEXT,      
                Post_URL VARCHAR(1023), 
                Total_Comments INT,  
                Score INT 
            );
        """
        self.comment_table_query = """
            -- Create a table to store comments associated with posts
            CREATE TABLE RedditComments (
                comment_id INT AUTO_INCREMENT PRIMARY KEY,
                post_id INT,
                comment_text TEXT,
                comment_author VARCHAR(100),
                comment_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (post_id) REFERENCES RedditPosts(post_id)
            );"""
        self.index_query = """
            -- Create an index for faster retrieval
            CREATE INDEX idx_subreddit ON RedditPosts(subreddit);
        """

    def connect_to_database(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            print("Connected to MySQL database")

            return self.connection.cursor()
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            return None

    def readin_csv(self, csvfile, table_name):
        self.cursor.execute(self.posts_table_query)
        with open(csvfile, "r") as file:
                csv_reader = csv.reader(file)
                next(csv_reader)  # Skip the header row
                
                for row in csv_reader:
                    try:
                        insert_query = f"""
                        INSERT INTO {table_name} (Title, post_text, Post_URL, Total_Comments, Score)
                        VALUES (%s, %s, %s, %s, %s)
                        """
                        self.cursor.execute(insert_query, tuple(row[1:]))
                        self.connection.commit()
                    except mysql.connector.Error as err:
                        print(row)
                        print(f"Error: {err}")
                        return None

    def print_table(self):
        self.cursor.execute(self.select_query)

    def table_def(self):
        pass

if __name__ == "__main__":
    host = "localhost"
    user = "root"
    password = getpass.getpass("Enter your password: ")
    database="dsci560_lab4"

    portfolio = RedditStorage(
        host=host,
        user=user,
        password=password,
        database=database,
    )

    portfolio.readin_csv("cleaned_data.csv", "RedditPosts")


