import threading
import time
import getpass
from storage import RedditStorage

# Global variable to signal when to stop the execution
stop_execution = False

def your_function(rs):
    # Define the function you want to execute here
    print("Executing your_function()")

def execution_thread(interval, rs):
    global stop_execution
    while not stop_execution:
        your_function(rs)  # Call your function
        time.sleep(interval)

def main():
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

    update_interval = int(input("Enter the execution interval in seconds (X): "))
    
    execution_thread_instance = threading.Thread(target=execution_thread, args=(update_interval,portfolio))
    execution_thread_instance.start()
    
    while True:
        if "quit" == getpass.getpass("Enter 'Quit' to quit...").lower():
            global stop_execution
            stop_execution = True
            execution_thread_instance.join()
            break

if __name__ == "__main__":
    main()
