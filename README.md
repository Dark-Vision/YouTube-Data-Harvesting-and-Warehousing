
# YouTube Data Harvesting and Warehousing

This Python script is designed to harvest data from YouTube using the YouTube Data API. It collects various information including channel details, playlist information, video details, and comments. The harvested data is then stored in both MongoDB and MySQL databases for further analysis.

## Prerequisites
Before running the script, make sure you have the following packages installed:

- Python 3.x
- Google API Client Library
- pymongo
- mysql-connector-python
- pandas
- streamlit

You'll also need a YouTube Data API key to access the YouTube API
## Setup
* Install the required packages using pip
* Obtain a YouTube Data API key from the Google Cloud Console.
* Set up MongoDB and MySQL databases with appropriate configurations.
* Update the script with your API key and database connection details.

## Execution
Run the script using :
* Any python IDE with a ".py" extension.
* Enter the desired YouTube channel ID to retrieve data.
* Click on "Get Data" to fetch channel information in MongoDB.
* Select the required channel and Click "Migrate" to populate the SQL databases.
* Use the Streamlit interface to view and interact with the data of various channels.

## Features
* Fetches channel details including subscribers count, view count, and total videos.
* Retrieves playlist information and stores it in MySQL database.
* Collects video details such as title, tags, views, likes, comments, etc.
* Gathers comments data for each video.
* Provides functionalities to query the databases for analysis.

## Queries
The script supports various queries to analyze the collected data:

1. List of videos and their corresponding channels.
2. Channels with the most number of videos.
3. Top 10 most viewed videos and their channels.
4. Number of comments for each video.
5. Videos with the highest number of likes.
6. Total likes and dislikes for each video.
7. Total views for each channel.
8. Channels that published videos in a specific year.
9. Average duration of videos for each channel.
10. Videos with the highest number of comments.