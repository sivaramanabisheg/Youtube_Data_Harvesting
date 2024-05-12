# Capstone---P1
# YouTube Data Harvesting and Warehousing using Python, SQL and Streamlit

This project is a YouTube data analysis tool built with Python, utilizing the YouTube Data API to extract information about channels, videos, comments, and playlists. It also integrates with a MySQL database to store and manage the collected data.

## Installation

To run this project locally, you'll need to:

1. Install the necessary Python libraries (`google-api-python-client`, `pandas`, `mysql-connector-python`, `streamlit`) using pip.
2. Obtain a YouTube Data API key from the Google Cloud Console and replace the placeholder `api_key` in the code with your API key.
3. Configure a MySQL database and update the connection details (`host`, `user`, `password`) in the code accordingly.

## Usage

Once the project is set up, you can:

1. Enter a YouTube Channel ID to extract data from.
2. Click on the "Move to Database" button to store the extracted data in the MySQL database.
3. View the collected data in various tables using the Streamlit app.
4. Perform data analysis and generate insights using the provided SQL queries.

Feel free to explore the code and customize it to suit your needs!

## Contributing

Contributions are welcome! If you have any suggestions, bug fixes, or enhancements, please open an issue or submit a pull request on GitHub.
