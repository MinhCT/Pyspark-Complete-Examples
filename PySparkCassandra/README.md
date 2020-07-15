# Pyspark Cassandra batch processing example

**I created this repository in order to share my code and knowledge
when working with big data tools, specifically, Spark and Cassandra.
One of the finest example I'm gonna use is Web Analytics data 
(https://en.wikipedia.org/wiki/Web_analytics)
that can generate millions of records per day. Later, after collecting
such tremendous data, we need to process it to gain many insights
that can help accelerating business decisions. Cassandra is a great
storage options for big web data, but missing of aggregation operations, and
that is why Apache Spark has come to the rescue!**

---------------------------------
- This project focuses on getting analytic data from users' behaviours during their web sessions
and store all raw data in a Cassandra table. I will name the table "user_logs".

- For the sake of simplicity, I have used _generatedata.com_ to generate example data
for the "user_logs" table, feel free to have a look at "data/" directory to get
an overview of the structure.

- In this example, all data is stored in a 4-node Cassandra Cluster (it does not actually
matter, since this is just an example)

- Assuming that the logs are generated from 3 e-commerce stores on web (not including mobile version), and the user data 
is collected and saved to Cassandra whenever the user performs one of the following events:
    * View a page after all web resources have load successfully (Pageview) [**event_id=1**].
    * Click any clickable area within the web (Click) [**event_id=2**].
    * Time staying in a page (Time On Page). [**event_id=3**]
    
- The schema of the "user_logs" table include the below columns:
    * `store_id`: the ID of the website store, accept values: 1, 2, 3
    * `year`: the year that the event is triggered
    * `month`: the month that the event is triggered
    * `day`: the day of the month that the event is triggered
    * `hour`: the hour of the event that the event is triggered
    * `event_id`: the ID of the event (defined as a range of numbers: from 1 to 3)
    * `url`: the URL where the event triggers on
    * `user_id`: the 'unique identified user', unique for each user visiting a store
    * `session_id`: the ID of a visit session (typically last around 30 minutes) of an user.
    This is different from `user_id`
    * `log_time`: the event logged time as milliseconds
    * `detail`: the detailed information of the logged event, stored as JSON string.
    The JSON string can contain values like: time on page, click target, etc.
    You can view the example to understand more about this column

With `store_id`, `year`, `month`, `day`, `hour`, `event_id`, `user_id`, `session_id`, `log_time`
being primary keys

- With this data acquired, the next step is to process it to gain some insights,
useful information for analytic purposes such as:
    * Top items viewed by users per day/week/month...
    * Top items got "Add To Card" button clicked per month,...
    * The conversion rate of each item sold on a store (Click count / impressions)...
    ... Many more based on the type of insightful results that you want to achieve.

This is when the PySpark batch processes take place.   

