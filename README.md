

# Question 1
Discuss the purpose of this database in the context of the startup, Sparkify, and their analytical goals.


The Sparkify analytics team wants to understand what songs users are listening to and definitely with their data in json files, it is not easy to query the data and extract to extract the insights they need. Setting up a database is the best way to address their needs as it is scalable and one is able to query the data and get insights relatively quiclky.


# Question 2
State and justify your database schema design and ETL pipeline.

The schema of the database is a star schema. A star schema was adopted because the architecture has a FACT Table and Dimenson tables.

The benneefit of this schema is that it is relatively simpler to understand and build. With a star schema, there is no need for complex joins when querying data which makes it faster to access data. With less joins it also makes it simpler to derive business insights.