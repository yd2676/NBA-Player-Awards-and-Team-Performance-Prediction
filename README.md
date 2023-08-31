# 202212-19-NBA-Team-Performance-and-Player-Awards-Prediction

## Project Goals and Objectives:
- Predict awards voting results (Rookie of the Year, Sixth Man of the Year, Most Valuable Player, Defensive Player of the year) and the probability of each team entering the playoffs of current year, based on their previous performance.
- Design a web application to help fans get the prediction results and statistics of the players and teams.

## Datasets
The dataset is downloaded from www.kaggle.com/datasets/sumitrodatta/nba-aba-baa-stats. The data was scraped from basketball-reference.com, one of the greatest comprehensive basketball stats site. We also use NBA_API to get the latest data and make predictions using pre-trained machine learning models.

## How to run the web application
The project is built using various service provided by Google Could Platform. To use all the functions of the application, all the datasets needs to be uploaded to BigQuery and VM instance with installation of Airflow. Team_DAG.py and Player_DAG.py should also be uploaded to VM instance. With the help of these service, the two DAGs will trigger at 7 a.m. and update the four csv files with the latest data. \
However, for simplicity of the demo, we have downloaded the four csv files and GCP is not required to open the web application. \
Below are the detailed instructions:
- open the demo folder
- download html folder and open with vscode \
![alt text](figure/figure1.png?raw=true)
- right click on any html files and select Open with Live Server \
![alt text](figure/figure2.png?raw=true)
