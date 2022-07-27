"""
Name: Clayton Blake
Date: 06/26/2022
Program: This is a basic API that can be used to analyze the stats of all NBA players for the 2021-22 season
"""
import sqlite3
from sqlite3 import Error
import json
import csv
import pandas as pd
import flask
from flask import request, jsonify, render_template


def create_connection(db_file):
    conn = None

    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def make_database():
    database = "NBA_Player_Stats_202122.db"
    sql_create_stats_table = """CREATE TABLE IF NOT EXISTS NBAStats (
                                rank string,
                                full_name text NOT NULL, 
                                team text NOT NULL,
                                pos text NOT NULL,
                                age text NOT NULL, 
                                gp integer,
                                mpg real, 
                                min_percentage real, 
                                usg_percentage real,
                                to_percentage real,
                                fta integer,
                                ft_percentage float,
                                two_point_attempts integer,
                                two_point_percentage real,
                                three_point_attempts integer,
                                three_point_percentage real,
                                eFG_percentage float,
                                ts_percentage float,
                                ppg real,
                                rpg real,
                                trb_percentage real,
                                apg real,
                                ast_percentage real,
                                spg float,
                                bpg float,
                                topg float,
                                vi real,
                                ortg real,
                                drtg real
                                ); """
    conn = create_connection(database)

    if conn is not None:
        create_table(conn, sql_create_stats_table)
    else:
        print('Error! Cannot create the database connection.')

    with open('NBA_Stats_202122.csv') as file:
        data = csv.DictReader(file)

        to_db = [(i['\ufeffRANK'], i['FULL NAME'], i['TEAM'], i['POS'], i['AGE'], i['GP'], i['MPG'], i['MIN%'], i['USG%'], i['TO%'], i['FTA'], i['FT%'], i['2PA'], i['2P%'], i['3PA'], i['3P%'], i['eFG%'], i['TS%'], i['PPG'], i['RPG'], i['TRB%'], i['APG'], i['AST%'], i['SPG'], i['BPG'], i['TOPG'], i['VI'], i['ORTG'], i['DRTG']) for i in data]

    cur = conn.cursor()

    cur.executemany("REPLACE INTO NBAStats (rank,full_name,team,pos,age,gp,mpg,min_percentage,usg_percentage,to_percentage,fta,ft_percentage,two_point_attempts,two_point_percentage,three_point_attempts,three_point_percentage,eFG_percentage,ts_percentage,ppg,rpg,trb_percentage,apg,ast_percentage,spg,bpg,topg,vi,ortg,drtg) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);", to_db)

    conn.commit()
    conn.close()


def main():
    make_database()

    app = flask.Flask(__name__)
    app.config["DEBUG"] = True

    def dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    @app.route('/', methods=['GET'])
    def home():
        return render_template('index.html')

    @app.route('/api/v1/resources/stats/all', methods=['GET'])
    def stats_all():
        conn = sqlite3.connect('NBA_Player_Stats_202122.db')
        conn.row_factory = dict_factory
        cur = conn.cursor()
        all_stats = cur.execute('SELECT * FROM NBAStats;').fetchall()

        return jsonify(all_stats)

    @app.errorhandler(404)
    def page_not_found(e):
        return "<h1>404</h1><p>The resource could not be found.</p>", 404

    # this statement will start the app
    app.run(debug=True, use_reloader=False)


if __name__ == '__main__':
    main()