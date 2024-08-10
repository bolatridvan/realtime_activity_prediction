from flask import Flask, render_template, jsonify, make_response
import psycopg2 
import json

app = Flask(__name__) 



# Fetch the data 
def query_db(query, args=(), one=False):

    conn = psycopg2.connect(database="sensors", 
                            user="train", 
                            password="Ankara06", 
                            host="postgresql", port="5432") 
    cur = conn.cursor() 
    cur.execute(query, args)
    r = [dict((cur.description[i][0], value) \
               for i, value in enumerate(row)) for row in cur.fetchall()]
    cur.connection.close()
    return (r[0] if r else None) if one else r

#my_query = query_db("SELECT * FROM occupancy limit %s", (3,))
#print(my_query)

@app.route('/') 
def index(): 
    data = query_db("SELECT * FROM occupancy order by timestamp desc limit %s", (1000,))
    response = make_response(jsonify(data))
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response
@app.route('/realtimeActivityPrediction')
def get_data():
    return render_template('index.html')


if __name__ == '__main__': 
    app.run(debug=True, host='0.0.0.0', port=5000)