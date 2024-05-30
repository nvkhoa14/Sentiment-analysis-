from flask import Flask,jsonify, request
import ast

app = Flask(__name__)

count_pos = 0
count_neu = 0
count_neg = 0


@app.route('/getData',methods=['GET'])
def get_data():
    global count_neg, count_neu, count_pos 
    return jsonify(pos = count_pos, neu=count_neu, neg=count_neg)

@app.route('/updateData',methods=['POST'])
def update_data():
    global count_neg, count_neu, count_pos 
    if not request.form:
        return "error", 400
    count_pos = ast.literal_eval(request.form['count_pos'])
    count_neu = ast.literal_eval(request.form['count_neu'])
    count_neg = ast.literal_eval(request.form['count_neg'])
    return "success",201


if __name__ == "__main__":
    app.run(host="localhost",port=2703)
