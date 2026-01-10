from flask import Flask, render_template, request
from flask_predict import *
app = Flask(__name__)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/ask', methods=['POST'])
def ask():
    user_input = request.form['user_input']

    # 使用 GPT-2 模型进行问答处理
    response = model_predict(user_input)

    return render_template('index.html', user_input=user_input, answer=response)


if __name__ == '__main__':
    app.run(debug=True)
