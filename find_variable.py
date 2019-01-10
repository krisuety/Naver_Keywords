from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import MultinomialNB
from sklearn.pipeline import Pipeline
from sklearn.metrics import accuracy_score
import pandas as pd
import pickle
import requests, json, sys, os

def fine_variable(alpha):
    path = os.path.dirname(os.path.realpath(__file__))
    # 기사 데이터 프레임 로드
    print(path)
    article_df = pd.read_pickle("{}/article_2016-06-01.plk".format(path))

    # 테스트 데이터와 트레인 데이터 분리
    X_train, X_test, y_train, y_test = train_test_split(article_df.content, article_df.category, test_size=0.1, random_state=1)

    # vectorizer와 classification algorithm 설정
    clf = Pipeline([
        ('vect', TfidfVectorizer()), 
        ('clf', MultinomialNB(alpha=alpha)) 
    ])

    # 모델 생성
    model = clf.fit(X_train, y_train) 

    # 테스트 데이터 예측 결과 출력
    y_pred = model.predict(X_test)

    # 정확도 확인
    result = accuracy_score(y_test, y_pred)
    send_slack("alpha:{}, accuracy:{}".format(alpha, result))
    return result

def send_slack(msg, channel="#dss", username="model_bot" ):
    webhook_URL = "https://hooks.slack.com/services/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"
    payload = {
        "channel": channel,
        "username": username,
        "icon_emoji": ":provision:",
        "text": msg,
    }
    response = requests.post(
        webhook_URL,
        data = json.dumps(payload),
    )

#변수값 설정
alpha = float(sys.argv[1])
accuracy = fine_variable(alpha)
print(alpha, accuracy)
    
# alphas = [0.1, 0.01, 0.001]
# for alpha in alphas:
#     print(alpha, fine_variable(alpha))
    
