from datetime import datetime
import json
import pymysql
import db_info

# 매월 1일 지난 달의 데이터를 삭제합니다.


db_connection = pymysql.connect(
    host=db_info.db_host,
    user=db_info.db_username,
    passwd=db_info.db_password,
    db=db_info.db_name,
    port=db_info.db_port,
    charset='utf8',
    cursorclass=pymysql.cursors.DictCursor
)  # db 접근 하기 위한 정보

today = datetime.today().strftime('%Y-%m-%d') #YYYY-MM-DD


def lambda_handler(event, context):
    print(f'람다 시작 시각:{datetime.now()}')

    delete_past_event()
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


def delete_past_event():

    print_deleted_data();
    cursor = db_connection.cursor()
    delete_query = "DELETE FROM conference WHERE end_date_time < %s"
    cursor.execute(delete_query, today)

    db_connection.commit()

    print(f"end_date가 {today} 이전인 {cursor.rowcount}개의 행이 삭제되었습니다.")

    # 커서 닫기
    cursor.close()


def print_deleted_data():
    cursor = db_connection.cursor()
    select_query = "SELECT * FROM conference WHERE end_date_time < %s"
    cursor.execute(select_query, today)
    rows_to_delete = cursor.fetchall()  # 삭제할 데이터 조회
    print_new_events(rows_to_delete);


def print_new_events(new_dev_events):
    for event in new_dev_events:
        print(f"ID: {event['id']}")
        print(f"Title: {event['title']}")
        print(f"Start Date: {event['start_date_time']}")
        print(f"End Date: {event['end_date_time']}")
        print(f"Tags: {[tag['tag_name'] for tag in event['tags']]}")
        print("------------------------------")
        print()
