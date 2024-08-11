import requests
import aiohttp
import asyncio
from bs4 import BeautifulSoup
from datetime import datetime
import json
import pymysql
import db_info

db_connection = pymysql.connect(
    host=db_info.db_host,
    user=db_info.db_username,
    passwd=db_info.db_password,
    db=db_info.db_name,
    port=db_info.db_port,
    charset='utf8',
    cursorclass=pymysql.cursors.DictCursor
)  # db 접근 하기 위한 정보

WEBHOOK = 'https://discord.com/api/webhooks/1265875438807289958/eHenaHiUhpk7Y93peMKPXA85n_2no25IskGskS8CYDfqb_6doIUjZokdL0bu7enODUua'

def lambda_handler(event, context):
    print(f'람다 시작 시각:{datetime.now()}')

    asyncio.run(save_and_send_conference_message())
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

async def save_and_send_conference_message():
    new_dev_events = scrape_events()
    save_new_events(new_dev_events)
    for new_events in new_dev_events:
        await send_events(new_events)

    print(f'총 {len(new_dev_events)}개의 새로운 이벤트')


# 크롤링 함수
def scrape_events():
    url = 'https://dev-event.vercel.app/events'
    response = requests.get(url, headers={})
    soup = BeautifulSoup(response.text, 'html.parser')

    news_container = soup.find('script', id='__NEXT_DATA__', type='application/json')
    if news_container is None:
        print("[error] news container not found")
        return

    # JSON 파싱
    data = json.loads(news_container.string)

    dev_events_for_months = data['props']['pageProps']['fallbackData']

    new_dev_events = []

    for dev_events_for_month in dev_events_for_months:
        new_dev_events_for_month = dev_events_for_month['dev_event']
        new_event = get_new_events(new_dev_events_for_month)
        new_dev_events.extend(new_event)

    return new_dev_events


def get_new_events(dev_events):
    cursor = db_connection.cursor(dictionary=True)
    cursor.execute("SELECT * FROM conference")
    pre_dev_events = cursor.fetchall()
    cursor.close()

    pre_event_ids = {event['id'] for event in pre_dev_events}
    new_dev_events = [event for event in dev_events if event['id'] not in pre_event_ids]
    # print_new_events(new_dev_events)

    return new_dev_events


# 필터링 결과 출력
def print_new_events(new_dev_events):
    for event in new_dev_events:
        print(f"ID: {event['id']}")
        print(f"Title: {event['title']}")
        print(f"Organizer: {event['organizer']}")
        print(f"Event Link: {event['event_link']}")
        print(f"Display Event Time: {event['display_event_time']}")
        print(f"Start Date: {event['start_date_time']}")
        print(f"End Date: {event['end_date_time']}")
        print(f"Tags: {[tag['tag_name'] for tag in event['tags']]}")
        print("------------------------------")
        print()


def save_new_events(new_events):
    cursor = db_connection.cursor()
    # SQL 쿼리 작성
    insert_query = """
            INSERT INTO conference (id, title, link, start_date_time, end_date_time, organizer, tags, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """

    # 데이터 삽입
    for event in new_events:
        tags = [tag['tag_name'] for tag in event['tags']]
        string_tags = ', '.join(tags)
        cursor.execute(insert_query, (
            event['id'],
            event['title'],
            event['event_link'],
            event['start_date_time'],
            event['end_date_time'],
            event['organizer'],
            string_tags,
            datetime.now()
        ))

    # 변경사항 커밋
    db_connection.commit()
    cursor.close()

    print(f"{cursor.rowcount}개의 행이 삽입되었습니다.")


# 디스코드 전송
async def send_events(event):
    title = event['title']
    organizer = event['organizer']
    link = event["event_link"]
    event_time = event['display_event_time']
    start_date = event['start_date_time']
    end_date = event['end_date_time']
    tags = [tag['tag_name'] for tag in event['tags']]
    string_tags = ', '.join(tags)

    news_info = f'{"# " + title}\n - 링크 : {link}\n - 시작 시간: {start_date}\n - 종료 시간: {end_date}\n - 태그: {string_tags}'
    message = {"content": f"{news_info}"}

    async with aiohttp.ClientSession() as session:
        await session.post(WEBHOOK, data=message)
