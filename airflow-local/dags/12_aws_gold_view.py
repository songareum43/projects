'''
- ma 에서 silver 단계 처리
- 최종 형태의 데이터셋 구성, View 형식으로 구성
'''
# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

# 2. 환경변수
DATABASE_SILVER = 'de_ai_09_ma_silver_db'
DATABASE_GOLD = 'de_ai_09_ma_gold_db' 
# 쿼리 히스토리 저장 -> 메타 데이터
ATHENA_RESULTS  = 's3://de-ai-09-827913617635-ap-northeast-2-an/athena-results/'
# 임시로 증분용 테이블이 아닌 ctas용 테이블 참조
SILVER_TBL_NAME = 'sales_silver_tbl'
GOLD_VIEW_NAME = 'daily_sales_summary_gold_view'

# 3. DAG 정의
with DAG(
    dag_id      = "12_medallion_silver_to_gold_view", 
    description = "gold 단계 작업",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    # 모든 매장은 21시 마감(설정)
    schedule_interval = '@daily', # 00시00분00초에 스케줄 작동
    start_date  = datetime(2026,2,25),     
    catchup     = False,
    tags        = ['aws', 'medallion', 'gold', 'athena', 'view'],
) as dag:
  
    # 작동하면 최신 정보까지 모두 수집 -> 가상테이블
    # 조건 : 어제 데이터를 오늘 수행 -> 조건의 날짜는 1일 전 과거가 됨 => dt 연산 처리 필요
    create_gold_view = AthenaOperator(
        task_id = 'create_or_replace_gold_view',
        query="""
            create or replace view {{params.datacase_gold}}.{{params.view_nm}} as 
            select 
                item_id,
                sum(qty) as total_qty,
                sum(total_price) as total_revenue,
                count(distinct user_id) as unique_customer,
                dt as sales_date
            from {{params.datacase_silver}}.{{params.table_nm}}
            where dt = '2026-04-22'        -- '{{(execution_date-macros.timedelta(days=1)).format('YYYY-MM-DD')}}'
            group by dt, item_id;
        """,
        params = {
            'datacase_gold':DATABASE_GOLD,
            'datacase_silver':DATABASE_SILVER,
            'view_nm':GOLD_VIEW_NAME,
            'table_nm':SILVER_TBL_NAME
        },
        database=DATABASE_GOLD,
        output_location=ATHENA_RESULTS

    )


    # 태스크 순서 설정
    create_gold_view 