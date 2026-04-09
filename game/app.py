# stage1
GAME_TITLE_MAX_LEN = 28
GAME_TITLE_START_PROMPT = f'게임 제목을 입력해 주세요. (최대 {GAME_TITLE_MAX_LEN}자이내(<={GAME_TITLE_MAX_LEN}), 반드시 영문 or 숫자 만 사용 가능)'
GAME_TITLE_ERROR_MSG = '정확하게 입력하세요'


while True:
  game_title = input(GAME_TITLE_START_PROMPT + '\n').strip()
  if not game_title:
    print(GAME_TITLE_ERROR_MSG)
  elif len(game_title) > GAME_TITLE_MAX_LEN:
    print(GAME_TITLE_ERROR_MSG)
  else:
    break

# stage2
PLAYER_TITLE_MAX_LEN = GAME_TITLE_MAX_LEN # 관리상 관련 파트에서 사용할 변수를 정의
PLAYER_START_PROMPT = f'플레이어의 이름을 입력해 주세요. (최대 {PLAYER_TITLE_MAX_LEN}자이내(<={PLAYER_TITLE_MAX_LEN}), 반드시 영문 or 숫자 만 사용 가능)'

while True:
  player_name = input(PLAYER_START_PROMPT + '\n').strip()
  if not player_name:
    print(GAME_TITLE_ERROR_MSG)
    continue # 하위 코드 실행 x, 나를 감싸고 있는 가장 가까운 반복문의 조건식으로 점프

  if len(player_name) > PLAYER_TITLE_MAX_LEN:
    print(GAME_TITLE_ERROR_MSG)
    continue  # 하위 코드 실행 x, 나를 감싸고 있는 가장 가까운 반복문의 조건식으로 점프

    # 코드가 실행하는 부분이 여기까지 도착했다 => 정상입력했음을 의미하는 것
  break # 탈출

#stage 3
print(f'''
------------------------------
-{game_title:^28}-
-{player_name:^28}-
-       press any key        -
------------------------------'''.strip())

# stage 4 : 실제 게임 시작
# AI 기능 사용을 위해서 모듈 가져오기(맨 위에서 통상 사용, 필요시 필요한 곳에서 사용 가능)
# 모듈의 경로는 무조건 엔트리포인트를 가진 모듈의 위치에서부터 계산
from ai import gen_random_number

is_playing = True # 계속 게임중임
while is_playing:
    ai_number = gen_random_number(1,100)
    #print(ai_number)
    try_count = 0 # 시도횟수
    while True:
        # stage5 
        '''- 요구사항
        - 플레이어에게 한개의 값을 입력 받음
            - 프럼프트
            - AI가 생성한 값 1 ~ 100 사이중 하나의 정수값을 맞추시오
            - 사용자 입력 후 엔터
            - 입력하기 전(입력후 엔터키 입력)까지는 무한대기
            - 검사
            - 문자열 데이터 기준 검사
                - 빈값, 스페이스 검사 -> 정확하게 입력하세요 (오류메세지)
                - 정수값 아닌지 검사 -> 정수만 입력됩니다. (오류메세지)
                - isnumeric() 사용
            - 수치 데이터 기준 검사
                - 1보다 작거나, 100보가 큰값을 넣으면
                -> 유효 범위값만 입력됩니다. (오류메세지)
            - 정상 입력되면 다음 스테이지로 이동
                '''
        while True:
        # 1. 사용자 입력 대기
            player_number = input('AI가 생성한 값 1 ~ 100 사이중 하나의 정수값을 맞추시오').strip()
            # 2. 유효성 검사
            # 2-1. 값이 비어있으면 
            if not player_number:
                print('정확하게 입력하세요')
                continue
            # 2-2. 값이 있기는 하다 -> 정수 변환이 가능여부 체크
            if not player_number.isnumeric():
                print('정확하게 입력하세요. 정수만 입력됩니다')
                continue
            # 2-3. 값이 있고 정수 형태이다 -> 정수로 변환(타입변경)
            player_number = int(player_number)
            # 2-4. 1보다 작거'나':or, 100보다 크면 => 다시
            if player_number < 1 or player_number > 100:
                print('유효 범위값만 입력됩니다.')
                continue
            # 2-5. 정상입력
            break

        # ai의 값은 ai_number, 플레이어의 값은 player_number가 가지고 있음
        # stage 6
        '''
        - 요구 사항
        - 판단 처리 -> 맞추지 못하면 다시 stage5
            - ai의 숫자과 플레이어 숫자 비교
            - 크다
                - 힌트제공 후 -> stage5 진행
            - 작다
                - 힌트제공 후 -> stage5 진행
            - 같다
                - 게임 클리어 상태
                - 점수 계산
                - (10-시도횟수) * 10
                - 점수를 저장하지는 않음
                - stage7 진행 
        '''
        import ai 
        try_count += 1 # 1회 시도 증가
        hint = ai.check_answer(ai_number, player_number)
        if hint == '정답':
            print(f'정답입니다. 점수는 {(10-try_count)*10}입니다')
        # 점수 계산 : (10-시도횟수)*10
            break
        else:
            print(hint)
        

        # 다시 입력 - stage5

    # stage 7
    '''
    - 요구사항
    - 프럼프트 제공 : 다시 게임을 하시겠습니까? -> 정확하게 게인 진행여부에 대한 의사를 밝힐때까지 무한 반복
        - yes, Yes, Y, y, yEs, yeS, YES, .. -> 모두 OK 처리
        - stage4 진행
        - No,no, nO, NO,.. -> 모두 부정으로 인식
        - 'bye bye ~ ' 출력
        - 게임 종료 처리
    '''
    while True: 
    # 입력 -> 엔터-> 공백 제거 -> 내부적으로 무조건 소문자 처리(대소문자 구분 없음)
        player_answer = input('다시 게임을 하시겠습니까?(yes|no, y|n 중 대소문자 구분없이 가능)').strip().lower()
        if player_answer == 'yes' or player_answer == 'y':
            break
        elif player_answer == 'no' or player_answer == 'n':
            print('bye bye~')
            is_playing = False # 게임을 이제 그만한다 => 반복문 탈출
            break
        else:
            print('정확하게 입력하세요.')