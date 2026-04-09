
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


PLAYER_TITLE_MAX_LEN = GAME_TITLE_MAX_LEN
PLAYER_START_PROMPT = f'플레이어의 이름을 입력해 주세요. (최대 {PLAYER_TITLE_MAX_LEN}자이내(<={PLAYER_TITLE_MAX_LEN}), 반드시 영문 or 숫자 만 사용 가능)'

while True:
  player_name = input(PLAYER_START_PROMPT + '\n').strip()
  if not player_name:
    print(GAME_TITLE_ERROR_MSG)
    continue 

  if len(player_name) > PLAYER_TITLE_MAX_LEN:
    print(GAME_TITLE_ERROR_MSG)
    continue  
  break 

print(f'''
------------------------------
-{game_title:^28}-
-{player_name:^28}-
-       press any key        -
------------------------------'''.strip())


from ai import gen_random_number

is_playing = True 
while is_playing:
    ai_number = gen_random_number(1,100)
    print(ai_number)
    try_count = 0 
    while True:
        while True:
            player_number = input('AI가 생성한 값 1 ~ 100 사이중 하나의 정수값을 맞추시오').strip()
            if not player_number:
                print('정확하게 입력하세요')
                continue
            if not player_number.isnumeric():
                print('정확하게 입력하세요. 정수만 입력됩니다')
                continue
            player_number = int(player_number)
            if player_number < 1 or player_number > 100:
                print('유효 범위값만 입력됩니다.')
                continue
            break

        import ai 
        try_count += 1 
        hint = ai.check_answer(ai_number, player_number)
        if hint == '정답':
            print(f'정답입니다. 점수는 {(10-try_count)*10}입니다')
            break
        else:
            print(hint)
        
    while True: 
        player_answer = input('다시 게임을 하시겠습니까?(yes|no, y|n 중 대소문자 구분없이 가능)').strip().lower()
        if player_answer == 'yes' or player_answer == 'y':
            break
        elif player_answer == 'no' or player_answer == 'n':
            print('bye bye~')
            is_playing = False 
            break
        else:
            print('정확하게 입력하세요.')