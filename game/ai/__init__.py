import random

# ai가 임의의 수 생성
gen_random_number = lambda x,y: random.randint(x,y)

# ai의 값과 플레이어의 값을 비교하여 결과를 반환 - 판단
def check_answer(ai : int, player : int) -> str:
    if ai > player:
        return '힌트 - 작다'
    elif ai < player:
        return '힌트 - 크다'
    else: 
        return '정답'




# 테스트
if __name__ == '__main__':
    # 테스트 코드, 다른 코드에서 모듈 가져오기로 작동되면 이 부분은 실행 X
    for n in range(10):    
        print(gen_random_number(1,100))
    # 판단 테스트
    print(check_answer(1,10))
    print(check_answer(10,10))
    print(check_answer(11,10))