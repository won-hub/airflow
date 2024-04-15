#Python 함수 인자 
#1.*arg :
#args로 들어온 값은 튜플로 저장
#args에서 값을 꺼낼 떄는 인덱스를 이용 (ex: args[0] , args[1])
#args라는 이름 외 다른 이름으로 받아도 OK (ex: some_func(*kk):)
#2.**kwargs : 함수에서 여러 개의 인자 n개를 key-value 형태로 받을 때 사용, 인자를 dictionary로 전달

def get_sftp():
    print('sftp 작업을 시작합니다')

def regist(name, sex, *args):
    print(f'이름: {name}')
    print(f'성별: {sex}')
    print(f'기타옵션들: {args}')

def regist2(name, sex, *args, **kwargs):
    print(f'이름: {name}')
    print(f'성별: {sex}')
    print(f'기타옵션들: {args}')
    email = kwargs['email'] or None
    phone = kwargs['phone'] or None
    if email:
        print(email)
    if phone:
        print(phone)    