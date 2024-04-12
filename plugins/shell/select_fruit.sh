#쉘(Shell) 스크립트 :
#Unix/Linux Shell 명령을 이용하여 만들어지고 인터프리터에 의해 한 줄씩 처리되는 파일
#Echo, mkdir, cd, cp, tar, touch 등의 기본적인 쉘 명령어를 입력하여 작성하며 변수를 입력받거나 
#For문, if문, 함수도 사용 가능
#확장자가 없어도 동작하지만 주로 파일명에 .sh확장자를 붙임

#쉘 명령어를 이용하여 복잡한 로직을 처리하거나, 쉘 명령어를 재사용함

#Worker 컨테이너는 외부의 파일을 인식할 수 없고, 컨테이너 안에 파일을 만들면 컨테이너 재시작시 파일이 사라지기 때문에
#vi 편집기를 통해 컨테이너를 수정

FRUIT=$1
if [ $FRUIT == APPLE ];then
	echo "You selected Apple!"
elif [ $FRUIT == ORANGE ];then
	echo "You selected Orange!"
elif [ $FRUIT == GRAPE ];then
	echo "You selected Grape!"
else
	echo "You selected other Fruit!"
fi

