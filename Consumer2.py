from kafka import KafkaConsumer
from json import loads
import matplotlib.pyplot as plt
from collections import deque
import matplotlib.patches as mpatches

# 현재 받아오는 값으로는 문자열로
# US,값,날짜 를 split(',')으로 해서 matplotlib에 찍고 있음
# time complexity를 줄이기 위해, len은 변수로 선언
# list를 for문으로 당기는 반복문을 없애고 deque를 사용해 코드 수정

# Producer send Nation, value, date as a csv type.

us_xpo=deque([]) # us x coordinate
node_us = deque([]) # us y coordinate
us_counter=0
shift_counter1=0
us_tmp = 0
tmp1 = []

eu_xpo=deque([])
node_eu = deque([])
eu_counter=0
shift_counter2=0
eu_tmp = 0
tmp2 = []

gbp_xpo=deque([])
node_gbp = deque([])
gbp_counter=0
shift_counter3=0
gbp_tmp = 0
tmp3 = []

node_avg_us = []
node_avg_eu = []
node_avg_gbp = []

date = []














# Window shift function
# shift를 할 때 항상 xpo와 node를 동시에 하기 때문에 하나의 함수에서 둘다처리

# x and y coordinate come in. that is date and chnge value
def shift1(lst, lst2):
    length=len(lst)
    if length < 2:
        return
    tmp=lst.popleft()
    print(lst)

    length2 = len(lst2)
    if length2 < 2:
        return
    tmp2 = lst2.popleft()
    print(lst2)
    return tmp, tmp2

# Window shift
def shift2(lst, lst2):
    length = len(lst)
    if length < 2:
        return
    tmp=lst.popleft()
    print(lst)

    length2 = len(lst2)
    if length2 < 2:
        return
    tmp2 = lst2.popleft()
    print(lst2)
    return tmp, tmp2

# Window shift
def shift3(lst, lst2):
    length = len(lst)
    if length < 2:
        return
    tmp=lst.popleft()
    print(lst)

    length2 = len(lst2)
    if length2 < 2:
        return
    tmp2 = lst2.popleft()
    print(lst2)
    return tmp, tmp2







# matplot initialize line
# kafka consumer connect
consumer = KafkaConsumer(
    'data1', 'data2', 'data3',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my_group',
    value_deserializer=lambda x: loads(x.decode('utf-8')))

plt.rcParams['animation.html'] = 'jshtml'
fig=plt.figure(figsize=(8,8))
ax=fig.add_subplot(111)
fig.show()
ax.grid()
rc = mpatches.Patch(color='red', label='US')
gc = mpatches.Patch(color='green', label='EU')
bc = mpatches.Patch(color='blue', label='GBP')
avg = mpatches.Patch(color='black', label='AVG')
ax.legend(handles=[rc,gc,bc,avg], loc='upper right')
i=0
plt.title('Cost')
plt.xlabel('Date')
plt.ylabel('Change')















# receive data from kafka producer and draw real time graph
for fmessage in consumer:
    fmessage = fmessage.value
    print(fmessage)
    t,x,d = fmessage.split(",")

# US line
# 결국 새로 들어온 메시지가 us, eu, gbp 중 하나이기 때문에
# window도 3개의 window 중 하나에만 변화가 있음, 조건문 최소화


    if t == 'US':
        node_us.append(round(float(x),3))
        us_tmp = us_tmp + round(float(x),3)
        us_xpo.append(d)
        us_counter = us_counter+1

        if us_counter >= 10:
            if us_xpo is not None:
                ax.plot(us_xpo, node_us, color='r', label='US')
                if us_counter >= 30:
                    node_avg_us.append(us_tmp/us_counter)
                    tmp1.append(d)
                fig.autofmt_xdate(bottom=0.2, rotation=30, ha='right')
                fig.canvas.draw()
                shift_counter1=0
                shift1(us_xpo, node_us)
        if us_counter >= 30:
            ax.plot(tmp1, node_avg_us, color='black')
            fig.autofmt_xdate(bottom=0.2, rotation=30, ha='right')
            fig.canvas.draw()

    elif t == 'EU':
        node_eu.append(round(float(x),3))
        eu_tmp = eu_tmp + round(float(x),3)
        eu_xpo.append(d)
        eu_counter = eu_counter+1

        if eu_counter >= 10:
            if eu_xpo is not None:
                ax.plot(eu_xpo, node_eu, color='g', label='EU')
                if eu_counter >= 30:
                    node_avg_eu.append(eu_tmp/eu_counter)
                    tmp2.append(d)
                fig.autofmt_xdate(bottom=0.2, rotation=30, ha='right')
                fig.canvas.draw()
                shift_counter2=0
                shift2(eu_xpo, node_eu)
        if eu_counter >= 30:
            ax.plot(tmp2, node_avg_eu, color='black')
            fig.autofmt_xdate(bottom=0.2, rotation=30, ha='right')
            fig.canvas.draw()

    elif t == 'GBP':
        node_gbp.append(round(float(x),3))
        gbp_tmp = gbp_tmp + round(float(x),3)
        gbp_xpo.append(d)
        gbp_counter = gbp_counter+1

        if gbp_counter >= 10:
            if gbp_xpo is not None:
                ax.plot(gbp_xpo, node_gbp, color='b', label='GBP')
                if us_counter >= 30:
                    node_avg_gbp.append(gbp_tmp/gbp_counter)
                    tmp3.append(d)
                fig.autofmt_xdate(bottom=0.2, rotation=30, ha='right')
                fig.canvas.draw()
                shift_counter3=0
                shift1(gbp_xpo, node_gbp)
        if gbp_counter >= 30:
            ax.plot(tmp3, node_avg_gbp, color='black')
            fig.autofmt_xdate(bottom=0.2, rotation=30, ha='right')
            fig.canvas.draw()
    i=i+1
