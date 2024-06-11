import pandas as pd

# CSV 파일 경로
file_path = "ai-crypto-project-3-live-btc-krw.csv"  # 업로드한 파일의 실제 경로를 사용하십시오.

# CSV 파일 읽기
df = pd.read_csv(file_path)

# 평단가 및 보유 수량 초기화
average_price = 0
total_quantity = 0
total_cost = 0  # 총 비용

# 결과를 저장할 리스트
average_prices = []
quantities = []

# 평단가를 실시간으로 업데이트하는 함수
def update_average_price(row):
    global average_price, total_quantity, total_cost
    if row['side'] == 0:  # 매수
        total_cost += (row['quantity'] * row['price'] + row['fee'])
        total_quantity += row['quantity']
        average_price = total_cost / total_quantity if total_quantity != 0 else 0
    elif row['side'] == 1:  # 매도
        total_cost -= (row['quantity'] * row['price'] - row['fee'])
        total_quantity -= row['quantity']
        # 매도 시에는 평단가를 업데이트하지 않음 (총 비용은 감소)
        if total_quantity == 0:  # 모든 비트코인을 매도한 경우
            total_cost = 0  # 총 비용 초기화
            average_price = 0  # 평단가 초기화

    average_prices.append(average_price)
    quantities.append(total_quantity)

# 각 거래에 대해 평단가와 잔여 수량 업데이트
df.apply(update_average_price, axis=1)

# 결과를 데이터프레임에 추가
df['updated_average_price'] = average_prices
df['remaining_quantity'] = quantities

# 실현 손익 계산
realized_profit_loss = df['amount'].sum()
# 마지막 행의 updated_average_price와 remaining_quantity의 곱 출력. 즉 미실현 손익계산
last_row = df.iloc[-1]
unrealized_profit_loss = last_row['updated_average_price'] * last_row['remaining_quantity']

# 실현 손익 출력
print(f"실현손익: {realized_profit_loss}")
# 미실현 손익 출력
print(f"미실현손익: {unrealized_profit_loss}")


