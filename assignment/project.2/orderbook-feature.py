import pandas as pd
import math
import timeit

level_1 = 2
level_2 = 5

file_path = 'C:\\Users\\dyl2e\\Desktop\\24_1\\AI와암호화폐이야기\\Group Project\\Phase 2\\2024-05-01-upbit-BTC-book.csv'
file_path_t = 'C:\\Users\\dyl2e\\Desktop\\24_1\\AI와암호화폐이야기\\Group Project\\Phase 2\\2024-05-01-upbit-BTC-trade.csv'
mid_type = 'wt'

book_imbalance_params = [0.2, level_2, 1]
book_delta_params = [0.2, level_1, 1]
trade_indicator_params = [(0.2, level_1, 1), (0.2, level_1, 5), (0.2, level_1, 15)]

def truncate(number, digits):
    stepper = 10.0 ** digits
    return math.trunc(stepper * number) / stepper

def get_sim_df(fn):
    print('loading... %s' % fn)
    df = pd.read_csv(fn).apply(pd.to_numeric, errors='ignore')
    gr_o = df.groupby(['timestamp'])
    return gr_o

def get_sim_df_trade(fn):
    print('loading... %s' % fn)
    df = pd.read_csv(fn).apply(pd.to_numeric, errors='ignore')
    gr_t = df.groupby(['timestamp'])
    return gr_t

start_time = timeit.default_timer()
group_o = get_sim_df(file_path)
group_t = get_sim_df_trade(file_path_t)
delay = timeit.default_timer() - start_time
print('df loading delay: %.2fs' % delay)

def cal_mid_price(gr_bid_level, gr_ask_level, group_t):
    level = 15

    if len(gr_bid_level) > 0 and len(gr_ask_level) > 0:
        bid_top_price = gr_bid_level.iloc[0].price
        bid_top_level_qty = gr_bid_level.iloc[0].quantity
        ask_top_price = gr_ask_level.iloc[0].price
        ask_top_level_qty = gr_ask_level.iloc[0].quantity
        mid_price = (bid_top_price + ask_top_price) * 0.5

        if mid_type == 'wt':
            mid_price = ((gr_bid_level.head(level))['price'].mean() + (gr_ask_level.head(level))['price'].mean()) * 0.5
        elif mid_type == 'mkt':
            mid_price = ((bid_top_price * ask_top_level_qty) + (ask_top_price * bid_top_level_qty)) / (
                        bid_top_level_qty + ask_top_level_qty)
            mid_price = truncate(mid_price, 1)
        elif mid_type == 'vwap':
            mid_price = (group_t['total'].sum()) / (group_t['units_traded'].sum())
            mid_price = truncate(mid_price, 1)

        return (mid_price, bid_top_price, ask_top_price, bid_top_level_qty, ask_top_level_qty)
    else:
        print('Error: serious cal_mid_price')
        return (-1, -1, -2, -1, -1)

def live_cal_book_i_v1(param, gr_bid_level, gr_ask_level, mid):
    mid_price = mid

    ratio = param[0]
    level = param[1]
    interval = param[2]

    quant_v_bid = gr_bid_level.quantity ** ratio
    price_v_bid = gr_bid_level.price * quant_v_bid

    quant_v_ask = gr_ask_level.quantity ** ratio
    price_v_ask = gr_ask_level.price * quant_v_ask

    askQty = quant_v_ask.values.sum()
    bidPx = price_v_bid.values.sum()
    bidQty = quant_v_bid.values.sum()
    askPx = price_v_ask.values.sum()
    bid_ask_spread = interval

    if bidQty > 0 and askQty > 0:
        book_price = (((askQty * bidPx) / bidQty) + ((bidQty * askPx) / askQty)) / (bidQty + askQty)

    indicator_value = (book_price - mid_price) / bid_ask_spread

    return indicator_value

def live_cal_book_d_v1(param, gr_bid_level, gr_ask_level, var, diff, mid):

    ratio = param[0]
    level = param[1]
    interval = param[2]

    decay = math.exp(-1.0 / interval)

    _flag = var.get('_flag', True)
    prevBidQty = var.get('prevBidQty', 0)
    prevAskQty = var.get('prevAskQty', 0)
    prevBidTop = var.get('prevBidTop', 0)
    prevAskTop = var.get('prevAskTop', 0)
    bidSideAdd = var.get('bidSideAdd', 0)
    bidSideDelete = var.get('bidSideDelete', 0)
    askSideAdd = var.get('askSideAdd', 0)
    askSideDelete = var.get('askSideDelete', 0)
    bidSideTrade = var.get('bidSideTrade', 0)
    askSideTrade = var.get('askSideTrade', 0)
    bidSideFlip = var.get('bidSideFlip', 0)
    askSideFlip = var.get('askSideFlip', 0)
    bidSideCount = var.get('bidSideCount', 0)
    askSideCount = var.get('askSideCount', 0)

    curBidQty = gr_bid_level['quantity'].sum()
    curAskQty = gr_ask_level['quantity'].sum()
    curBidTop = gr_bid_level.iloc[0].price
    curAskTop = gr_ask_level.iloc[0].price

    if curBidQty > prevBidQty:
        bidSideAdd += 1
        bidSideCount += 1
    if curBidQty < prevBidQty:
        bidSideDelete += 1
        bidSideCount += 1
    if curAskQty > prevAskQty:
        askSideAdd += 1
        askSideCount += 1
    if curAskQty < prevAskQty:
        askSideDelete += 1
        askSideCount += 1

    if curBidTop < prevBidTop:
        bidSideFlip += 1
        bidSideCount += 1
    if curAskTop > prevAskTop:
        askSideFlip += 1
        askSideCount += 1

    (_count_1, _count_0, _units_traded_1, _units_traded_0, _price_1, _price_0) = diff

    bidSideTrade += _count_1
    bidSideCount += _count_1

    askSideTrade += _count_0
    askSideCount += _count_0

    if bidSideCount == 0:
        bidSideCount = 1
    if askSideCount == 0:
        askSideCount = 1

    bidBookV = (-bidSideDelete + bidSideAdd - bidSideFlip) / (bidSideCount ** ratio)
    askBookV = (askSideDelete - askSideAdd + askSideFlip) / (askSideCount ** ratio)
    tradeV = (askSideTrade / askSideCount ** ratio) - (bidSideTrade / bidSideCount ** ratio)
    bookDIndicator = askBookV + bidBookV + tradeV

    var['bidSideCount'] = bidSideCount * decay
    var['askSideCount'] = askSideCount * decay
    var['bidSideAdd'] = bidSideAdd * decay
    var['bidSideDelete'] = bidSideDelete * decay
    var['askSideAdd'] = askSideAdd * decay
    var['askSideDelete'] = askSideDelete * decay
    var['bidSideTrade'] = bidSideTrade * decay
    var['askSideTrade'] = askSideTrade * decay
    var['bidSideFlip'] = bidSideFlip * decay
    var['askSideFlip'] = askSideFlip * decay

    var['prevBidQty'] = curBidQty
    var['prevAskQty'] = curAskQty
    var['prevBidTop'] = curBidTop
    var['prevAskTop'] = curAskTop

    return bookDIndicator

def get_diff_count_units(diff):
    _count_1 = _count_0 = _units_traded_1 = _units_traded_0 = 0
    _price_1 = _price_0 = 0

    diff_len = len(diff)
    if diff_len == 1:
        row = diff.iloc[0]
        if row['type'] == 1:
            _count_1 = row['count']
            _units_traded_1 = row['units_traded']
            _price_1 = row['price']
        else:
            _count_0 = row['count']
            _units_traded_0 = row['units_traded']
            _price_0 = row['price']

        return (_count_1, _count_0, _units_traded_1, _units_traded_0, _price_1, _price_0)

    elif diff_len == 2:
        row_1 = diff.iloc[1]
        row_0 = diff.iloc[0]
        _count_1 = row_1['count']
        _count_0 = row_0['count']

        _units_traded_1 = row_1['units_traded']
        _units_traded_0 = row_0['units_traded']

        _price_1 = row_1['price']
        _price_0 = row_0['price']

        return (_count_1, _count_0, _units_traded_1, _units_traded_0, _price_1, _price_0)

def calculate_rsi(prices, window=14):
    delta = prices.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    avg_gain = gain.ewm(com=window - 1, min_periods=window).mean()
    avg_loss = loss.ewm(com=window - 1, min_periods=window).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    return rsi

first_run = True  # Indicator Value의 첫 번째 값이 0이 되도록 첫 번째 호출인지 여부를 확인하는 변수
header_written = False  # CSV 파일에 헤더가 쓰여졌는지 여부를 확인하는 변수
mid_prices = []

for gr_o, gr_t in zip(group_o, group_t):

    gr_t = gr_t[1]

    gr_bid_level = gr_o[1][(gr_o[1].type == 0)]
    gr_ask_level = gr_o[1][(gr_o[1].type == 1)]
    diff = get_diff_count_units(gr_t)

    mid_price, bid, ask, bid_qty, ask_qty = cal_mid_price(gr_bid_level, gr_ask_level, gr_t)
    mid_prices.append(mid_price)
    if len(mid_prices) >= 14:  # Ensure we have enough data points to calculate RSI
        rsi = calculate_rsi(pd.Series(mid_prices)).iloc[-1]
    else:
        rsi = None  # Not enough data points yet

    results = []  # 결과를 저장할 리스트
    if first_run:
        bookImbalance = 0  # 첫 번째 호출일 때 Indicator Value를 0으로 설정
        bookDIndicator = 0
        first_run = False
    else:
        bookImbalance = live_cal_book_i_v1(book_imbalance_params, gr_bid_level, gr_ask_level, mid_price)
        bookDIndicator = live_cal_book_d_v1(book_delta_params, gr_bid_level, gr_ask_level, {}, diff, mid_price)

    results.append({
        'book-delta-v1-.02-5-1': bookDIndicator,
        'book-imbalance-0.2-5-1': bookImbalance,
        'mid_price': mid_price,
        'RSI': rsi,
        'timestamp': (gr_o[1].iloc[0])['timestamp']
    })  # timestamp 추가

    # 결과 리스트를 데이터프레임으로 변환
    result_df = pd.DataFrame(results)

    # CSV 파일로 저장
    if not header_written:
        result_df.to_csv('C:\\Users\\dyl2e\\Desktop\\24_1\\AI와암호화폐이야기\\Group Project\\Phase 2\\2024-05-01-upbit-btc-feature.csv', mode='a', header=True, index=False)
        header_written = True  # 헤더가 쓰여졌음을 표시
    else:
        result_df.to_csv('C:\\Users\\dyl2e\\Desktop\\24_1\\AI와암호화폐이야기\\Group Project\\Phase 2\\2024-05-01-upbit-btc-feature.csv', mode='a', header=False, index=False)
