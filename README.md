# InvestStrategy
> FactorSelection 을 통해 생성한 Factor별 분위값으로 전략별 매월 투자 스케줄 생성

각 전략을 key-value 형태로 저장한다.

#### ex) 전략1: 종목 밸류 por 하위 20% 시가총액 가중 리밸런싱
  |date|	item_type|	item_cd|	w_type|	weight|
  |---|---|---|---|---|
  |2006-01-31|	stock|	000230|	market_cap|	0.003833|
  |2006-01-31|	stock|	000480|	market_cap|	0.003215|
  
  output은 **monthly_invest_strategy["stock"]["value"]["por"]** 에 저장된다.
  
  
#### ex) 전략2: 종목 성장 영업이익 yoy 성장률 스프레드 상위 20% 동일 가중 리밸런싱  
  |date|	item_type|	item_cd|	w_type|	weight|
  |---|---|---|---|---|
  |2006-01-31|	stock|	000230|	equal|	0.003833|
  |2006-01-31|	stock|	000480|	equal|	0.003215|
  
  output은 **monthly_invest_strategy["stock"]["growth"]["op_yoy_spr"]** 에 저장된다.


***
# OutPut Sample

### columns info
 * item_type
   * 의미: 자산 타입 
   * 종류: stock, bond, commodity  
 * w_type
   * 의미: weight 기준
   * 종류: equal, market_cap, z_score  
 * weight
   * 의미: 투자비중
 
|date|	item_type|	item_cd|	w_type|	weight|
|---|---|---|---|---|
|2006-01-31|	stock|	000230|	market_cap|	0.003833|
|2006-01-31|	stock|	000480|	market_cap|	0.003215|
|2006-01-31|	stock|	000500|	market_cap|	0.001480|
|2006-01-31|	stock|	000590|	market_cap|	0.000455|
|2006-01-31|	stock|	000680|	market_cap|	0.001033|
|2006-01-31|	stock|	000700|	market_cap|	0.034888|
|2006-01-31|	stock|	000850|	market_cap|	0.001055|
|2006-01-31|	stock|	000990|	market_cap|	0.003516|
|2006-01-31|	stock|	001020|	market_cap|	0.001019|
|2006-01-31|	stock|	001060|	market_cap|	0.004393|
