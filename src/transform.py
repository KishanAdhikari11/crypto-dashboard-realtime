import pandas as pd

class CryptoDataTransformer:
    def __init__(self,df):
        self.df=df.copy()
    def transform_all(self):
        return(self.df.pipe(self._clean_data).
               pipe(self._add_market_metrics).
               pipe(self._add_time_features).
               pipe(self._add_technical_indicators).
               pipe(self._add_supply_metrics))
        
    def _clean_data(self,df):
        df=df.copy()
        df.drop_duplicates(subset=["id","last_updated"],inplace=True)
        df["last_updated"]=pd.to_datetime(df['last_updated'])
        df['max_supply']=df['max_supply'].fillna(0)
        price_columns=['price','volume_24h','market_cap','ath_price']
        df[price_columns]=df[price_columns].apply(pd.to_numeric)
        return df
    
    def _add_market_metrics(self,df):
        df=df.copy()
        df['price_distance_from_ath']=df['ath_price']-df['price']
        df['volume_to_market_cap_ratio']=(df['volume_24h']/df['market_cap']).round(4)
        # fix this code 
        total_market_cap=df.groupby('last_updated')['market_cap'].transform('sum')
        df['market_dominance']=(df['market_cap']/total_market_cap*100).round(2)
        df['price_per_rank']=(df['price']/df['rank']).round(2)
        return df
    
    def _add_time_features(self,df):
        df=df.copy()
        df['update_hour']=df['last_updated'].dt.hour
        df['update_day']=df['last_updated'].dt.day
        df['update_month']=df['last_updated'].dt.month
        df['is_weekend']=df['last_updated'].dt.weekday.isin([5,6])
        return df
    
    def _add_technical_indicators(self,df):
        df=df.copy()
        df['price_to_ath_ratio']=(df['price']/df['ath_price']).round(4)
        df['volatility_indicator']=abs(df['percent_change_24h'])
        return df
    
    def _add_supply_metrics(self,df):
        df=df.copy()
        df['Supply_issued_percentage']=((df['total_supply']/df['max_supply'])
                                        .where(df['max_supply']>0).fillna(0))
        df['market_cap_per_supply'] = (df['market_cap'] / df['total_supply']).round(4)
        return df
    
    

