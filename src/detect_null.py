        # ts_str = row.get("date")
        # if not ts_str:
        #     continue
        # ts_exch = parse_api_time(ts_str)
        # if start <= ts_exch < end:
        #     # grab all data fields from api 
        #     open_val   = row.get("open")
        #     high_val   = row.get("high")
        #     low_val    = row.get("low")
        #     close_val  = row.get("close")
        #     volume_val = row.get("volume")
        #     # require close price to exist 
        #     if close_val is None:
        #         


import sys      

def validate_row(row):
    for key, value in row.items():
        if value is None:
            print('Null value found for key: ${key}')